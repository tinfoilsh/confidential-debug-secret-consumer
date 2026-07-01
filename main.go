package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

type keyBundle struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type Server struct {
	buckets    *Client
	inventory  InventoryDB // public inventory DB (shared Postgres, read-only); private data is in S3 via buckets
	mu         sync.RWMutex
	keys       map[string][]byte // itemID -> encryption key (ephemeral, in-memory only)
	storageURL string
	domain     string
}

func main() {
	storageURL := os.Getenv("STORAGE_URL")
	domain := os.Getenv("DOMAIN")

	// Open the inventory DB (Postgres)
	inventory, err := NewInventoryDBFromEnv(context.Background())
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}
	defer inventory.Close()

	// Start the secret consumer server
	srv := &Server{
		buckets:    NewBucketsClient(os.Getenv("BUCKETS_URL"), "secret-storage"),
		inventory:  inventory,
		keys:       make(map[string][]byte),
		storageURL: storageURL,
		domain:     domain,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.syncLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", srv.handleHealth)
	mux.HandleFunc("POST /receive", srv.handleReceive)
	mux.HandleFunc("GET /inventory", srv.handleInventory)
	mux.HandleFunc("POST /consume", srv.handleConsume)

	log.Printf("secret-consumer listening on :8089")
	log.Fatal(http.ListenAndServe(":8089", mux))
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// /receive accepts key bundles from the storage enclave over attested TLS.
// Keys are stored in memory only - item metadata is already in the shared inventory DB.
func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	var bundles []keyBundle
	if err := json.NewDecoder(r.Body).Decode(&bundles); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stored := 0
	for _, b := range bundles {
		encKey, err := base64.StdEncoding.DecodeString(b.Key)
		if err != nil {
			log.Printf("/receive: invalid key for %s: %v", b.ID, err)
			continue
		}
		s.mu.Lock()
		s.keys[b.ID] = encKey
		s.mu.Unlock()
		stored++
	}

	log.Printf("/receive: stored %d/%d key bundles", stored, len(bundles))
	json.NewEncoder(w).Encode(map[string]int{"received": stored})
}

// /inventory returns the public inventory of all items stored in the shared Postgres database.
func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	items, err := s.inventory.AllItems(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	entries := make([]struct {
		ID   string          `json:"id"`
		Meta json.RawMessage `json:"metadata"`
	}, len(items))
	for i, it := range items {
		entries[i].ID = it.ID
		entries[i].Meta = it.Metadata
	}

	json.NewEncoder(w).Encode(map[string]any{"count": len(entries), "items": entries})
}

// /consume fetches the encrypted data from the Tinfoil Bucket and processes it in-memory for MPC consumption.
func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	items, err := s.inventory.AllItems(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	type dataset struct {
		ID   string          `json:"id"`
		Size int             `json:"size"`
		Meta json.RawMessage `json:"metadata"`
	}
	datasets := make([]dataset, 0, len(items))
	totalBytes := 0

	for _, it := range items {
		s.mu.RLock()
		encKey, ok := s.keys[it.ID]
		s.mu.RUnlock()
		if !ok {
			log.Printf("/consume: no key for %s, skipping", it.ID)
			continue
		}

		plaintext, err := s.buckets.Get(ctx, it.ID, encKey)
		if err != nil {
			log.Printf("/consume: retrieving %s: %v", it.ID, err)
			continue
		}
		totalBytes += len(plaintext)
		datasets = append(datasets, dataset{ID: it.ID, Size: len(plaintext), Meta: it.Metadata})
	}

	log.Printf("/consume: processed %d datasets (%d bytes total)", len(datasets), totalBytes)
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "consumed",
		"datasets":    len(datasets),
		"total_bytes": totalBytes,
		"items":       datasets,
	})
}

// /syncLoop periodically pings the storage enclave to get the user's encryption keys over attested TLS.
func (s *Server) syncLoop(ctx context.Context) {
	if s.storageURL == "" || s.domain == "" {
		log.Printf("STORAGE_URL or DOMAIN not set, skipping sync loop")
		return
	}

	pushBody, _ := json.Marshal(map[string]string{"host": s.domain})
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resp, err := http.Post(s.storageURL+"/push", "application/json", bytes.NewReader(pushBody))
			if err != nil {
				log.Printf("sync: push failed: %v", err)
				continue
			}
			resp.Body.Close()
			log.Printf("sync: push returned %d", resp.StatusCode)
		}
	}
}
