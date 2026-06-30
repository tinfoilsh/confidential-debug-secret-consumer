package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type keyBundle struct {
	ID       string          `json:"id"`
	Key      string          `json:"key"`
	Metadata json.RawMessage `json:"metadata"`
}

type Server struct {
	buckets    *Client
	store      Store
	mu         sync.RWMutex
	keys       map[string][]byte // itemID -> encryption key (ephemeral, in-memory only)
	storageURL string
	selfHost   string
	selfRepo   string
}

func main() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}
	bucketsURL := os.Getenv("BUCKETS_URL")
	if bucketsURL == "" {
		log.Fatal("BUCKETS_URL is required")
	}
	storageURL := os.Getenv("STORAGE_URL")
	selfHost := os.Getenv("SELF_HOST")
	selfRepo := os.Getenv("SELF_REPO")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := NewStore(ctx, databaseURL)
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}
	defer store.Close()

	srv := &Server{
		buckets:    NewBucketsClient(bucketsURL, "secret-storage"),
		store:      store,
		keys:       make(map[string][]byte),
		storageURL: storageURL,
		selfHost:   selfHost,
		selfRepo:   selfRepo,
	}

	go srv.syncLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/receive", srv.handleReceive)
	mux.HandleFunc("/inventory", srv.handleInventory)
	mux.HandleFunc("/train", srv.handleTrain)

	httpSrv := &http.Server{
		Addr:              ":8089",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      300 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	go func() {
		log.Printf("secret-consumer listening on :8089")
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Printf("shutdown signal received")

	cancel()
	shutdownCtx, sCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer sCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var bundles []keyBundle
	if err := json.NewDecoder(r.Body).Decode(&bundles); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	stored := s.processBundles(r.Context(), bundles)
	log.Printf("/receive: stored %d/%d key bundles", stored, len(bundles))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int{"received": stored})
}

func (s *Server) processBundles(ctx context.Context, bundles []keyBundle) int {
	stored := 0
	for _, b := range bundles {
		encKey, err := base64.StdEncoding.DecodeString(b.Key)
		if err != nil {
			log.Printf("invalid key for %s: %v", b.ID, err)
			continue
		}

		s.mu.Lock()
		s.keys[b.ID] = encKey
		s.mu.Unlock()

		if err := s.store.PutItem(ctx, b.ID, b.Metadata); err != nil {
			log.Printf("storing %s in db: %v", b.ID, err)
			continue
		}
		stored++
	}
	return stored
}

type inventoryEntry struct {
	ID   string          `json:"id"`
	Meta json.RawMessage `json:"metadata"`
}

func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	items, err := s.store.AllItems(r.Context())
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	entries := make([]inventoryEntry, len(items))
	for i, it := range items {
		entries[i] = inventoryEntry{ID: it.ID, Meta: it.Metadata}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"count": len(entries), "items": entries})
}

func (s *Server) handleTrain(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	items, err := s.store.AllItems(r.Context())
	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
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
			log.Printf("/train: no key for %s, skipping", it.ID)
			continue
		}

		plaintext, err := s.buckets.Get(ctx, it.ID, encKey)
		if err != nil {
			log.Printf("/train: retrieving %s: %v", it.ID, err)
			continue
		}
		totalBytes += len(plaintext)
		datasets = append(datasets, dataset{ID: it.ID, Size: len(plaintext), Meta: it.Metadata})
	}

	log.Printf("/train: processed %d datasets (%d bytes total)", len(datasets), totalBytes)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":      "trained",
		"datasets":    len(datasets),
		"total_bytes": totalBytes,
		"items":       datasets,
	})
}

func (s *Server) syncLoop(ctx context.Context) {
	if s.storageURL == "" {
		log.Printf("STORAGE_URL not set, skipping sync loop")
		return
	}
	if s.selfHost == "" || s.selfRepo == "" {
		log.Printf("SELF_HOST or SELF_REPO not set, skipping sync loop")
		return
	}

	pushBody, _ := json.Marshal(map[string]string{
		"host": s.selfHost,
		"repo": s.selfRepo,
	})

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
