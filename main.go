package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type receivedItem struct {
	ID       string          `json:"id"`
	Data     string          `json:"data"`
	Metadata json.RawMessage `json:"metadata"`
}

type inventoryEntry struct {
	ID   string `json:"id"`
	Size int    `json:"size"`
}

type inventory struct {
	Count int              `json:"count"`
	Total int              `json:"total_bytes"`
	Items []inventoryEntry `json:"items"`
}

type Server struct {
	mu         sync.RWMutex
	secrets    []receivedItem
	storageURL string
}

// handleHealth returns a simple liveness response.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleReceive accepts secrets pushed by the storage enclave over attested TLS.
func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var items []receivedItem
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.secrets = append(s.secrets, items...)
	s.mu.Unlock()

	log.Printf("/receive: accepted %d secret(s)", len(items))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]int{"received": len(items)})
}

// handleInventory returns metrics about received secrets without exposing
// the secret data itself.
func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	inv := inventory{Items: make([]inventoryEntry, len(s.secrets))}
	for i, item := range s.secrets {
		inv.Items[i] = inventoryEntry{ID: item.ID, Size: len(item.Data)}
		inv.Total += len(item.Data)
	}
	inv.Count = len(s.secrets)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(inv)
}

// handleTrigger tells the storage enclave to push secrets to this consumer.
func (s *Server) handleTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if s.storageURL == "" {
		http.Error(w, "STORAGE_URL not configured", http.StatusInternalServerError)
		return
	}

	resp, err := http.Post(s.storageURL+"/push", "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		http.Error(w, "trigger failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	w.Write(body)
}

func main() {
	storageURL := os.Getenv("STORAGE_URL")

	srv := &Server{
		storageURL: storageURL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/receive", srv.handleReceive)
	mux.HandleFunc("/inventory", srv.handleInventory)
	mux.HandleFunc("/trigger", srv.handleTrigger)

	httpSrv := &http.Server{
		Addr:              ":8089",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
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

	shutdownCtx, sCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer sCancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
}
