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

// gitSHA is injected at build time via -ldflags="-X main.gitSHA=...".
var gitSHA = "unknown"

// receivedItem is one secret pushed by the storage enclave.
type receivedItem struct {
	ID       string          `json:"id"`
	Data     string          `json:"data"`
	Metadata json.RawMessage `json:"metadata"`
}

// Server holds the in-memory state for the consumer app.
type Server struct {
	mu         sync.RWMutex
	secrets    []receivedItem
	storageURL string // e.g. "https://secret-storage.tinfoil.containers.tinfoil.dev"
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

// handleReceive accepts secrets pushed by the storage enclave over attested
// TLS. The storage enclave verifies the consumer's attestation before pushing,
// so no client-side verification is needed here.
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

// handleReceived returns all secrets received since startup.
func (s *Server) handleReceived(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(s.secrets)
}

// handleTrigger tells the storage enclave to push secrets to this consumer.
// The storage enclave will verify the consumer's attestation via SecureClient
// before pushing. This endpoint is unauthenticated — it just triggers the
// push; the security is in the storage enclave's attested push.
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
	addr := envDefault("LISTEN_ADDR", ":8089")
	storageURL := os.Getenv("STORAGE_URL")
	if env := os.Getenv("GIT_SHA"); env != "" {
		gitSHA = env
	}

	srv := &Server{
		storageURL: storageURL,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/receive", srv.handleReceive)
	mux.HandleFunc("/received", srv.handleReceived)
	mux.HandleFunc("/trigger", srv.handleTrigger)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	go func() {
		log.Printf("secret-consumer listening on %s (git=%s)", addr, gitSHA)
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

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
