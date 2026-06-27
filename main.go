package main

import (
	"context"
	"crypto/tls"
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

// storageItem is a single secret returned by the storage enclave's /pull
// endpoint.
type storageItem struct {
	ID       string          `json:"id"`
	Data     string          `json:"data"`
	Metadata json.RawMessage `json:"metadata"`
}

// ReceivedSecret is a secret pulled from the storage enclave, stored in
// memory for later retrieval via /received.
type ReceivedSecret struct {
	ID         string          `json:"id"`
	Data       string          `json:"data"`
	Metadata   json.RawMessage `json:"metadata"`
	ReceivedAt time.Time       `json:"received_at"`
}

// errStorageURLRequired is returned when a pull is attempted without
// STORAGE_URL configured.
var errStorageURLRequired = errSimple("STORAGE_URL is required to pull")

// errUpstream describes a non-200 status from the storage enclave.
type errUpstream struct {
	status int
}

func (e errUpstream) Error() string {
	return "storage enclave returned status " + http.StatusText(e.status)
}

type errSimple string

func (e errSimple) Error() string { return string(e) }

// Server holds the in-memory state and configuration for the consumer app.
type Server struct {
	mu         sync.RWMutex
	secrets    []ReceivedSecret
	storageURL string
	client     *http.Client
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

// handlePullNow triggers a pull from the storage enclave via the shim's
// attested-egress endpoint and returns the freshly received items.
func (s *Server) handlePullNow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	items, err := s.pullFromStorage(r.Context())
	if err != nil {
		log.Printf("pull failed: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(items)
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

// pullFromStorage issues a pull against the storage enclave over HTTPS.
// The storage enclave verifies the consumer's attestation (client cert +
// attestation headers forwarded by the shim). In dev mode (without real
// attestation), the storage app accepts the pull without verification.
func (s *Server) pullFromStorage(ctx context.Context) ([]ReceivedSecret, error) {
	if s.storageURL == "" {
		return nil, errStorageURLRequired
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.storageURL+"/pull", nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errUpstream{status: resp.StatusCode}
	}

	var items []storageItem
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, err
	}

	now := time.Now()
	received := make([]ReceivedSecret, 0, len(items))
	for _, it := range items {
		received = append(received, ReceivedSecret{
			ID:         it.ID,
			Data:       it.Data,
			Metadata:   it.Metadata,
			ReceivedAt: now,
		})
	}

	s.mu.Lock()
	s.secrets = append(s.secrets, received...)
	s.mu.Unlock()

	log.Printf("pulled %d secret(s) from storage", len(received))
	return received, nil
}

func main() {
	addr := envDefault("LISTEN_ADDR", ":8089")
	storageURL := os.Getenv("STORAGE_URL")
	autoPull := os.Getenv("AUTO_PULL")
	if env := os.Getenv("GIT_SHA"); env != "" {
		gitSHA = env
	}

	srv := &Server{
		storageURL: storageURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true, // storage enclave uses cert-proxy; trust via attestation channel
				},
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.handleHealth)
	mux.HandleFunc("/pull-now", srv.handlePullNow)
	mux.HandleFunc("/received", srv.handleReceived)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 16,
	}

	if autoPull != "" {
		go func() {
			log.Printf("AUTO_PULL set, triggering initial pull")
			if _, err := srv.pullFromStorage(context.Background()); err != nil {
				log.Printf("auto pull failed: %v", err)
			}
		}()
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
