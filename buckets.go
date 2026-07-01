package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Client struct {
	baseURL string
	tenant  string
	http    *http.Client
}

func NewBucketsClient(baseURL, tenant string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		tenant:  tenant,
		http:    &http.Client{Timeout: 5 * time.Minute},
	}
}

func (c *Client) Get(ctx context.Context, key string, encKey []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.objectURL(key), nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req, encKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("buckets get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buckets: get status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (c *Client) objectURL(key string) string {
	return c.baseURL + "/bucket/" + key
}

func (c *Client) setHeaders(req *http.Request, encKey []byte) {
	req.Header.Set("X-Tinfoil-Tenant-Id", c.tenant)
	req.Header.Set("X-Tinfoil-Encryption-Key", base64.StdEncoding.EncodeToString(encKey))
}
