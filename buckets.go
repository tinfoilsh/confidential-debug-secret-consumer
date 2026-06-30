package main

import (
	"context"
	"encoding/base64"
	"encoding/xml"
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
	switch resp.StatusCode {
	case http.StatusOK:
		return io.ReadAll(resp.Body)
	case http.StatusNotFound:
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("buckets: object %s not found", key)
	default:
		code, msg := s3Error(resp.Body)
		return nil, fmt.Errorf("buckets: get status %d: %s", resp.StatusCode, joinCodeMessage(code, msg))
	}
}

func (c *Client) objectURL(key string) string {
	return c.baseURL + "/bucket/" + key
}

func (c *Client) setHeaders(req *http.Request, encKey []byte) {
	req.Header.Set("X-Tinfoil-Tenant-Id", c.tenant)
	req.Header.Set("X-Tinfoil-Encryption-Key", base64.StdEncoding.EncodeToString(encKey))
}

type s3ErrorBody struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

func s3Error(r io.Reader) (code, message string) {
	raw, _ := io.ReadAll(io.LimitReader(r, 8192))
	var e s3ErrorBody
	if err := xml.Unmarshal(raw, &e); err == nil && e.Code != "" {
		return e.Code, e.Message
	}
	return "", strings.TrimSpace(string(raw))
}

func joinCodeMessage(code, message string) string {
	switch {
	case code != "" && message != "":
		return code + ": " + message
	case code != "":
		return code
	default:
		return message
	}
}
