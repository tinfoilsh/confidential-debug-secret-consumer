package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// InventoryDB is a read-only view of the shared inventory database.
// The storage enclave writes items; the consumer reads them.
// Private data (plaintext) lives in S3 via the buckets sidecar — never in this database.
type InventoryDB interface {
	AllItems(ctx context.Context) ([]item, error)
	Close() error
}

type item struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
}

type pgInventory struct {
	pool *pgxpool.Pool
}

func NewInventoryDBFromEnv(ctx context.Context) (InventoryDB, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		host := os.Getenv("DATABASE_HOST")
		if host == "" {
			return nil, fmt.Errorf("DATABASE_HOST or DATABASE_URL is required")
		}
		db := os.Getenv("DATABASE_DB")
		if db == "" {
			return nil, fmt.Errorf("DATABASE_DB is required")
		}
		user := os.Getenv("DATABASE_USER")
		if user == "" {
			return nil, fmt.Errorf("DATABASE_USER is required")
		}
		password := os.Getenv("DATABASE_PASSWORD")
		if password == "" {
			return nil, fmt.Errorf("DATABASE_PASSWORD is required")
		}
		databaseURL = fmt.Sprintf("postgres://%s:%s@%s:5432/%s?sslmode=require", user, password, host, db)
	}
	log.Printf("connecting to db")

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to db: %w", err)
	}
	// Storage owns the schema. We just verify the table exists.
	if _, err := pool.Exec(ctx, `SELECT 1 FROM secret_storage_items LIMIT 1`); err != nil {
		pool.Close()
		return nil, fmt.Errorf("secret_storage_items table not accessible: %w", err)
	}
	return &pgInventory{pool: pool}, nil
}

func (s *pgInventory) AllItems(ctx context.Context) ([]item, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, user_id, metadata, created_at FROM secret_storage_items ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []item
	for rows.Next() {
		var it item
		var meta *string
		if err := rows.Scan(&it.ID, &it.UserID, &meta, &it.CreatedAt); err != nil {
			return nil, err
		}
		if meta != nil && *meta != "" {
			it.Metadata = json.RawMessage(*meta)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (s *pgInventory) Close() error {
	s.pool.Close()
	return nil
}
