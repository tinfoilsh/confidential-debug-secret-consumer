package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is a read-only view of the shared secret_storage_items table.
// The storage enclave writes items; the consumer reads them.
type Store interface {
	AllItems(ctx context.Context) ([]item, error)
	Close() error
}

type item struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	Metadata  json.RawMessage `json:"metadata"`
	CreatedAt time.Time       `json:"created_at"`
}

type pgStore struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, databaseURL string) (Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to db: %w", err)
	}
	// Storage owns the schema. We just verify the table exists.
	if _, err := pool.Exec(ctx, `SELECT 1 FROM secret_storage_items LIMIT 1`); err != nil {
		pool.Close()
		return nil, fmt.Errorf("secret_storage_items table not accessible: %w", err)
	}
	return &pgStore{pool: pool}, nil
}

func (s *pgStore) AllItems(ctx context.Context) ([]item, error) {
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
		if meta != nil {
			it.Metadata = json.RawMessage(*meta)
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

func (s *pgStore) Close() error {
	s.pool.Close()
	return nil
}
