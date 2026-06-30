package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store interface {
	PutItem(ctx context.Context, id string, metadata json.RawMessage) error
	AllItems(ctx context.Context) ([]item, error)
	Close() error
}

type item struct {
	ID       string          `json:"id"`
	Metadata json.RawMessage `json:"metadata"`
}

type pgStore struct {
	pool *pgxpool.Pool
}

func NewStore(ctx context.Context, databaseURL string) (Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connecting to db: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS consumer_items (
			id          TEXT PRIMARY KEY,
			metadata    TEXT,
			received_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		return nil, fmt.Errorf("creating schema: %w", err)
	}
	return &pgStore{pool: pool}, nil
}

func (s *pgStore) PutItem(ctx context.Context, id string, metadata json.RawMessage) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO consumer_items (id, metadata) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		id, string(metadata),
	)
	return err
}

func (s *pgStore) AllItems(ctx context.Context) ([]item, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, metadata FROM consumer_items ORDER BY received_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []item
	for rows.Next() {
		var it item
		var meta *string
		if err := rows.Scan(&it.ID, &meta); err != nil {
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
