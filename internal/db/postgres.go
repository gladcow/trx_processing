package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v4/pgxpool"
)

func Connect(ctx context.Context, postgresURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, migrationsPath string) error {
	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		files = append(files, filepath.Join(migrationsPath, entry.Name()))
	}
	sort.Strings(files)
	for _, file := range files {
		payload, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		if len(payload) == 0 {
			continue
		}
		if _, err := pool.Exec(ctx, string(payload)); err != nil {
			return fmt.Errorf("apply migration %s: %w", file, err)
		}
	}
	return nil
}
