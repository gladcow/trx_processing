package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v4/pgxpool"

	"casino_trxes/internal/logger"
)

func Connect(ctx context.Context, postgresURL string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(postgresURL)
	if err != nil {
		logger.Errorf("db: parse postgres url: %v", err)
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		logger.Errorf("db: connect postgres: %v", err)
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		logger.Errorf("db: postgres ping failed: %v", err)
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	logger.Infof("db: postgres connected")
	return pool, nil
}

func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, migrationsPath string) error {
	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		logger.Errorf("db: read migrations dir %s: %v", migrationsPath, err)
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
			logger.Errorf("db: read migration %s: %v", file, err)
			return fmt.Errorf("read migration %s: %w", file, err)
		}
		if len(payload) == 0 {
			logger.Warnf("db: skipping empty migration %s", filepath.Base(file))
			continue
		}
		if _, err := pool.Exec(ctx, string(payload)); err != nil {
			logger.Errorf("db: apply migration %s: %v", file, err)
			return fmt.Errorf("apply migration %s: %w", file, err)
		}
		logger.Infof("db: applied migration %s", filepath.Base(file))
	}
	return nil
}
