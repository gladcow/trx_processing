package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"casino_trxes/internal/api"
	"casino_trxes/internal/config"
	"casino_trxes/internal/consumer"
	"casino_trxes/internal/db"
	"casino_trxes/internal/logger"
	"casino_trxes/internal/transactions"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		logger.Fatalf("db connect: %v", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool, cfg.MigrationsPath); err != nil {
		logger.Fatalf("db migrate: %v", err)
	}

	repo := transactions.NewPostgresRepository(pool)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, repo, cfg.BatchSize, cfg.BatchFlushInterval)
	defer func() {
		err := kafkaConsumer.Close()
		if err != nil {
			logger.Errorf("kafka consumer close: %v", err)
		}
	}()

	apiServer := api.NewServer(repo)
	httpServer := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      apiServer.Routes(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Infof("http listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	go func() {
		logger.Infof("kafkaConsumer listening on topic %s", cfg.KafkaTopic)
		if err := kafkaConsumer.Run(ctx); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		logger.Errorf("shutdown due to error: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Errorf("http shutdown error: %v", err)
	}
}
