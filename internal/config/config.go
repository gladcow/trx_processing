package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr           string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaGroupID       string
	PostgresURL        string
	MigrationsPath     string
	ReadTimeout        time.Duration
	WriteTimeout       time.Duration
	BatchSize          int
	BatchFlushInterval time.Duration
}

func Load() Config {
	return Config{
		HTTPAddr:           getEnv("HTTP_ADDR", ":8080"),
		KafkaBrokers:       splitCSV(getEnv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:         getEnv("KAFKA_TOPIC", "casino.transactions"),
		KafkaGroupID:       getEnv("KAFKA_GROUP_ID", "casino_txn_consumer"),
		PostgresURL:        getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/casino?sslmode=disable"),
		MigrationsPath:     getEnv("MIGRATIONS_PATH", "migrations"),
		ReadTimeout:        getDurationEnv("HTTP_READ_TIMEOUT", 10*time.Second),
		WriteTimeout:       getDurationEnv("HTTP_WRITE_TIMEOUT", 10*time.Second),
		BatchSize:          getIntEnv("BATCH_SIZE", 100),
		BatchFlushInterval: getDurationEnv("BATCH_FLUSH_INTERVAL", 1*time.Second),
	}
}

func getEnv(key, def string) string {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	return val
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func getDurationEnv(key string, def time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return def
	}
	return parsed
}

func getIntEnv(key string, def int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return parsed
}
