package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"casino_trxes/internal/config"
	"casino_trxes/internal/logger"
	"casino_trxes/internal/transactions"
)

const batchSize = 1000

func main() {
	if err := logger.Initialize(); err != nil {
		panic(err)
	}

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.KafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer func() {
		if err := writer.Close(); err != nil {
			logger.Errorf("kafka writer close: %v", err)
		}
	}()

	logger.Infof("tx_generator: producing to %s (topic %s), batch size %d; Ctrl+C to stop", strings.Join(cfg.KafkaBrokers, ","), cfg.KafkaTopic, batchSize)

	var n int64
	batch := make([]kafka.Message, 0, batchSize)
	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				_ = writer.WriteMessages(context.Background(), batch...)
				n += int64(len(batch))
			}
			logger.Infof("tx_generator: stopped after %d messages", n)
			return
		default:
		}

		for i := 0; i < batchSize; i++ {
			msg := randomMessage()
			payload, err := json.Marshal(msg)
			if err != nil {
				logger.Errorf("tx_generator: marshal: %v", err)
				continue
			}
			batch = append(batch, kafka.Message{Value: payload})
		}

		if err := writer.WriteMessages(ctx, batch...); err != nil {
			if ctx.Err() != nil {
				if len(batch) > 0 {
					_ = writer.WriteMessages(context.Background(), batch...)
					n += int64(len(batch))
				}
				logger.Infof("tx_generator: stopped after %d messages", n)
				return
			}
			logger.Errorf("tx_generator: write: %v", err)
			continue
		}
		n += int64(len(batch))
		batch = batch[:0]
	}
}

func randomMessage() *transactions.TransactionMessage {
	userID := "user_" + strconv.Itoa(1+rand.Intn(10000))
	var txnType transactions.TransactionType
	if rand.Intn(2) == 0 {
		txnType = transactions.TransactionTypeBet
	} else {
		txnType = transactions.TransactionTypeWin
	}
	amount := 0.01 + rand.Float64()*499.99
	return &transactions.TransactionMessage{
		UserID:          userID,
		TransactionType: txnType,
		Amount:          amount,
		Timestamp:       time.Now(),
	}
}
