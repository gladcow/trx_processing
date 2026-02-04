package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"casino_trxes/internal/logger"
	"casino_trxes/internal/transactions"
)

type MessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type KafkaConsumer struct {
	reader          MessageReader
	repo            transactions.Repository
	maxBatchSize    int
	flushInterval   time.Duration
	batch           []pendingMessage
	batchMu         sync.Mutex
	flushTicker     *time.Ticker
	flushTickerStop chan struct{}
}

type pendingMessage struct {
	msg kafka.Message
	txn *transactions.Transaction
}

func NewKafkaConsumer(brokers []string, topic, groupID string, repo transactions.Repository, batchSize int, flushInterval time.Duration) *KafkaConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		IsolationLevel: kafka.ReadCommitted, // slower,  but  supports transactions on producer side
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
	})
	return &KafkaConsumer{
		reader:          reader,
		repo:            repo,
		maxBatchSize:    batchSize,
		flushInterval:   flushInterval,
		batch:           make([]pendingMessage, 0, batchSize),
		flushTickerStop: make(chan struct{}),
	}
}

func NewKafkaConsumerWithReader(reader MessageReader, repo transactions.Repository, batchSize int, flushInterval time.Duration) *KafkaConsumer {
	return &KafkaConsumer{
		reader:          reader,
		repo:            repo,
		maxBatchSize:    batchSize,
		flushInterval:   flushInterval,
		batch:           make([]pendingMessage, 0, batchSize),
		flushTickerStop: make(chan struct{}),
	}
}

func (c *KafkaConsumer) Run(ctx context.Context) error {
	// Start flush ticker
	c.flushTicker = time.NewTicker(c.flushInterval)
	go func() {
		for {
			select {
			case <-c.flushTicker.C:
				c.flushBatch(ctx)
			case <-c.flushTickerStop:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		// Check if context is done. We can remove this check because
		// FetchMessage processes context, but it could be useful for
		// faster exit during shutdown
		if ctx.Err() != nil {
			// Flush any pending messages before shutdown
			c.flushBatch(context.Background())
			return nil
		}

		msg, err := c.reader.FetchMessage(ctx)

		if err != nil {
			if ctx.Err() != nil {
				// Flush any pending messages before shutdown
				c.flushBatch(context.Background())
				return nil
			}
			logger.Errorf("consumer: failed to fetch message: %v", err)
			// Continue to retry
			time.Sleep(100 * time.Millisecond)
			continue
		}
		currentBatchSize, err := c.addToBatch(msg)
		if err != nil {
			logger.Errorf("consumer: failed to add message to batch: %v", err)
			// Don't commit offset on error - let Kafka redeliver
			// Continue processing next message
			continue
		}

		// Check if we should flush based on batch size
		if currentBatchSize >= c.maxBatchSize {
			c.flushBatch(ctx)
		}
	}
}

func (c *KafkaConsumer) addToBatch(msg kafka.Message) (int, error) {
	// Decode and validate message
	txnMsg, err := transactions.DecodeMessage(msg.Value)
	if err != nil {
		logger.Warnf("consumer: failed to decode message (skipping): %v", err)
		return 0, err
	}

	// Generate message ID for idempotency
	messageID := transactions.GenerateMessageID(msg.Key, msg.Value, msg.Offset)
	txn := txnMsg.ToTransaction(messageID)

	if err := txn.Validate(); err != nil {
		logger.Warnf("consumer: invalid transaction (skipping message): %v", err)
		return 0, err
	}

	// Add to batch
	c.batchMu.Lock()
	defer c.batchMu.Unlock()
	c.batch = append(c.batch, pendingMessage{msg: msg, txn: txn})
	return len(c.batch), nil
}

func (c *KafkaConsumer) flushBatch(ctx context.Context) {
	c.batchMu.Lock()
	if len(c.batch) == 0 {
		c.batchMu.Unlock()
		return
	}

	// Copy batch and clear
	batch := make([]pendingMessage, len(c.batch))
	copy(batch, c.batch)
	c.batch = c.batch[:0]
	c.batchMu.Unlock()

	if len(batch) == 0 {
		return
	}

	// Extract transactions
	txns := make([]transactions.Transaction, 0, len(batch))
	for _, pm := range batch {
		txns = append(txns, *pm.txn)
	}

	// Insert batch into database
	if err := c.repo.BatchInsert(ctx, txns); err != nil {
		logger.Errorf("consumer: failed to insert batch: %v", err)
		// Don't commit offsets - Kafka will redeliver
		return
	}

	// Commit Kafka offsets only after successful DB insert
	messages := make([]kafka.Message, 0, len(batch))
	for _, pm := range batch {
		messages = append(messages, pm.msg)
	}

	if err := c.reader.CommitMessages(ctx, messages...); err != nil {
		logger.Errorf("consumer: failed to commit offsets: %v", err)
		// This is a problem - DB insert succeeded but offset commit failed
		// The messages will be redelivered, but ON CONFLICT will prevent duplicates
		return
	}

	logger.Infof("consumer: successfully processed batch of %d messages", len(batch))
}

func (c *KafkaConsumer) Close() error {
	// Stop flush ticker
	if c.flushTicker != nil {
		c.flushTicker.Stop()
		close(c.flushTickerStop)
	}

	// Flush any remaining messages
	c.flushBatch(context.Background())

	return c.reader.Close()
}
