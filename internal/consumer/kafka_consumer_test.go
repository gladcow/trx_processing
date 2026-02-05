package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"casino_trxes/internal/transactions"
)

type fakeReader struct {
	messages      chan kafka.Message
	err           error
	fetchErrCount int // return err for first N FetchMessage calls
	commitErr     error
	committedMsgs []kafka.Message
	closeCalled   bool
	mu            sync.Mutex
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	if f.err != nil && f.fetchErrCount > 0 {
		f.fetchErrCount--
		err := f.err
		f.mu.Unlock()
		return kafka.Message{}, err
	}
	f.mu.Unlock()
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case msg := <-f.messages:
		return msg, nil
	}
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committedMsgs = append(f.committedMsgs, msgs...)
	return nil
}

func (f *fakeReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalled = true
	return nil
}

func (f *fakeReader) closeCalledWithLock() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closeCalled
}

type fakeRepo struct {
	mu           sync.Mutex
	inserts      []transactions.Transaction
	batchInserts [][]transactions.Transaction
	err          error
}

func (f *fakeRepo) BatchInsert(ctx context.Context, txns []transactions.Transaction) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batchInserts = append(f.batchInserts, txns)
	f.inserts = append(f.inserts, txns...)
	return nil
}

func (f *fakeRepo) List(ctx context.Context, filter transactions.Filter) ([]transactions.Transaction, *string, error) {
	return nil, nil, errors.New("not implemented")
}

func TestKafkaConsumerRun(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 1)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 100*time.Millisecond)

	msg := transactions.TransactionMessage{
		UserID:          "user-1",
		TransactionType: transactions.TransactionTypeBet,
		Amount:          12.5,
		Timestamp:       time.Now().UTC(),
	}
	payload, _ := json.Marshal(msg)
	reader.messages <- kafka.Message{Value: payload, Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for flush interval
	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	insertCount := len(repo.inserts)
	repo.mu.Unlock()

	if insertCount != 1 {
		t.Fatalf("expected 1 insert, got %d", insertCount)
	}

	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()

	if committedCount != 1 {
		t.Fatalf("expected 1 committed message, got %d", committedCount)
	}
}

func TestKafkaConsumerInvalidPayload(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 1)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 100*time.Millisecond)

	reader.messages <- kafka.Message{Value: []byte(`not-json`), Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	insertCount := len(repo.inserts)
	repo.mu.Unlock()

	if insertCount != 0 {
		t.Fatalf("expected no inserts, got %d", insertCount)
	}

	// Invalid messages should not be committed
	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()

	if committedCount != 0 {
		t.Fatalf("expected no committed messages, got %d", committedCount)
	}
}

func TestKafkaConsumerBatchInsert(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 10)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 5, 1*time.Second)

	// Send 3 messages (less than batch size)
	for i := 0; i < 3; i++ {
		msg := transactions.TransactionMessage{
			UserID:          "user-1",
			TransactionType: transactions.TransactionTypeBet,
			Amount:          float64(i + 1),
			Timestamp:       time.Now().UTC(),
		}
		payload, _ := json.Marshal(msg)
		reader.messages <- kafka.Message{Value: payload, Key: []byte(fmt.Sprintf("msg-%d", i)), Offset: int64(i)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for flush interval to trigger batch insert
	time.Sleep(1200 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	batchCount := len(repo.batchInserts)
	insertCount := len(repo.inserts)
	repo.mu.Unlock()

	if batchCount != 1 {
		t.Fatalf("expected 1 batch insert, got %d", batchCount)
	}
	if insertCount != 3 {
		t.Fatalf("expected 3 inserts, got %d", insertCount)
	}

	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()

	if committedCount != 3 {
		t.Fatalf("expected 3 committed messages, got %d", committedCount)
	}
}

func TestKafkaConsumerInvalidTransactionValidation(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 1)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 100*time.Millisecond)

	// Valid JSON but invalid transaction (zero amount)
	payload := []byte(`{"user_id":"user-1","transaction_type":"bet","amount":0,"timestamp":"2025-02-04T12:00:00Z"}`)
	reader.messages <- kafka.Message{Value: payload, Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	insertCount := len(repo.inserts)
	repo.mu.Unlock()
	if insertCount != 0 {
		t.Fatalf("expected no inserts for invalid transaction, got %d", insertCount)
	}

	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()
	if committedCount != 0 {
		t.Fatalf("expected no committed messages, got %d", committedCount)
	}
}

func TestKafkaConsumerFetchErrorRetry(t *testing.T) {
	reader := &fakeReader{
		messages:      make(chan kafka.Message, 1),
		err:           errors.New("fetch temporarily failed"),
		fetchErrCount: 1, // fail first call, then succeed
	}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 100*time.Millisecond)

	msg := transactions.TransactionMessage{
		UserID:          "user-1",
		TransactionType: transactions.TransactionTypeBet,
		Amount:          1,
		Timestamp:       time.Now().UTC(),
	}
	payload, _ := json.Marshal(msg)
	reader.messages <- kafka.Message{Value: payload, Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	insertCount := len(repo.inserts)
	repo.mu.Unlock()
	if insertCount != 1 {
		t.Fatalf("expected 1 insert after retry, got %d", insertCount)
	}
}

func TestKafkaConsumerContextCancel(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message)} // no messages sent
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 1*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	cancel()

	err := <-done
	if err != nil {
		t.Fatalf("expected nil error on context cancel, got %v", err)
	}
}

func TestKafkaConsumerBatchInsertError(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 1)}
	repo := &fakeRepo{err: errors.New("db unavailable")}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 50*time.Millisecond)

	msg := transactions.TransactionMessage{
		UserID:          "user-1",
		TransactionType: transactions.TransactionTypeBet,
		Amount:          1,
		Timestamp:       time.Now().UTC(),
	}
	payload, _ := json.Marshal(msg)
	reader.messages <- kafka.Message{Value: payload, Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()
	if committedCount != 0 {
		t.Fatalf("expected no commits when BatchInsert fails, got %d", committedCount)
	}
}

func TestKafkaConsumerCommitError(t *testing.T) {
	reader := &fakeReader{
		messages:  make(chan kafka.Message, 1),
		commitErr: errors.New("commit failed"),
	}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 50*time.Millisecond)

	msg := transactions.TransactionMessage{
		UserID:          "user-1",
		TransactionType: transactions.TransactionTypeBet,
		Amount:          1,
		Timestamp:       time.Now().UTC(),
	}
	payload, _ := json.Marshal(msg)
	reader.messages <- kafka.Message{Value: payload, Key: []byte("msg-1"), Offset: 1}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	insertCount := len(repo.inserts)
	repo.mu.Unlock()
	if insertCount != 1 {
		t.Fatalf("expected batch insert to succeed (1 insert), got %d", insertCount)
	}

	reader.mu.Lock()
	committedCount := len(reader.committedMsgs)
	reader.mu.Unlock()
	if committedCount != 0 {
		t.Fatalf("expected no commits when CommitMessages fails, got %d", committedCount)
	}
}

func TestKafkaConsumerClose(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 1)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, 100*time.Millisecond)

	// Start Run so that flushTicker is started
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	time.Sleep(20 * time.Millisecond)
	err := consumer.Close()
	cancel()
	<-done

	if err != nil {
		t.Fatalf("Close() should return nil when reader.Close() succeeds: %v", err)
	}
	if !reader.closeCalledWithLock() {
		t.Fatal("reader.Close() was not called")
	}
}

func TestKafkaConsumerCloseWithoutRun(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 10, time.Second)

	err := consumer.Close()
	if err != nil {
		t.Fatalf("Close() should return nil: %v", err)
	}
	if !reader.closeCalledWithLock() {
		t.Fatal("reader.Close() was not called")
	}
}

func TestNewKafkaConsumer(t *testing.T) {
	repo := &fakeRepo{}
	c := NewKafkaConsumer(
		[]string{"localhost:9092"},
		"test-topic",
		"test-group",
		repo,
		10,
		100*time.Millisecond,
	)
	if c == nil {
		t.Fatal("NewKafkaConsumer returned nil")
	}
	if c.reader == nil {
		t.Fatal("consumer reader is nil")
	}
	_ = c.Close() // avoid leak if reader holds resources
}

func TestKafkaConsumerBatchSizeFlush(t *testing.T) {
	reader := &fakeReader{messages: make(chan kafka.Message, 10)}
	repo := &fakeRepo{}
	consumer := NewKafkaConsumerWithReader(reader, repo, 3, 10*time.Second)

	// Send 5 messages (more than batch size of 3)
	for i := 0; i < 5; i++ {
		msg := transactions.TransactionMessage{
			UserID:          "user-1",
			TransactionType: transactions.TransactionTypeBet,
			Amount:          float64(i + 1),
			Timestamp:       time.Now().UTC(),
		}
		payload, _ := json.Marshal(msg)
		reader.messages <- kafka.Message{Value: payload, Key: []byte(fmt.Sprintf("msg-%d", i)), Offset: int64(i)}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx)
	}()

	// Wait for batch size flush
	time.Sleep(200 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	repo.mu.Lock()
	batchCount := len(repo.batchInserts)
	insertCount := len(repo.inserts)
	repo.mu.Unlock()

	// Should have at least 1 batch (first 3 messages), possibly 2 (next 2 messages)
	if batchCount < 1 {
		t.Fatalf("expected at least 1 batch insert, got %d", batchCount)
	}
	if insertCount != 5 {
		t.Fatalf("expected 5 inserts, got %d", insertCount)
	}
}
