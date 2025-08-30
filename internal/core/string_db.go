// Licensed under the MIT License. See LICENSE file in the project root for details.

package db

import (
	"context"
	"time"

	"github.com/kianostad/lfdb/internal/monitoring/metrics"
)

// StringDB provides a string-keyed interface to the database
// while internally using []byte for performance
type StringDB[V any] struct {
	db DB[[]byte, V]
}

// StringSnapshot provides a string-keyed snapshot interface
type StringSnapshot[V any] struct {
	snapshot Snapshot[[]byte, V]
}

// StringTxn provides a string-keyed transaction interface
type StringTxn[V any] struct {
	txn Txn[[]byte, V]
}

// StringBatchGetResult represents the result of a batch get operation with string keys
type StringBatchGetResult[V any] struct {
	Key   string
	Value V
	Found bool
}

// StringBatch represents a batch of operations with string keys
type StringBatch[V any] struct {
	batch *Batch[[]byte, V]
}

// NewStringDB creates a new database instance with string keys
func NewStringDB[V any]() *StringDB[V] {
	return &StringDB[V]{
		db: New[[]byte, V](),
	}
}

// Get retrieves a value for the given string key
func (s *StringDB[V]) Get(ctx context.Context, key string) (V, bool) {
	return s.db.Get(ctx, []byte(key))
}

// Put stores a value for the given string key
func (s *StringDB[V]) Put(ctx context.Context, key string, val V) {
	s.db.Put(ctx, []byte(key), val)
}

// Delete removes a string key from the database
func (s *StringDB[V]) Delete(ctx context.Context, key string) bool {
	return s.db.Delete(ctx, []byte(key))
}

// Snapshot creates a new snapshot for consistent reads
func (s *StringDB[V]) Snapshot(ctx context.Context) *StringSnapshot[V] {
	return &StringSnapshot[V]{
		snapshot: s.db.Snapshot(ctx),
	}
}

// Txn executes a transaction with snapshot isolation
func (s *StringDB[V]) Txn(ctx context.Context, fn func(tx *StringTxn[V]) error) error {
	return s.db.Txn(ctx, func(tx Txn[[]byte, V]) error {
		stringTx := &StringTxn[V]{txn: tx}
		return fn(stringTx)
	})
}

// GetMetrics returns current database metrics
func (s *StringDB[V]) GetMetrics(ctx context.Context) metrics.MetricsSnapshot {
	return s.db.GetMetrics(ctx)
}

// Flush ensures all pending operations are completed
func (s *StringDB[V]) Flush(ctx context.Context) error {
	return s.db.Flush(ctx)
}

// ManualGC manually triggers garbage collection
func (s *StringDB[V]) ManualGC(ctx context.Context) error {
	return s.db.ManualGC(ctx)
}

// Truncate removes all data from the database
func (s *StringDB[V]) Truncate(ctx context.Context) error {
	return s.db.Truncate(ctx)
}

// Close closes the database and performs cleanup
func (s *StringDB[V]) Close(ctx context.Context) {
	s.db.Close(ctx)
}

// Atomic operations for numeric values
func (s *StringDB[V]) Add(ctx context.Context, key string, delta V) (V, bool) {
	return s.db.Add(ctx, []byte(key), delta)
}

func (s *StringDB[V]) Increment(ctx context.Context, key string) (V, bool) {
	return s.db.Increment(ctx, []byte(key))
}

func (s *StringDB[V]) Decrement(ctx context.Context, key string) (V, bool) {
	return s.db.Decrement(ctx, []byte(key))
}

func (s *StringDB[V]) Multiply(ctx context.Context, key string, factor V) (V, bool) {
	return s.db.Multiply(ctx, []byte(key), factor)
}

func (s *StringDB[V]) Divide(ctx context.Context, key string, divisor V) (V, bool) {
	return s.db.Divide(ctx, []byte(key), divisor)
}

// TTL operations
func (s *StringDB[V]) PutWithTTL(ctx context.Context, key string, val V, ttl time.Duration) {
	s.db.PutWithTTL(ctx, []byte(key), val, ttl)
}

func (s *StringDB[V]) PutWithExpiry(ctx context.Context, key string, val V, expiresAt time.Time) {
	s.db.PutWithExpiry(ctx, []byte(key), val, expiresAt)
}

func (s *StringDB[V]) GetTTL(ctx context.Context, key string) (time.Duration, bool) {
	return s.db.GetTTL(ctx, []byte(key))
}

func (s *StringDB[V]) ExtendTTL(ctx context.Context, key string, extension time.Duration) bool {
	return s.db.ExtendTTL(ctx, []byte(key), extension)
}

func (s *StringDB[V]) RemoveTTL(ctx context.Context, key string) bool {
	return s.db.RemoveTTL(ctx, []byte(key))
}

// Batch operations
func (s *StringDB[V]) BatchGet(ctx context.Context, keys []string) []StringBatchGetResult[V] {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}

	results := s.db.BatchGet(ctx, byteKeys)
	stringResults := make([]StringBatchGetResult[V], len(results))

	for i, result := range results {
		stringResults[i] = StringBatchGetResult[V]{
			Key:   string(result.Key),
			Value: result.Value,
			Found: result.Found,
		}
	}

	return stringResults
}

func (s *StringDB[V]) BatchPut(ctx context.Context, keys []string, values []V) error {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}
	return s.db.BatchPut(ctx, byteKeys, values)
}

func (s *StringDB[V]) BatchPutWithTTL(ctx context.Context, keys []string, values []V, ttls []time.Duration) error {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}
	return s.db.BatchPutWithTTL(ctx, byteKeys, values, ttls)
}

func (s *StringDB[V]) BatchDelete(ctx context.Context, keys []string) error {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}
	return s.db.BatchDelete(ctx, byteKeys)
}

func (s *StringDB[V]) ExecuteBatch(ctx context.Context, batch *StringBatch[V]) error {
	return s.db.ExecuteBatch(ctx, batch.batch)
}

func (s *StringDB[V]) BatchGetWithSnapshot(ctx context.Context, keys []string, snapshot *StringSnapshot[V]) []StringBatchGetResult[V] {
	byteKeys := make([][]byte, len(keys))
	for i, key := range keys {
		byteKeys[i] = []byte(key)
	}

	results := s.db.BatchGetWithSnapshot(ctx, byteKeys, snapshot.snapshot)
	stringResults := make([]StringBatchGetResult[V], len(results))

	for i, result := range results {
		stringResults[i] = StringBatchGetResult[V]{
			Key:   string(result.Key),
			Value: result.Value,
			Found: result.Found,
		}
	}

	return stringResults
}

// StringSnapshot methods
func (s *StringSnapshot[V]) Get(ctx context.Context, key string) (V, bool) {
	return s.snapshot.Get(ctx, []byte(key))
}

func (s *StringSnapshot[V]) Iterate(ctx context.Context, fn func(key string, val V) bool) {
	s.snapshot.Iterate(ctx, func(key []byte, val V) bool {
		return fn(string(key), val)
	})
}

func (s *StringSnapshot[V]) Close(ctx context.Context) {
	s.snapshot.Close(ctx)
}

// StringTxn methods
func (s *StringTxn[V]) Get(ctx context.Context, key string) (V, bool) {
	return s.txn.Get(ctx, []byte(key))
}

func (s *StringTxn[V]) Put(ctx context.Context, key string, val V) {
	s.txn.Put(ctx, []byte(key), val)
}

func (s *StringTxn[V]) Delete(ctx context.Context, key string) {
	s.txn.Delete(ctx, []byte(key))
}

func (s *StringTxn[V]) Flush(ctx context.Context) error {
	return s.txn.Flush(ctx)
}

// Atomic operations within transactions
func (s *StringTxn[V]) Add(ctx context.Context, key string, delta V) (V, bool) {
	return s.txn.Add(ctx, []byte(key), delta)
}

func (s *StringTxn[V]) Increment(ctx context.Context, key string) (V, bool) {
	return s.txn.Increment(ctx, []byte(key))
}

func (s *StringTxn[V]) Decrement(ctx context.Context, key string) (V, bool) {
	return s.txn.Decrement(ctx, []byte(key))
}

func (s *StringTxn[V]) Multiply(ctx context.Context, key string, factor V) (V, bool) {
	return s.txn.Multiply(ctx, []byte(key), factor)
}

func (s *StringTxn[V]) Divide(ctx context.Context, key string, divisor V) (V, bool) {
	return s.txn.Divide(ctx, []byte(key), divisor)
}

// TTL operations within transactions
func (s *StringTxn[V]) PutWithTTL(ctx context.Context, key string, val V, ttl time.Duration) {
	s.txn.PutWithTTL(ctx, []byte(key), val, ttl)
}

func (s *StringTxn[V]) PutWithExpiry(ctx context.Context, key string, val V, expiresAt time.Time) {
	s.txn.PutWithExpiry(ctx, []byte(key), val, expiresAt)
}

func (s *StringTxn[V]) GetTTL(ctx context.Context, key string) (time.Duration, bool) {
	return s.txn.GetTTL(ctx, []byte(key))
}

func (s *StringTxn[V]) ExtendTTL(ctx context.Context, key string, extension time.Duration) bool {
	return s.txn.ExtendTTL(ctx, []byte(key), extension)
}

func (s *StringTxn[V]) RemoveTTL(ctx context.Context, key string) bool {
	return s.txn.RemoveTTL(ctx, []byte(key))
}

// StringBatch methods
func NewStringBatch[V any]() *StringBatch[V] {
	return &StringBatch[V]{
		batch: NewBatch[[]byte, V](),
	}
}

func (b *StringBatch[V]) Put(key string, val V) {
	b.batch.Put([]byte(key), val)
}

func (b *StringBatch[V]) Delete(key string) {
	b.batch.Delete([]byte(key))
}

func (b *StringBatch[V]) PutWithTTL(key string, val V, ttl time.Duration) {
	b.batch.PutWithTTL([]byte(key), val, ttl)
}

// Note: PutWithExpiry is not available in the batch interface
// Use PutWithTTL instead for batch operations with expiration
