// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package db provides batch operation support for the lock-free database.
//
// The batch functionality allows you to group multiple database operations
// together for improved performance and atomicity. Batch operations are
// particularly useful for bulk data operations where you need to perform
// many reads or writes efficiently.
//
// # Key Features
//
//   - Group multiple operations (Put, Delete, PutWithTTL) into a single batch
//   - Improved performance over individual operations
//   - Atomic execution of all operations in the batch
//   - Support for TTL operations within batches
//   - Result tracking for each operation in the batch
//
// # Usage Examples
//
// Creating and executing a batch:
//
//	batch := core.NewBatch[[]byte, string]()
//
//	// Add operations to the batch
//	batch.Put([]byte("key1"), "value1")
//	batch.Put([]byte("key2"), "value2")
//	batch.PutWithTTL([]byte("temp"), "expires", 5*time.Minute)
//	batch.Delete([]byte("old_key"))
//
//	// Execute the batch
//	err := db.ExecuteBatch(ctx, batch)
//	if err != nil {
//	    // Handle error
//	}
//
// Batch get operations:
//
//	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
//	results := db.BatchGet(ctx, keys)
//	for _, result := range results {
//	    if result.Success && result.Found {
//	        fmt.Printf("Key: %s, Value: %v\n", string(result.Key), result.Value)
//	    }
//	}
//
// Batch put operations:
//
//	keys := [][]byte{[]byte("key1"), []byte("key2")}
//	values := []string{"value1", "value2"}
//	err := db.BatchPut(ctx, keys, values)
//
// # Dangers and Warnings
//
//   - **Memory Usage**: Large batches consume significant memory. Avoid creating batches with thousands of operations.
//   - **Atomicity**: While batch operations are atomic, they are not transactional. Partial failures may occur.
//   - **Key-Value Mismatch**: BatchPut requires keys and values slices to have the same length.
//   - **Batch Reuse**: Once a batch is executed, it cannot be reused. Create a new batch for subsequent operations.
//   - **Context Cancellation**: Long-running batch operations should respect context cancellation.
//   - **TTL Precision**: TTL operations within batches have the same precision limitations as individual TTL operations.
//
// # Best Practices
//
//   - Keep batch sizes reasonable (typically 100-1000 operations)
//   - Pre-allocate batch capacity for known operation counts
//   - Always check batch execution errors
//   - Use BatchGet for multiple reads instead of individual Get calls
//   - Clear batches after use if you plan to reuse the batch object
//   - Monitor memory usage when processing large batches
//   - Use appropriate timeouts for batch operations
//   - Consider using transactions for operations that require strict consistency
//
// # Performance Considerations
//
//   - Batch operations provide significant performance improvements over individual operations
//   - BatchGet is more efficient than multiple individual Get calls
//   - BatchPut reduces the overhead of multiple individual Put operations
//   - Large batches may cause temporary memory spikes
//   - Consider the trade-off between batch size and memory usage
//
// # Thread Safety
//
// Batch objects are not thread-safe. Each goroutine should create and use its own batch instance.
// However, the ExecuteBatch method is thread-safe and can be called concurrently.
//
// # Error Handling
//
// Batch operations can fail partially or completely:
//
//	results := db.BatchGet(ctx, keys)
//	for i, result := range results {
//	    if !result.Success {
//	        fmt.Printf("Operation %d failed: %v\n", i, result.Error)
//	    }
//	}
//
// # Memory Management
//
// Batch objects hold references to keys and values until execution. For large batches,
// consider clearing the batch after execution to allow garbage collection:
//
//	batch.Clear() // Optional: allows GC to reclaim memory
//
// # See Also
//
// For more information about the database interface, see the main db package.
package db

import (
	"context"
	"errors"
	"time"
)

// Batch operation errors
var (
	ErrBatchSizeMismatch     = errors.New("batch size mismatch")
	ErrBatchAlreadyCommitted = errors.New("batch already committed")
)

// BatchOperation represents a single operation in a batch
type BatchOperation[K ~[]byte, V any] struct {
	Op    BatchOpType
	Key   K
	Value V
	TTL   *time.Duration // optional TTL
}

// BatchOpType represents the type of batch operation
type BatchOpType int

const (
	BatchOpPut BatchOpType = iota
	BatchOpDelete
	BatchOpPutWithTTL
)

// BatchResult represents the result of a batch operation
type BatchResult[V any] struct {
	Success bool
	Value   V
	Found   bool
	Error   error
}

// Batch represents a batch of operations
type Batch[K ~[]byte, V any] struct {
	operations []BatchOperation[K, V]
	results    []BatchResult[V]
	committed  bool
}

// NewBatch creates a new batch
func NewBatch[K ~[]byte, V any]() *Batch[K, V] {
	return &Batch[K, V]{
		operations: make([]BatchOperation[K, V], 0, 100), // pre-allocate for common batch sizes
		results:    make([]BatchResult[V], 0, 100),
	}
}

// Put adds a put operation to the batch
func (b *Batch[K, V]) Put(key K, value V) {
	b.operations = append(b.operations, BatchOperation[K, V]{
		Op:    BatchOpPut,
		Key:   key,
		Value: value,
	})
}

// PutWithTTL adds a put operation with TTL to the batch
func (b *Batch[K, V]) PutWithTTL(key K, value V, ttl time.Duration) {
	b.operations = append(b.operations, BatchOperation[K, V]{
		Op:    BatchOpPutWithTTL,
		Key:   key,
		Value: value,
		TTL:   &ttl,
	})
}

// Delete adds a delete operation to the batch
func (b *Batch[K, V]) Delete(key K) {
	b.operations = append(b.operations, BatchOperation[K, V]{
		Op:  BatchOpDelete,
		Key: key,
	})
}

// Size returns the number of operations in the batch
func (b *Batch[K, V]) Size() int {
	return len(b.operations)
}

// Clear clears all operations from the batch
func (b *Batch[K, V]) Clear() {
	b.operations = b.operations[:0]
	b.results = b.results[:0]
	b.committed = false
}

// GetResults returns the results of the batch operations
func (b *Batch[K, V]) GetResults() []BatchResult[V] {
	return b.results
}

// IsCommitted returns whether the batch has been committed
func (b *Batch[K, V]) IsCommitted() bool {
	return b.committed
}

// BatchGetResult represents the result of a batch get operation
type BatchGetResult[V any] struct {
	Key   []byte
	Value V
	Found bool
}

// BatchGet performs batch get operations
func (db *db[K, V]) BatchGet(ctx context.Context, keys []K) []BatchGetResult[V] {
	results := make([]BatchGetResult[V], len(keys))

	// Use a single read timestamp for consistency
	rt := db.clock.Load()

	// Process all gets with the same read timestamp
	for i, key := range keys {
		entry := db.index.GetOrCreate(key)
		value, found := entry.Get(rt)
		results[i] = BatchGetResult[V]{
			Key:   key,
			Value: value,
			Found: found,
		}
	}

	return results
}

// BatchPut performs batch put operations
func (db *db[K, V]) BatchPut(ctx context.Context, keys []K, values []V) error {
	if len(keys) != len(values) {
		return ErrBatchSizeMismatch
	}

	// Use a single commit timestamp for all operations
	ct := db.clock.Add(1)

	// Process all puts with the same commit timestamp
	for i, key := range keys {
		entry := db.index.GetOrCreate(key)
		entry.Put(values[i], ct)
	}

	return nil
}

// BatchPutWithTTL performs batch put operations with TTL
func (db *db[K, V]) BatchPutWithTTL(ctx context.Context, keys []K, values []V, ttls []time.Duration) error {
	if len(keys) != len(values) || len(keys) != len(ttls) {
		return ErrBatchSizeMismatch
	}

	// Use a single commit timestamp for all operations
	ct := db.clock.Add(1)

	// Process all puts with the same commit timestamp
	for i, key := range keys {
		entry := db.index.GetOrCreate(key)
		entry.PutWithTTL(values[i], ct, ttls[i])
	}

	return nil
}

// BatchDelete performs batch delete operations
func (db *db[K, V]) BatchDelete(ctx context.Context, keys []K) error {
	// Use a single commit timestamp for all operations
	ct := db.clock.Add(1)

	// Process all deletes with the same commit timestamp
	for _, key := range keys {
		entry := db.index.GetOrCreate(key)
		entry.Delete(ct)
	}

	return nil
}

// ExecuteBatch executes a batch of operations atomically
func (db *db[K, V]) ExecuteBatch(ctx context.Context, batch *Batch[K, V]) error {
	if batch.committed {
		return ErrBatchAlreadyCommitted
	}

	// Use a single commit timestamp for all operations
	ct := db.clock.Add(1)

	// Pre-allocate results slice
	batch.results = make([]BatchResult[V], len(batch.operations))

	// Execute all operations with the same commit timestamp
	for i, op := range batch.operations {
		entry := db.index.GetOrCreate(op.Key)

		switch op.Op {
		case BatchOpPut:
			entry.Put(op.Value, ct)
			batch.results[i] = BatchResult[V]{Success: true}

		case BatchOpPutWithTTL:
			if op.TTL != nil {
				entry.PutWithTTL(op.Value, ct, *op.TTL)
			} else {
				entry.Put(op.Value, ct)
			}
			batch.results[i] = BatchResult[V]{Success: true}

		case BatchOpDelete:
			deleted := entry.Delete(ct)
			batch.results[i] = BatchResult[V]{Success: deleted}
		}
	}

	batch.committed = true
	return nil
}

// BatchGetWithSnapshot performs batch get operations using a snapshot
func (db *db[K, V]) BatchGetWithSnapshot(ctx context.Context, keys []K, snapshot Snapshot[K, V]) []BatchGetResult[V] {
	results := make([]BatchGetResult[V], len(keys))

	// Process all gets using the snapshot
	for i, key := range keys {
		value, found := snapshot.Get(ctx, key)
		results[i] = BatchGetResult[V]{
			Key:   key,
			Value: value,
			Found: found,
		}
	}

	return results
}
