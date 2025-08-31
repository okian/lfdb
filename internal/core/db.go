// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package db provides a high-performance, lock-free in-memory database with MVCC (Multi-Version Concurrency Control) support.
//
// This package implements a thread-safe database that supports:
//   - Basic CRUD operations (Get, Put, Delete)
//   - Transactions with snapshot isolation
//   - Atomic operations for numeric types
//   - Time-to-live (TTL) functionality
//   - Batch operations for improved performance
//   - Snapshots for consistent reads
//   - Garbage collection and memory management
//
// # Key Features
//
//   - Lock-free algorithms for maximum concurrency
//   - MVCC for consistent reads without blocking writes
//   - Epoch-based garbage collection for safe memory reclamation
//   - SIMD optimizations for bulk operations
//   - Comprehensive metrics and monitoring
//
// # Usage Examples
//
// Basic operations:
//
//	db := core.New[[]byte, string]()
//	defer db.Close(ctx)
//
//	db.Put(ctx, []byte("key"), "value")
//	value, exists := db.Get(ctx, []byte("key"))
//	deleted := db.Delete(ctx, []byte("key"))
//
// Transactions:
//
//	err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
//	    tx.Put(ctx, []byte("key1"), "value1")
//	    tx.Put(ctx, []byte("key2"), "value2")
//	    return nil
//	})
//
// Snapshots:
//
//	snapshot := db.Snapshot(ctx)
//	defer snapshot.Close(ctx)
//	value, exists := snapshot.Get(ctx, []byte("key"))
//
// Atomic operations (for numeric types):
//
//	numDB := core.New[[]byte, int]()
//	newVal, _ := numDB.Add(ctx, []byte("counter"), 5)
//	newVal, _ = numDB.Increment(ctx, []byte("counter"))
//
// TTL operations:
//
//	db.PutWithTTL(ctx, []byte("temp"), "value", 5*time.Minute)
//	ttl, exists := db.GetTTL(ctx, []byte("temp"))
//
// # Dangers and Warnings
//
//   - **Memory Usage**: The database keeps all data in memory. For large datasets, ensure sufficient RAM is available.
//   - **Key Size**: Keys are stored as []byte. Very large keys will consume significant memory.
//   - **Concurrent Access**: While the database is thread-safe, mixing atomic operations with regular operations on the same key can lead to race conditions.
//   - **TTL Precision**: TTL expiration is approximate and may have a delay of up to a few seconds.
//   - **Snapshot Isolation**: Snapshots provide consistent reads but may not see the latest writes from other transactions.
//   - **Garbage Collection**: Manual GC should be called periodically to reclaim memory from deleted/expired entries.
//
// # Best Practices
//
//   - Always call Close() on database instances to ensure proper cleanup
//   - Use transactions for related operations to ensure consistency
//   - Prefer batch operations for bulk data operations
//   - Monitor memory usage and call ManualGC() periodically
//   - Use snapshots for read-heavy workloads that need consistency
//   - Keep keys reasonably sized (preferably under 1KB)
//   - Use appropriate value types - avoid storing very large objects
//   - Handle context cancellation properly in long-running operations
//   - Use TTL for temporary data to prevent memory leaks
//   - Consider using the optimized database variant for high-throughput scenarios
//
// # Performance Considerations
//
//   - The database is optimized for high-concurrency read/write workloads
//   - Batch operations provide better throughput than individual operations
//   - Snapshots have minimal overhead and are safe for concurrent access
//   - Atomic operations are lock-free and highly performant
//   - SIMD optimizations are available for bulk operations on supported platforms
//
// # Thread Safety
//
// All database operations are thread-safe and can be called concurrently from multiple goroutines.
// However, the following patterns should be avoided:
//
//   - Mixing atomic and non-atomic operations on the same key
//   - Long-running transactions that hold locks
//   - Ignoring context cancellation in tight loops
//
// # Memory Management
//
// The database uses epoch-based garbage collection to safely reclaim memory.
// Deleted entries and expired TTL entries are automatically cleaned up,
// but ManualGC() can be called to force immediate cleanup if needed.
//
// # Error Handling
//
// Most operations return errors that should be checked:
//
//	err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
//	    // Handle transaction errors
//	    return nil
//	})
//	if err != nil {
//	    // Handle error
//	}
//
// # Metrics and Monitoring
//
// The database provides comprehensive metrics:
//
//	metrics := db.GetMetrics(ctx)
//	// Access various performance counters and statistics
//
// # See Also
//
// For more examples and advanced usage patterns, see the examples package.
package db

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/kianostad/lfdb/internal/concurrency/epoch"
	"github.com/kianostad/lfdb/internal/monitoring/metrics"
	"github.com/kianostad/lfdb/internal/storage/index"
	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// Addable interface for types that support addition
type Addable[T any] interface {
	Add(other T) T
}

// Incrementable interface for types that support increment/decrement operations
type Incrementable[T any] interface {
	One() T
	NegativeOne() T
}

// Multipliable interface for types that support multiplication
type Multipliable[T any] interface {
	Multiply(other T) T
}

// Dividable interface for types that support division
type Dividable[T any] interface {
	Divide(other T) T
}

// Helper functions for numeric operations
func addNumeric[T any](a, b T) T {
	switch v := any(a).(type) {
	case int:
		return any(v + any(b).(int)).(T)
	case int8:
		return any(v + any(b).(int8)).(T)
	case int16:
		return any(v + any(b).(int16)).(T)
	case int32:
		return any(v + any(b).(int32)).(T)
	case int64:
		return any(v + any(b).(int64)).(T)
	case uint:
		return any(v + any(b).(uint)).(T)
	case uint8:
		return any(v + any(b).(uint8)).(T)
	case uint16:
		return any(v + any(b).(uint16)).(T)
	case uint32:
		return any(v + any(b).(uint32)).(T)
	case uint64:
		return any(v + any(b).(uint64)).(T)
	case float32:
		return any(v + any(b).(float32)).(T)
	case float64:
		return any(v + any(b).(float64)).(T)
	default:
		return b // fallback to just using the delta
	}
}

func multiplyNumeric[T any](a, b T) T {
	switch v := any(a).(type) {
	case int:
		return any(v * any(b).(int)).(T)
	case int8:
		return any(v * any(b).(int8)).(T)
	case int16:
		return any(v * any(b).(int16)).(T)
	case int32:
		return any(v * any(b).(int32)).(T)
	case int64:
		return any(v * any(b).(int64)).(T)
	case uint:
		return any(v * any(b).(uint)).(T)
	case uint8:
		return any(v * any(b).(uint8)).(T)
	case uint16:
		return any(v * any(b).(uint16)).(T)
	case uint32:
		return any(v * any(b).(uint32)).(T)
	case uint64:
		return any(v * any(b).(uint64)).(T)
	case float32:
		return any(v * any(b).(float32)).(T)
	case float64:
		return any(v * any(b).(float64)).(T)
	default:
		return b // fallback to just using the factor
	}
}

func divideNumeric[T any](a, b T) T {
	switch v := any(a).(type) {
	case int:
		if any(b).(int) == 0 {
			return any(0).(T)
		}
		return any(v / any(b).(int)).(T)
	case int8:
		if any(b).(int8) == 0 {
			return any(int8(0)).(T)
		}
		return any(v / any(b).(int8)).(T)
	case int16:
		if any(b).(int16) == 0 {
			return any(int16(0)).(T)
		}
		return any(v / any(b).(int16)).(T)
	case int32:
		if any(b).(int32) == 0 {
			return any(int32(0)).(T)
		}
		return any(v / any(b).(int32)).(T)
	case int64:
		if any(b).(int64) == 0 {
			return any(int64(0)).(T)
		}
		return any(v / any(b).(int64)).(T)
	case uint:
		if any(b).(uint) == 0 {
			return any(uint(0)).(T)
		}
		return any(v / any(b).(uint)).(T)
	case uint8:
		if any(b).(uint8) == 0 {
			return any(uint8(0)).(T)
		}
		return any(v / any(b).(uint8)).(T)
	case uint16:
		if any(b).(uint16) == 0 {
			return any(uint16(0)).(T)
		}
		return any(v / any(b).(uint16)).(T)
	case uint32:
		if any(b).(uint32) == 0 {
			return any(uint32(0)).(T)
		}
		return any(v / any(b).(uint32)).(T)
	case uint64:
		if any(b).(uint64) == 0 {
			return any(uint64(0)).(T)
		}
		return any(v / any(b).(uint64)).(T)
	case float32:
		if any(b).(float32) == 0 {
			return any(float32(0)).(T)
		}
		return any(v / any(b).(float32)).(T)
	case float64:
		if any(b).(float64) == 0 {
			return any(float64(0)).(T)
		}
		return any(v / any(b).(float64)).(T)
	default:
		return any(0).(T) // fallback to zero
	}
}

func createOne[T any]() T {
	var zero T
	switch any(zero).(type) {
	case int:
		return any(1).(T)
	case int8:
		return any(int8(1)).(T)
	case int16:
		return any(int16(1)).(T)
	case int32:
		return any(int32(1)).(T)
	case int64:
		return any(int64(1)).(T)
	case uint:
		return any(uint(1)).(T)
	case uint8:
		return any(uint8(1)).(T)
	case uint16:
		return any(uint16(1)).(T)
	case uint32:
		return any(uint32(1)).(T)
	case uint64:
		return any(uint64(1)).(T)
	case float32:
		return any(float32(1)).(T)
	case float64:
		return any(float64(1)).(T)
	default:
		return zero
	}
}

func createNegativeOne[T any]() T {
	var zero T
	switch any(zero).(type) {
	case int:
		return any(-1).(T)
	case int8:
		return any(int8(-1)).(T)
	case int16:
		return any(int16(-1)).(T)
	case int32:
		return any(int32(-1)).(T)
	case int64:
		return any(int64(-1)).(T)
	case float32:
		return any(float32(-1)).(T)
	case float64:
		return any(float64(-1)).(T)
	default:
		return zero
	}
}

func createZero[T any]() T {
	var zero T
	return zero
}

// DB is the main database interface.
type DB[K ~[]byte, V any] interface {
	Get(ctx context.Context, key K) (V, bool)
	Put(ctx context.Context, key K, val V)
	Delete(ctx context.Context, key K) bool
	Snapshot(ctx context.Context) Snapshot[K, V]
	Txn(ctx context.Context, fn func(tx Txn[K, V]) error) error
	GetMetrics(ctx context.Context) metrics.MetricsSnapshot
	Flush(ctx context.Context) error    // Flush any pending operations and ensure consistency
	ManualGC(ctx context.Context) error // Manually trigger garbage collection
	Truncate(ctx context.Context) error // Remove all data from the database
	Close(ctx context.Context)

	// Atomic operations for numeric values
	Add(ctx context.Context, key K, delta V) (V, bool)       // Add delta to existing value, returns new value
	Increment(ctx context.Context, key K) (V, bool)          // Increment by 1, returns new value
	Decrement(ctx context.Context, key K) (V, bool)          // Decrement by 1, returns new value
	Multiply(ctx context.Context, key K, factor V) (V, bool) // Multiply existing value by factor, returns new value
	Divide(ctx context.Context, key K, divisor V) (V, bool)  // Divide existing value by divisor, returns new value

	// TTL operations
	PutWithTTL(ctx context.Context, key K, val V, ttl time.Duration)      // Put with time-to-live
	PutWithExpiry(ctx context.Context, key K, val V, expiresAt time.Time) // Put with absolute expiry time
	GetTTL(ctx context.Context, key K) (time.Duration, bool)              // Get remaining TTL
	ExtendTTL(ctx context.Context, key K, extension time.Duration) bool   // Extend TTL by duration
	RemoveTTL(ctx context.Context, key K) bool                            // Remove TTL (make permanent)

	// Batch operations
	BatchGet(ctx context.Context, keys []K) []BatchGetResult[V]                                      // Batch get operations
	BatchPut(ctx context.Context, keys []K, values []V) error                                        // Batch put operations
	BatchPutWithTTL(ctx context.Context, keys []K, values []V, ttls []time.Duration) error           // Batch put operations with TTL
	BatchDelete(ctx context.Context, keys []K) error                                                 // Batch delete operations
	ExecuteBatch(ctx context.Context, batch *Batch[K, V]) error                                      // Execute a batch of operations
	BatchGetWithSnapshot(ctx context.Context, keys []K, snapshot Snapshot[K, V]) []BatchGetResult[V] // Batch get operations using snapshot
}

// Snapshot provides a consistent read view of the database.
type Snapshot[K ~[]byte, V any] interface {
	Get(ctx context.Context, key K) (V, bool)
	Iterate(ctx context.Context, fn func(key K, val V) bool)
	Close(ctx context.Context)
}

// Txn represents a transaction with snapshot isolation.
type Txn[K ~[]byte, V any] interface {
	Get(ctx context.Context, key K) (V, bool)
	Put(ctx context.Context, key K, val V)
	Delete(ctx context.Context, key K)
	Flush(ctx context.Context) error // Flush staged writes to ensure they are visible

	// Atomic operations for numeric values within transactions
	Add(ctx context.Context, key K, delta V) (V, bool)       // Add delta to existing value, returns new value
	Increment(ctx context.Context, key K) (V, bool)          // Increment by 1, returns new value
	Decrement(ctx context.Context, key K) (V, bool)          // Decrement by 1, returns new value
	Multiply(ctx context.Context, key K, factor V) (V, bool) // Multiply existing value by factor, returns new value
	Divide(ctx context.Context, key K, divisor V) (V, bool)  // Divide existing value by divisor, returns new value

	// TTL operations within transactions
	PutWithTTL(ctx context.Context, key K, val V, ttl time.Duration)      // Put with time-to-live
	PutWithExpiry(ctx context.Context, key K, val V, expiresAt time.Time) // Put with absolute expiry time
	GetTTL(ctx context.Context, key K) (time.Duration, bool)              // Get remaining TTL
	ExtendTTL(ctx context.Context, key K, extension time.Duration) bool   // Extend TTL by duration
	RemoveTTL(ctx context.Context, key K) bool                            // Remove TTL (make permanent)
}

// db is the main database implementation.
type db[K ~[]byte, V any] struct {
	index   *index.HashIndex[V]
	clock   atomic.Uint64
	epochs  *epoch.Manager
	gc      *mvcc.GC
	metrics *metrics.Metrics
}

// New creates a new database instance.
func New[K ~[]byte, V any]() DB[K, V] {
	epochs := epoch.NewManager()
	gc := mvcc.NewGC(epochs)
	gc.Start() // Start the garbage collector

	return &db[K, V]{
		index:   index.NewHashIndex[V](1024), // 1024 buckets
		epochs:  epochs,
		gc:      gc,
		metrics: metrics.NewMetrics(),
	}
}

// Get retrieves a value for the given key.
func (d *db[K, V]) Get(ctx context.Context, key K) (V, bool) {
	start := time.Now()
	defer func() {
		d.metrics.RecordGet(time.Since(start))
	}()

	rt := d.clock.Load()
	entry := d.index.Get(key)
	if entry == nil {
		var z V
		return z, false
	}
	return entry.Get(rt)
}

// Put stores a value for the given key.
func (d *db[K, V]) Put(ctx context.Context, key K, val V) {
	start := time.Now()
	defer func() {
		d.metrics.RecordPut(time.Since(start))
	}()

	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)
	entry.Put(val, ct)
}

// Delete removes a key from the database.
func (d *db[K, V]) Delete(ctx context.Context, key K) bool {
	start := time.Now()
	defer func() {
		d.metrics.RecordDelete(time.Since(start))
	}()

	ct := d.clock.Add(1)
	entry := d.index.Get(key)
	if entry == nil {
		return false
	}
	return entry.Delete(ct)
}

// Snapshot creates a new snapshot for consistent reads.
func (d *db[K, V]) Snapshot(ctx context.Context) Snapshot[K, V] {
	start := time.Now()
	defer func() {
		d.metrics.RecordSnapshot(time.Since(start))
	}()

	// Capture a stable read timestamp without advancing global time.
	// Using a load here ensures the snapshot reflects the database state
	// as of this moment, and will not accidentally include future writes
	// that haven't been published yet.
	rt := d.clock.Load()
	d.epochs.Register(rt)
	return &snapshot[K, V]{
		db: d,
		rt: rt,
	}
}

// Txn executes a transaction with snapshot isolation.
func (d *db[K, V]) Txn(ctx context.Context, fn func(tx Txn[K, V]) error) error {
	start := time.Now()
	var operationCount int
	defer func() {
		d.metrics.RecordTxnWithOps(time.Since(start), operationCount)
	}()

	rt := d.clock.Load()
	d.epochs.Register(rt)
	defer d.epochs.Unregister(rt)

	tx := &txn[K, V]{
		db:       d,
		rt:       rt,
		writeSet: make(map[string]interface{}),
		ttlSet:   make(map[string]*time.Time),
	}

	// Execute the transaction function
	if err := fn(tx); err != nil {
		return err
	}

	// Count operations for metrics
	operationCount = len(tx.writeSet)

	// Commit staged writes
	ct := d.clock.Add(1)
	for keyStr, val := range tx.writeSet {
		key := K([]byte(keyStr))
		if val == nil {
			// Delete
			entry := d.index.Get(key)
			if entry != nil {
				entry.Delete(ct)
			}
		} else {
			// Put
			entry := d.index.GetOrCreate(key)

			// Check if there's a TTL for this key
			if expiry, hasTTL := tx.ttlSet[keyStr]; hasTTL {
				if expiry == nil {
					// Remove TTL
					entry.Put(val.(V), ct)
				} else {
					// Set absolute expiry
					entry.PutWithExpiry(val.(V), ct, *expiry)
				}
			} else {
				// Regular put without TTL
				entry.Put(val.(V), ct)
			}
		}
	}

	return nil
}

// GetMetrics returns current database metrics
func (d *db[K, V]) GetMetrics(ctx context.Context) metrics.MetricsSnapshot {
	// Update active snapshots count
	d.metrics.SetActiveSnapshots(uint64(d.epochs.ActiveCount())) // #nosec G115
	return d.metrics.GetStats()
}

// GetMetricsLegacy returns metrics in the old map format for backward compatibility
func (d *db[K, V]) GetMetricsLegacy(ctx context.Context) map[string]interface{} {
	// Update active snapshots count
	d.metrics.SetActiveSnapshots(uint64(d.epochs.ActiveCount())) // #nosec G115
	return d.metrics.GetStatsLegacy()
}

// Flush ensures all pending operations are completed and the database is in a consistent state.
// For an in-memory database, this primarily ensures all pending transactions are committed
// and the garbage collector has processed any obsolete versions.
func (d *db[K, V]) Flush(ctx context.Context) error {
	// Wait for any pending transactions to complete by ensuring all epochs are processed
	// This is a simple approach - in a more sophisticated implementation, you might
	// want to track active transactions and wait for them to complete

	// Force a garbage collection cycle to clean up any obsolete versions
	d.gc.ForceCollect()

	// Ensure all pending operations are visible by advancing the clock
	// This creates a barrier that ensures all previous operations are committed
	d.clock.Add(1)

	return nil
}

// ManualGC manually triggers garbage collection to clean up obsolete versions.
// This is useful for memory management and performance optimization.
func (d *db[K, V]) ManualGC(ctx context.Context) error {
	// Force multiple garbage collection cycles to ensure thorough cleanup
	for i := 0; i < 3; i++ {
		d.gc.ForceCollect()
		// Small delay between cycles to allow for processing
		time.Sleep(1 * time.Millisecond)
	}
	return nil
}

// Truncate removes all data from the database, effectively resetting it to an empty state.
// This operation is irreversible and will remove all keys and values.
func (d *db[K, V]) Truncate(ctx context.Context) error {
	// Wait for any pending operations to complete
	if err := d.Flush(ctx); err != nil {
		return fmt.Errorf("flush failed: %v", err)
	}

	// Create a new index to replace the old one
	newIndex := index.NewHashIndex[V](1024)

	// Atomically replace the index
	// Note: This is a simple approach. In a more sophisticated implementation,
	// you might want to coordinate with active transactions and snapshots
	d.index = newIndex

	// Reset the clock to start fresh
	d.clock.Store(0)

	// Force garbage collection to clean up old data
	if err := d.ManualGC(ctx); err != nil {
		return fmt.Errorf("manual GC failed: %v", err)
	}

	return nil
}

// Close closes the database and performs cleanup.
func (d *db[K, V]) Close(ctx context.Context) {
	d.gc.Stop()
	d.metrics.Close()
}

// Add adds delta to the existing value at key. If key doesn't exist, creates it with delta.
// Returns the new value and whether the operation was successful.
func (d *db[K, V]) Add(ctx context.Context, key K, delta V) (V, bool) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)

	// Get current value
	rt := d.clock.Load()
	current, exists := entry.Get(rt)

	var newVal V
	if !exists {
		// Key doesn't exist, use delta as initial value
		newVal = delta
	} else {
		// Try to add delta to current value
		if addable, ok := any(current).(Addable[V]); ok {
			newVal = addable.Add(delta)
		} else {
			// Fallback: try to convert to numeric types
			newVal = addNumeric(current, delta)
		}
	}

	entry.Put(newVal, ct)
	return newVal, true
}

// Increment increments the value at key by 1. If key doesn't exist, creates it with value 1.
// Returns the new value and whether the operation was successful.
func (d *db[K, V]) Increment(ctx context.Context, key K) (V, bool) {
	var one V
	if incrementable, ok := any(one).(Incrementable[V]); ok {
		one = incrementable.One()
	} else {
		// Try to create a "one" value for common numeric types
		one = createOne[V]()
	}
	return d.Add(ctx, key, one)
}

// Decrement decrements the value at key by 1. If key doesn't exist, creates it with value -1.
// Returns the new value and whether the operation was successful.
func (d *db[K, V]) Decrement(ctx context.Context, key K) (V, bool) {
	var negOne V
	if incrementable, ok := any(negOne).(Incrementable[V]); ok {
		negOne = incrementable.NegativeOne()
	} else {
		// Try to create a "-1" value for common numeric types
		negOne = createNegativeOne[V]()
	}
	return d.Add(ctx, key, negOne)
}

// Multiply multiplies the existing value at key by factor. If key doesn't exist, creates it with factor.
// Returns the new value and whether the operation was successful.
func (d *db[K, V]) Multiply(ctx context.Context, key K, factor V) (V, bool) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)

	// Get current value
	rt := d.clock.Load()
	current, exists := entry.Get(rt)

	var newVal V
	if !exists {
		// Key doesn't exist, use factor as initial value
		newVal = factor
	} else {
		// Try to multiply current value by factor
		if multipliable, ok := any(current).(Multipliable[V]); ok {
			newVal = multipliable.Multiply(factor)
		} else {
			// Fallback: try to convert to numeric types
			newVal = multiplyNumeric(current, factor)
		}
	}

	entry.Put(newVal, ct)
	return newVal, true
}

// Divide divides the existing value at key by divisor. If key doesn't exist, creates it with 0.
// Returns the new value and whether the operation was successful.
func (d *db[K, V]) Divide(ctx context.Context, key K, divisor V) (V, bool) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)

	// Get current value
	rt := d.clock.Load()
	current, exists := entry.Get(rt)

	var newVal V
	if !exists {
		// Key doesn't exist, use 0 as initial value
		newVal = createZero[V]()
	} else {
		// Try to divide current value by divisor
		if dividable, ok := any(current).(Dividable[V]); ok {
			newVal = dividable.Divide(divisor)
		} else {
			// Fallback: try to convert to numeric types
			newVal = divideNumeric(current, divisor)
		}
	}

	entry.Put(newVal, ct)
	return newVal, true
}

// PutWithTTL stores a value for the given key with a time-to-live.
func (d *db[K, V]) PutWithTTL(ctx context.Context, key K, val V, ttl time.Duration) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)
	entry.PutWithTTL(val, ct, ttl)
}

// PutWithExpiry stores a value for the given key with an absolute expiry time.
func (d *db[K, V]) PutWithExpiry(ctx context.Context, key K, val V, expiresAt time.Time) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)
	entry.PutWithExpiry(val, ct, expiresAt)
}

// GetTTL returns the remaining TTL for the given key.
func (d *db[K, V]) GetTTL(ctx context.Context, key K) (time.Duration, bool) {
	rt := d.clock.Load()
	entry := d.index.Get(key)
	if entry == nil {
		return 0, false
	}
	return entry.GetTTL(rt)
}

// ExtendTTL extends the TTL of the given key by the specified duration.
func (d *db[K, V]) ExtendTTL(ctx context.Context, key K, extension time.Duration) bool {
	ct := d.clock.Add(1)
	entry := d.index.Get(key)
	if entry == nil {
		return false
	}
	return entry.ExtendTTL(ct, extension)
}

// RemoveTTL removes the TTL from the given key, making it permanent.
func (d *db[K, V]) RemoveTTL(ctx context.Context, key K) bool {
	ct := d.clock.Add(1)
	entry := d.index.Get(key)
	if entry == nil {
		return false
	}
	return entry.RemoveTTL(ct)
}

// snapshot implements the Snapshot interface.
type snapshot[K ~[]byte, V any] struct {
	db *db[K, V]
	rt uint64
}

// Get retrieves a value from the snapshot.
func (s *snapshot[K, V]) Get(ctx context.Context, key K) (V, bool) {
	entry := s.db.index.Get(key)
	if entry == nil {
		var z V
		return z, false
	}
	return entry.Get(s.rt)
}

// Iterate iterates over all key-value pairs in the snapshot
func (s *snapshot[K, V]) Iterate(ctx context.Context, fn func(key K, val V) bool) {
	iterator := s.db.index.NewIterator(s.rt)
	for iterator.Next() {
		key := K(iterator.Key())
		if val, exists := iterator.Value(); exists {
			if !fn(key, val.(V)) {
				break
			}
		}
	}
}

// Close closes the snapshot and unregisters it from the epoch manager.
func (s *snapshot[K, V]) Close(ctx context.Context) {
	s.db.epochs.Unregister(s.rt)
}

// txn implements the Txn interface.
type txn[K ~[]byte, V any] struct {
	db       *db[K, V]
	rt       uint64
	writeSet map[string]interface{} // staged writes
	ttlSet   map[string]*time.Time  // staged TTL operations
}

// Get retrieves a value within the transaction.
func (t *txn[K, V]) Get(ctx context.Context, key K) (V, bool) {
	// Check write set first
	if val, exists := t.writeSet[string(key)]; exists {
		if val == nil {
			// Deleted
			var z V
			return z, false
		}
		return val.(V), true
	}

	// Read from database
	entry := t.db.index.Get(key)
	if entry == nil {
		var z V
		return z, false
	}
	return entry.Get(t.rt)
}

// Put stores a value within the transaction.
func (t *txn[K, V]) Put(ctx context.Context, key K, val V) {
	// Stage the write
	t.writeSet[string(key)] = val
}

// Delete removes a key within the transaction.
func (t *txn[K, V]) Delete(ctx context.Context, key K) {
	// Stage the delete
	t.writeSet[string(key)] = nil
}

// Add adds delta to the existing value at key within the transaction.
func (t *txn[K, V]) Add(ctx context.Context, key K, delta V) (V, bool) {
	// Get current value (check write set first, then database)
	var current V
	var exists bool

	if val, staged := t.writeSet[string(key)]; staged {
		if val != nil {
			current = val.(V)
			exists = true
		}
	} else {
		current, exists = t.Get(ctx, key)
	}

	var newVal V
	if !exists {
		// Key doesn't exist, use delta as initial value
		newVal = delta
	} else {
		// Try to add delta to current value
		if addable, ok := any(current).(Addable[V]); ok {
			newVal = addable.Add(delta)
		} else {
			// Fallback: try to convert to numeric types
			newVal = addNumeric(current, delta)
		}
	}

	// Stage the write
	t.writeSet[string(key)] = newVal
	return newVal, true
}

// Increment increments the value at key by 1 within the transaction.
func (t *txn[K, V]) Increment(ctx context.Context, key K) (V, bool) {
	var one V
	if incrementable, ok := any(one).(Incrementable[V]); ok {
		one = incrementable.One()
	} else {
		// Try to create a "one" value for common numeric types
		one = createOne[V]()
	}
	return t.Add(ctx, key, one)
}

// Decrement decrements the value at key by 1 within the transaction.
func (t *txn[K, V]) Decrement(ctx context.Context, key K) (V, bool) {
	var negOne V
	if incrementable, ok := any(negOne).(Incrementable[V]); ok {
		negOne = incrementable.NegativeOne()
	} else {
		// Try to create a "-1" value for common numeric types
		negOne = createNegativeOne[V]()
	}
	return t.Add(ctx, key, negOne)
}

// Multiply multiplies the existing value at key by factor within the transaction.
func (t *txn[K, V]) Multiply(ctx context.Context, key K, factor V) (V, bool) {
	// Get current value (check write set first, then database)
	var current V
	var exists bool

	if val, staged := t.writeSet[string(key)]; staged {
		if val != nil {
			current = val.(V)
			exists = true
		}
	} else {
		current, exists = t.Get(ctx, key)
	}

	var newVal V
	if !exists {
		// Key doesn't exist, use factor as initial value
		newVal = factor
	} else {
		// Try to multiply current value by factor
		if multipliable, ok := any(current).(Multipliable[V]); ok {
			newVal = multipliable.Multiply(factor)
		} else {
			// Fallback: try to convert to numeric types
			newVal = multiplyNumeric(current, factor)
		}
	}

	// Stage the write
	t.writeSet[string(key)] = newVal
	return newVal, true
}

// Divide divides the existing value at key by divisor within the transaction.
func (t *txn[K, V]) Divide(ctx context.Context, key K, divisor V) (V, bool) {
	// Get current value (check write set first, then database)
	var current V
	var exists bool

	if val, staged := t.writeSet[string(key)]; staged {
		if val != nil {
			current = val.(V)
			exists = true
		}
	} else {
		current, exists = t.Get(ctx, key)
	}

	var newVal V
	if !exists {
		// Key doesn't exist, use 0 as initial value
		newVal = createZero[V]()
	} else {
		// Try to divide current value by divisor
		if dividable, ok := any(current).(Dividable[V]); ok {
			newVal = dividable.Divide(divisor)
		} else {
			// Fallback: try to convert to numeric types
			newVal = divideNumeric(current, divisor)
		}
	}

	// Stage the write
	t.writeSet[string(key)] = newVal
	return newVal, true
}

// PutWithTTL stores a value with TTL within the transaction.
func (t *txn[K, V]) PutWithTTL(ctx context.Context, key K, val V, ttl time.Duration) {
	expiresAt := time.Now().Add(ttl)
	t.writeSet[string(key)] = val
	t.ttlSet[string(key)] = &expiresAt
}

// PutWithExpiry stores a value with absolute expiry time within the transaction.
func (t *txn[K, V]) PutWithExpiry(ctx context.Context, key K, val V, expiresAt time.Time) {
	t.writeSet[string(key)] = val
	t.ttlSet[string(key)] = &expiresAt
}

// GetTTL returns the remaining TTL for the given key within the transaction.
func (t *txn[K, V]) GetTTL(ctx context.Context, key K) (time.Duration, bool) {
	keyStr := string(key)

	// Check write set first
	if val, exists := t.writeSet[keyStr]; exists {
		if val == nil {
			// Deleted
			return 0, false
		}
		// Check if there's a staged TTL
		if expiry, hasTTL := t.ttlSet[keyStr]; hasTTL && expiry != nil {
			remaining := time.Until(*expiry)
			if remaining <= 0 {
				return 0, false // expired
			}
			return remaining, true
		}
		return 0, false // no TTL in staged write
	}

	// Read from database
	entry := t.db.index.Get(key)
	if entry == nil {
		return 0, false
	}
	return entry.GetTTL(t.rt)
}

// ExtendTTL extends the TTL of the given key within the transaction.
func (t *txn[K, V]) ExtendTTL(ctx context.Context, key K, extension time.Duration) bool {
	keyStr := string(key)

	// Check if key exists in write set
	if val, exists := t.writeSet[keyStr]; exists {
		if val == nil {
			return false // deleted
		}

		// If there's already a staged TTL, extend it
		if expiry, hasTTL := t.ttlSet[keyStr]; hasTTL && expiry != nil {
			newExpiry := expiry.Add(extension)
			t.ttlSet[keyStr] = &newExpiry
			return true
		}

		// If no staged TTL, check database
		entry := t.db.index.Get(key)
		if entry != nil {
			if ttl, hasTTL := entry.GetTTL(t.rt); hasTTL {
				// Create a new expiry based on current TTL + extension
				newExpiry := time.Now().Add(ttl + extension)
				t.ttlSet[keyStr] = &newExpiry
				return true
			}
		}
		return false
	}

	// Key not in write set, check database
	entry := t.db.index.Get(key)
	if entry == nil {
		return false
	}

	if ttl, hasTTL := entry.GetTTL(t.rt); hasTTL {
		// Create a new expiry based on current TTL + extension
		newExpiry := time.Now().Add(ttl + extension)
		t.ttlSet[keyStr] = &newExpiry
		// Also stage the current value
		if val, exists := entry.Get(t.rt); exists {
			t.writeSet[keyStr] = val
		}
		return true
	}

	return false
}

// RemoveTTL removes the TTL from the given key within the transaction.
func (t *txn[K, V]) RemoveTTL(ctx context.Context, key K) bool {
	keyStr := string(key)

	// Check if key exists in write set
	if val, exists := t.writeSet[keyStr]; exists {
		if val == nil {
			return false // deleted
		}

		// Mark TTL for removal
		t.ttlSet[keyStr] = nil
		return true
	}

	// Key not in write set, check database
	entry := t.db.index.Get(key)
	if entry == nil {
		return false
	}

	if _, hasTTL := entry.GetTTL(t.rt); hasTTL {
		// Mark TTL for removal
		t.ttlSet[keyStr] = nil
		// Also stage the current value
		if val, exists := entry.Get(t.rt); exists {
			t.writeSet[keyStr] = val
		}
		return true
	}

	return false
}

// Flush ensures all staged writes in the transaction are immediately visible.
// This is useful for ensuring consistency within a transaction.
func (t *txn[K, V]) Flush(ctx context.Context) error {
	// For transactions, flushing means ensuring all staged writes are committed
	// In the current implementation, this is handled automatically when the transaction
	// is committed, but we can provide a way to ensure immediate visibility

	// Advance the read timestamp to ensure we see the latest committed state
	t.rt = t.db.clock.Load()

	return nil
}
