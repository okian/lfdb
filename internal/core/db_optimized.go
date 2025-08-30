// Licensed under the MIT License. See LICENSE file in the project root for details.

package db

import (
	"fmt"
	"sync/atomic"

	"github.com/kianostad/lfdb/internal/concurrency/epoch"
	"github.com/kianostad/lfdb/internal/monitoring/metrics"
	"github.com/kianostad/lfdb/internal/storage/index"
	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// OptimizedDB is a lock-free database with SIMD optimizations
type OptimizedDB[K ~[]byte, V any] struct {
	index  *index.OptimizedHashIndex[V]
	clock  atomic.Uint64
	gc     *mvcc.GC
	epochs *epoch.Manager
}

// NewOptimized creates a new optimized database with SIMD support
func NewOptimized[K ~[]byte, V any]() *OptimizedDB[K, V] {
	epochs := epoch.NewManager()
	gc := mvcc.NewGC(epochs)
	gc.Start() // Start the garbage collector

	db := &OptimizedDB[K, V]{
		index:  index.NewOptimizedHashIndex[V](1024),
		gc:     gc,
		epochs: epochs,
	}
	return db
}

// Get retrieves the value for the given key at the current timestamp.
func (d *OptimizedDB[K, V]) Get(key K) (V, bool) {
	rt := d.clock.Load()
	entry := d.index.Get(key)
	if entry == nil {
		var zero V
		return zero, false
	}
	return entry.Get(rt)
}

// Put stores the value for the given key at the current timestamp.
func (d *OptimizedDB[K, V]) Put(key K, value V) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)
	entry.Put(value, ct)
}

// Delete marks the key as deleted at the current timestamp.
func (d *OptimizedDB[K, V]) Delete(key K) bool {
	ct := d.clock.Add(1)
	entry := d.index.Get(key)
	if entry == nil {
		return false
	}
	entry.Delete(ct)
	return true
}

// Snapshot creates a consistent read view of the database.
func (d *OptimizedDB[K, V]) Snapshot() *OptimizedSnapshot[K, V] {
	rt := d.clock.Load()
	return &OptimizedSnapshot[K, V]{
		rt:    rt,
		index: d.index,
	}
}

// Flush ensures all pending operations are completed and the database is in a consistent state.
// For an in-memory database, this primarily ensures all pending transactions are committed
// and the garbage collector has processed any obsolete versions.
func (d *OptimizedDB[K, V]) Flush() error {
	// Force a garbage collection cycle to clean up any obsolete versions
	d.gc.ForceCollect()

	// Ensure all pending operations are visible by advancing the clock
	// This creates a barrier that ensures all previous operations are committed
	d.clock.Add(1)

	return nil
}

// ForceGC triggers forced garbage collection.
func (d *OptimizedDB[K, V]) ForceGC() error {
	d.gc.ForceCollect()
	return nil
}

// GetMetrics returns database metrics.
func (d *OptimizedDB[K, V]) GetMetrics() metrics.MetricsSnapshot {
	// Count total entries
	totalEntries := 0
	for i := uint64(0); i < d.index.Size(); i++ {
		totalEntries += d.index.BucketCount(i)
	}

	// Create a basic metrics snapshot for the optimized database
	return metrics.MetricsSnapshot{
		Operations: metrics.OperationCounts{
			Get: d.clock.Load(),
			Put: d.clock.Load(),
		},
		Errors: metrics.ErrorCounts{},
		Memory: metrics.MemoryMetrics{
			ActiveSnapshots: 0, // Would need to track this
		},
		Latency: metrics.LatencyMetrics{},
		Configuration: metrics.MetricsConfig{
			BufferSize:     0,
			LatencyBuffers: map[string]int{},
		},
	}
}

// Truncate removes all data from the database.
func (d *OptimizedDB[K, V]) Truncate() error {
	d.Flush()
	newIndex := index.NewOptimizedHashIndex[V](1024)
	d.index = newIndex
	d.clock.Store(0)
	if err := d.ForceGC(); err != nil {
		return fmt.Errorf("force GC failed: %v", err)
	}
	return nil
}

// Close closes the database and performs cleanup.
func (d *OptimizedDB[K, V]) Close() {
	d.gc.Stop()
}

// Add adds delta to the existing value at key. If key doesn't exist, creates it with delta.
func (d *OptimizedDB[K, V]) Add(key K, delta V) (V, bool) {
	ct := d.clock.Add(1)
	entry := d.index.GetOrCreate(key)

	rt := d.clock.Load()
	current, exists := entry.Get(rt)

	var newVal V
	if !exists {
		newVal = delta
	} else {
		if addable, ok := any(current).(Addable[V]); ok {
			newVal = addable.Add(delta)
		} else {
			newVal = addNumeric(current, delta)
		}
	}

	entry.Put(newVal, ct)
	return newVal, true
}

// Increment increments the value at key by 1.
func (d *OptimizedDB[K, V]) Increment(key K) (V, bool) {
	var one V
	if incrementable, ok := any(one).(Incrementable[V]); ok {
		one = incrementable.One()
	} else {
		one = createOne[V]()
	}
	return d.Add(key, one)
}

// Decrement decrements the value at key by 1.
func (d *OptimizedDB[K, V]) Decrement(key K) (V, bool) {
	var negOne V
	if incrementable, ok := any(negOne).(Incrementable[V]); ok {
		negOne = incrementable.NegativeOne()
	} else {
		// Try to create a "-1" value for common numeric types
		negOne = createNegativeOne[V]()
	}
	return d.Add(key, negOne)
}

// OptimizedSnapshot provides a consistent read view
type OptimizedSnapshot[K ~[]byte, V any] struct {
	rt    uint64
	index *index.OptimizedHashIndex[V]
}

// Get retrieves a value from the snapshot.
func (s *OptimizedSnapshot[K, V]) Get(key K) (V, bool) {
	entry := s.index.Get(key)
	if entry == nil {
		var zero V
		return zero, false
	}
	return entry.Get(s.rt)
}

// Iterate iterates over all key-value pairs in the snapshot.
func (s *OptimizedSnapshot[K, V]) Iterate(fn func(key K, value V) bool) {
	s.index.ForEach(func(key []byte, entry *mvcc.Entry[V]) bool {
		// Get the entry at the snapshot timestamp
		if val, exists := entry.Get(s.rt); exists {
			// Call the callback function
			return fn(K(key), val)
		}
		return true // Continue iteration
	})
}

// Close closes the snapshot.
func (s *OptimizedSnapshot[K, V]) Close() {
	// No cleanup needed for this implementation
}
