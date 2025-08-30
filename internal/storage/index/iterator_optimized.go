// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"sync"
	"sync/atomic"

	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// ReadAheadIterator provides optimized sequential access with read-ahead capabilities
type ReadAheadIterator[V any] struct {
	index     *HashIndex[V]
	rt        uint64
	bucketIdx int
	entryIdx  int
	buckets   []atomic.Pointer[node[V]]

	// Read-ahead buffer
	buffer     []*mvcc.Entry[V]
	bufferIdx  int
	bufferSize int

	// Prefetch state
	prefetching bool
	prefetchMu  sync.Mutex
	prefetchWg  sync.WaitGroup
}

// NewReadAheadIterator creates a new read-ahead iterator
func NewReadAheadIterator[V any](index *HashIndex[V], rt uint64, bufferSize int) *ReadAheadIterator[V] {
	if bufferSize <= 0 {
		bufferSize = 100 // default buffer size
	}

	iterator := &ReadAheadIterator[V]{
		index:       index,
		rt:          rt,
		buckets:     index.buckets,
		buffer:      make([]*mvcc.Entry[V], 0, bufferSize),
		bufferSize:  bufferSize,
		prefetching: false,
	}

	// Start initial prefetch
	iterator.startPrefetch()

	return iterator
}

// startPrefetch begins prefetching entries into the buffer
func (it *ReadAheadIterator[V]) startPrefetch() {
	it.prefetchMu.Lock()
	if it.prefetching {
		it.prefetchMu.Unlock()
		return
	}
	it.prefetching = true
	it.prefetchMu.Unlock()

	it.prefetchWg.Add(1)
	go func() {
		defer it.prefetchWg.Done()
		it.prefetchEntries()
	}()
}

// prefetchEntries fills the buffer with entries from the current position
func (it *ReadAheadIterator[V]) prefetchEntries() {
	entries := make([]*mvcc.Entry[V], 0, it.bufferSize)

	// Start from current position
	bucketIdx := it.bucketIdx
	entryIdx := it.entryIdx

	for bucketIdx < len(it.buckets) {
		bucket := it.buckets[bucketIdx].Load()
		if bucket == nil {
			bucketIdx++
			entryIdx = 0
			continue
		}

		// Get entries from current bucket
		bucketEntries := it.getBucketEntries(bucket, entryIdx)
		entries = append(entries, bucketEntries...)

		// Move to next bucket if we've exhausted this one
		if len(bucketEntries) == 0 {
			bucketIdx++
			entryIdx = 0
		} else {
			// Still in current bucket
			entryIdx += len(bucketEntries)
		}
	}

	// Update buffer and position atomically
	it.prefetchMu.Lock()
	it.buffer = entries
	it.bufferIdx = 0
	it.bucketIdx = bucketIdx
	it.entryIdx = entryIdx
	it.prefetching = false
	it.prefetchMu.Unlock()
}

// getBucketEntries extracts entries from a bucket starting from the given index
func (it *ReadAheadIterator[V]) getBucketEntries(bucket *node[V], startIdx int) []*mvcc.Entry[V] {
	entries := make([]*mvcc.Entry[V], 0)

	// Traverse the bucket list
	current := bucket
	idx := 0

	for current != nil && idx < startIdx {
		current = current.next.Load()
		idx++
	}

	// Collect entries from current position
	for current != nil {
		if current.entry != nil {
			entries = append(entries, current.entry)
		}
		current = current.next.Load()
	}

	return entries
}

// Next advances the iterator to the next entry
func (it *ReadAheadIterator[V]) Next() bool {
	it.prefetchMu.Lock()

	// Check if we have entries in buffer
	if it.bufferIdx < len(it.buffer) {
		it.bufferIdx++

		// Start prefetching if buffer is getting low
		if it.bufferIdx >= len(it.buffer)-it.bufferSize/2 && !it.prefetching {
			it.prefetching = true
			it.prefetchMu.Unlock()
			it.startPrefetch()
		} else {
			it.prefetchMu.Unlock()
		}
		return true
	}

	it.prefetchMu.Unlock()

	// Wait for prefetch to complete
	it.prefetchWg.Wait()

	// Check again after prefetch
	it.prefetchMu.Lock()
	defer it.prefetchMu.Unlock()

	if it.bufferIdx < len(it.buffer) {
		it.bufferIdx++
		return true
	}

	return false
}

// Entry returns the current entry
func (it *ReadAheadIterator[V]) Entry() *mvcc.Entry[V] {
	it.prefetchMu.Lock()
	defer it.prefetchMu.Unlock()

	if it.bufferIdx > 0 && it.bufferIdx <= len(it.buffer) {
		return it.buffer[it.bufferIdx-1]
	}
	return nil
}

// Key returns the current key
func (it *ReadAheadIterator[V]) Key() []byte {
	entry := it.Entry()
	if entry != nil {
		return entry.Key()
	}
	return nil
}

// Value returns the current value
func (it *ReadAheadIterator[V]) Value() (V, bool) {
	entry := it.Entry()
	if entry != nil {
		return entry.Get(it.rt)
	}
	var zero V
	return zero, false
}

// Reset resets the iterator to the beginning
func (it *ReadAheadIterator[V]) Reset() {
	it.prefetchMu.Lock()
	it.bucketIdx = 0
	it.entryIdx = 0
	it.bufferIdx = 0
	it.buffer = it.buffer[:0]
	it.prefetching = false
	it.prefetchMu.Unlock()

	// Start new prefetch (without holding the mutex)
	it.startPrefetch()
}

// Close cleans up the iterator
func (it *ReadAheadIterator[V]) Close() {
	it.prefetchWg.Wait()
}

// OptimizedIterator provides a high-performance iterator with read-ahead
type OptimizedIterator[V any] struct {
	*ReadAheadIterator[V]
}

// NewOptimizedIterator creates a new optimized iterator
func NewOptimizedIterator[V any](index *HashIndex[V], rt uint64) *OptimizedIterator[V] {
	return &OptimizedIterator[V]{
		ReadAheadIterator: NewReadAheadIterator(index, rt, 100),
	}
}

// ForEach executes a function for each entry in the iterator
func (it *OptimizedIterator[V]) ForEach(fn func(key []byte, value V) bool) {
	for it.Next() {
		key := it.Key()
		value, found := it.Value()
		if found {
			if !fn(key, value) {
				break
			}
		}
	}
}

// Collect collects all entries into slices
func (it *OptimizedIterator[V]) Collect() ([]byte, []V) {
	var keys [][]byte
	var values []V

	it.ForEach(func(key []byte, value V) bool {
		keys = append(keys, key)
		values = append(values, value)
		return true
	})

	// Flatten keys
	flatKeys := make([]byte, 0, len(keys)*8) // estimate key size
	for _, key := range keys {
		flatKeys = append(flatKeys, key...)
	}

	return flatKeys, values
}
