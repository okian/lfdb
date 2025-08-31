// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// DynamicHashIndex provides a hash index with dynamic bucket sizing
type DynamicHashIndex[V any] struct {
	*HashIndex[V]
	loadFactor float64
	maxLoad    float64
	entryCount atomic.Int64
	resizeMu   sync.RWMutex // Protects resize operations
}

// NewDynamicHashIndex creates a new dynamic hash index
func NewDynamicHashIndex[V any](initialSize uint64, loadFactor float64) *DynamicHashIndex[V] {
	if initialSize == 0 {
		initialSize = 1024 // default initial size
	}
	if loadFactor <= 0 {
		loadFactor = 0.75 // default load factor
	}

	return &DynamicHashIndex[V]{
		HashIndex:  NewHashIndex[V](initialSize),
		loadFactor: loadFactor,
		maxLoad:    loadFactor * float64(initialSize),
	}
}

// calculateOptimalBuckets calculates the optimal number of buckets based on data size
func calculateOptimalBuckets(dataSize int) uint64 {
	if dataSize <= 0 {
		return 1024 // minimum size
	}

	// Use 2^(log2(dataSize) + 2) for good distribution
	// This gives us roughly 4x the data size in buckets
	targetSize := 1 << (bits.Len(uint(dataSize)) + 2)

	// Ensure minimum and maximum bounds
	if targetSize < 1024 {
		targetSize = 1024
	} else if targetSize > 1<<20 { // 1M buckets max
		targetSize = 1 << 20
	}

	return uint64(targetSize) // #nosec G115
}

// GetOrCreate finds an entry for the given key, or creates a new one if it doesn't exist.
// This operation is lock-free and may trigger resizing.
func (h *DynamicHashIndex[V]) GetOrCreate(key []byte) *mvcc.Entry[V] {
	// Check if we need to resize
	h.checkResize()

	// Use read lock for accessing the hash index
	h.resizeMu.RLock()
	existingEntry := h.HashIndex.Get(key)
	h.resizeMu.RUnlock()

	if existingEntry != nil {
		return existingEntry
	}

	// Use read lock for creating new entry
	h.resizeMu.RLock()
	entry, created := h.HashIndex.GetOrCreateWithFlag(key)
	h.resizeMu.RUnlock()

	// Increment entry count only for new entries
	if created {
		h.entryCount.Add(1)
	}

	return entry
}

// checkResize checks if the index needs to be resized based on load factor
func (h *DynamicHashIndex[V]) checkResize() {
	// Use read lock to safely read size and maxLoad
	h.resizeMu.RLock()
	currentEntries := float64(h.entryCount.Load())
	maxEntries := h.maxLoad // max allowed entries before resize
	h.resizeMu.RUnlock()

	if currentEntries > maxEntries {
		h.resize()
	}
}

// resize creates a new larger hash index and migrates data
func (h *DynamicHashIndex[V]) resize() {
	// Use write lock to prevent concurrent resizes
	h.resizeMu.Lock()
	defer h.resizeMu.Unlock()

	// Double-check load factor after acquiring lock
	currentEntries := float64(h.entryCount.Load())
	if currentEntries <= h.maxLoad {
		return // Another goroutine already resized
	}

	// Calculate new size
	currentSize := h.entryCount.Load()
	newSize := calculateOptimalBuckets(int(currentSize))

	// Create new index
	newIndex := NewHashIndex[V](newSize)

	// Migrate all entries
	h.migrateEntries(newIndex)

	// Update the index atomically
	h.HashIndex = newIndex
	h.maxLoad = h.loadFactor * float64(newSize)
}

// migrateEntries migrates all entries from the old index to the new one
func (h *DynamicHashIndex[V]) migrateEntries(newIndex *HashIndex[V]) {
	// This is a simplified migration - in a production system,
	// you'd want to do this incrementally to avoid blocking

	for i := range h.buckets {
		bucket := &h.buckets[i]
		for node := bucket.Load(); node != nil; node = node.next.Load() {
			if node.entry != nil {
				// Insert existing entry pointer into the new index to preserve data
				newIndex.InsertExistingEntry(node.entry)
			}
		}
	}
}

// GetCurrentLoad returns the current load factor
func (h *DynamicHashIndex[V]) GetCurrentLoad() float64 {
	return float64(h.entryCount.Load()) / float64(h.size)
}

// GetEntryCount returns the current number of entries
func (h *DynamicHashIndex[V]) GetEntryCount() int64 {
	return h.entryCount.Load()
}

// GetBucketCount returns the current number of buckets
func (h *DynamicHashIndex[V]) GetBucketCount() uint64 {
	return h.size
}

// AdaptiveHashIndex provides adaptive sizing based on access patterns
type AdaptiveHashIndex[V any] struct {
	*DynamicHashIndex[V]
	accessCounts map[uint64]int64
	mu           sync.RWMutex
}

// NewAdaptiveHashIndex creates a new adaptive hash index
func NewAdaptiveHashIndex[V any](initialSize uint64) *AdaptiveHashIndex[V] {
	return &AdaptiveHashIndex[V]{
		DynamicHashIndex: NewDynamicHashIndex[V](initialSize, 0.75),
		accessCounts:     make(map[uint64]int64),
	}
}

// GetOrCreate finds an entry for the given key, or creates a new one if it doesn't exist.
// This operation tracks access patterns for adaptive sizing.
func (h *AdaptiveHashIndex[V]) GetOrCreate(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hash(key)

	// Track access pattern
	h.mu.Lock()
	h.accessCounts[bucketIdx]++
	h.mu.Unlock()

	return h.DynamicHashIndex.GetOrCreate(key)
}

// GetHotBuckets returns the buckets with the highest access counts
func (h *AdaptiveHashIndex[V]) GetHotBuckets(limit int) []uint64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Find buckets with highest access counts
	type bucketAccess struct {
		bucket uint64
		count  int64
	}

	var accesses []bucketAccess
	for bucket, count := range h.accessCounts {
		accesses = append(accesses, bucketAccess{bucket, count})
	}

	// Sort by access count (descending)
	// This is a simplified sort - in production, use a proper sorting algorithm
	for i := 0; i < len(accesses) && i < limit; i++ {
		for j := i + 1; j < len(accesses); j++ {
			if accesses[j].count > accesses[i].count {
				accesses[i], accesses[j] = accesses[j], accesses[i]
			}
		}
	}

	// Return top buckets
	result := make([]uint64, 0, limit)
	for i := 0; i < len(accesses) && i < limit; i++ {
		result = append(result, accesses[i].bucket)
	}

	return result
}

// OptimizeForAccessPatterns adjusts the index based on observed access patterns
func (h *AdaptiveHashIndex[V]) OptimizeForAccessPatterns() {
	hotBuckets := h.GetHotBuckets(10) // Get top 10 hot buckets

	// If we have significant hot bucket concentration, consider resizing
	if len(hotBuckets) > 0 {
		totalAccesses := int64(0)
		hotAccesses := int64(0)

		h.mu.RLock()
		for _, count := range h.accessCounts {
			totalAccesses += count
		}
		for _, bucket := range hotBuckets {
			hotAccesses += h.accessCounts[bucket]
		}
		h.mu.RUnlock()

		// If more than 50% of accesses are to hot buckets, resize
		if totalAccesses > 0 && float64(hotAccesses)/float64(totalAccesses) > 0.5 {
			h.resize()
		}
	}
}
