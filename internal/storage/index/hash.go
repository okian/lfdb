// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package index provides lock-free hash table implementations for the database.
//
// This package contains various index implementations optimized for different
// use cases and performance characteristics. The primary implementation is
// HashIndex, which provides a lock-free hash table with fixed-size buckets
// for efficient key-value storage and retrieval.
//
// # Key Features
//
//   - Lock-free hash table implementation using atomic operations
//   - Fixed-size bucket design for predictable memory usage
//   - Optimized hash function for good distribution
//   - CAS-based insertion for thread safety
//   - Support for concurrent read/write operations
//   - Memory-efficient design with minimal overhead
//
// # Usage Examples
//
// Creating and using a hash index:
//
//	// Create a hash index with 1024 buckets (must be power of 2)
//	index := index.NewHashIndex[string](1024)
//
//	// Get or create an entry for a key
//	entry := index.GetOrCreate([]byte("my_key"))
//
//	// Store a value in the entry
//	entry.Store("my_value")
//
//	// Retrieve a value
//	if value, exists := entry.Load(); exists {
//	    fmt.Printf("Value: %s\n", value)
//	}
//
//	// Get an existing entry without creating
//	if entry := index.Get([]byte("my_key")); entry != nil {
//	    // Entry exists
//	}
//
// # Dangers and Warnings
//
//   - **Bucket Size**: The number of buckets must be a power of 2. Invalid sizes will panic.
//   - **Hash Collisions**: While the hash function provides good distribution, collisions can still occur.
//   - **Memory Usage**: Each bucket consumes memory even when empty. Choose bucket count carefully.
//   - **Key Modifications**: Keys should not be modified after insertion as they are used for hash computation.
//   - **Concurrent Access**: While the index is thread-safe, modifying entries concurrently requires proper synchronization.
//   - **Entry Lifetime**: Entries remain in memory until explicitly removed or garbage collected.
//
// # Best Practices
//
//   - Choose bucket count based on expected key count (typically 2-4x the expected number of keys)
//   - Use power-of-2 bucket counts for optimal performance
//   - Avoid storing very large keys as they impact hash computation performance
//   - Use GetOrCreate for write operations and Get for read-only operations
//   - Consider the trade-off between memory usage and hash collision probability
//   - Monitor hash distribution in performance-critical applications
//   - Use appropriate key sizes (avoid extremely long keys)
//
// # Performance Considerations
//
//   - Hash computation is optimized for short keys (≤8 bytes) with a fast path
//   - Longer keys use FNV-1a hash for better distribution
//   - Bucket access is O(1) average case, O(n) worst case for hash collisions
//   - CAS operations may retry under high contention
//   - Memory overhead is minimal per entry
//
// # Thread Safety
//
// The HashIndex is fully thread-safe and supports concurrent access from multiple goroutines.
// However, individual entries may require additional synchronization depending on usage patterns.
//
// # Hash Function Details
//
// The hash function uses a hybrid approach:
//   - For keys ≤8 bytes: Custom fast hash with good distribution for short keys
//   - For keys >8 bytes: FNV-1a hash for excellent distribution and collision resistance
//
// # Memory Layout
//
// Each bucket contains a lock-free linked list of nodes, where each node contains:
//   - A pointer to an MVCC entry (the actual data)
//   - An atomic pointer to the next node in the chain
//
// # Collision Handling
//
// Hash collisions are handled using chaining with lock-free linked lists.
// New entries are inserted at the head of the bucket for optimal performance.
//
// # See Also
//
// For optimized variants and additional index implementations, see the other files in this package.
package index

import (
	"sync/atomic"

	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// node represents a node in the lock-free linked list within a bucket.
type node[V any] struct {
	entry *mvcc.Entry[V]
	next  atomic.Pointer[node[V]]
}

// HashIndex is a lock-free hash table with fixed-size buckets.
type HashIndex[V any] struct {
    buckets []atomic.Pointer[node[V]]
    size    uint64
    mask    uint64
}

// NewHashIndex creates a new hash index with the given size (must be power of 2).
func NewHashIndex[V any](size uint64) *HashIndex[V] {
	if size == 0 || (size&(size-1)) != 0 {
		panic("size must be a power of 2")
	}

	return &HashIndex[V]{
		buckets: make([]atomic.Pointer[node[V]], size),
		size:    size,
		mask:    size - 1,
	}
}

// hash computes the hash of the key and returns the bucket index.
// Optimized version using FNV-1a hash for better distribution and performance.
func (h *HashIndex[V]) hash(key []byte) uint64 {
	// Fast path for short keys (common case)
	if len(key) <= 8 {
		var hash uint64
		for i, b := range key {
			hash = hash*31 + uint64(b) + uint64(i)
		}
		return hash & h.mask
	}

	// Use FNV-1a hash for longer keys (better distribution)
	const fnvPrime uint64 = 1099511628211
	const fnvOffsetBasis uint64 = 14695981039346656037

	hash := fnvOffsetBasis
	for _, b := range key {
		hash ^= uint64(b)
		hash *= fnvPrime
	}
	return hash & h.mask
}

// GetOrCreate finds an entry for the given key, or creates a new one if it doesn't exist.
// This operation is lock-free using CAS.
func (h *HashIndex[V]) GetOrCreate(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hash(key)
	bucket := &h.buckets[bucketIdx]

	// First, try to find existing entry
	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqual(n.entry.Key(), key) {
			return n.entry
		}
	}

	// Create new entry and node
	entry := mvcc.NewEntry[V](key)
	newNode := &node[V]{
		entry: entry,
	}

	// Try to insert at head of bucket
	for {
		oldHead := bucket.Load()
		newNode.next.Store(oldHead)
		if bucket.CompareAndSwap(oldHead, newNode) {
			return entry
		}

		// CAS failed, check if someone else inserted our key
		for n := bucket.Load(); n != nil; n = n.next.Load() {
			if bytesEqual(n.entry.Key(), key) {
				return n.entry
			}
		}
	}
}

// GetOrCreateWithFlag is like GetOrCreate but returns whether a new entry was created.
// The boolean is true only when this call inserted a new entry into the index.
func (h *HashIndex[V]) GetOrCreateWithFlag(key []byte) (*mvcc.Entry[V], bool) {
    bucketIdx := h.hash(key)
    bucket := &h.buckets[bucketIdx]

    // First, try to find existing entry
    for n := bucket.Load(); n != nil; n = n.next.Load() {
        if bytesEqual(n.entry.Key(), key) {
            return n.entry, false
        }
    }

    // Create new entry and node
    entry := mvcc.NewEntry[V](key)
    newNode := &node[V]{
        entry: entry,
    }

    // Try to insert at head of bucket
    for {
        oldHead := bucket.Load()
        newNode.next.Store(oldHead)
        if bucket.CompareAndSwap(oldHead, newNode) {
            return entry, true
        }

        // CAS failed, check if someone else inserted our key
        for n := bucket.Load(); n != nil; n = n.next.Load() {
            if bytesEqual(n.entry.Key(), key) {
                return n.entry, false
            }
        }
    }
}

// InsertExistingEntry inserts an existing MVCC entry into the index if absent.
// Returns true if the entry was inserted, false if an entry with the same key already existed.
func (h *HashIndex[V]) InsertExistingEntry(entry *mvcc.Entry[V]) bool {
    if entry == nil {
        return false
    }
    key := entry.Key()
    bucketIdx := h.hash(key)
    bucket := &h.buckets[bucketIdx]

    // Check if an entry with the same key already exists
    for n := bucket.Load(); n != nil; n = n.next.Load() {
        if bytesEqual(n.entry.Key(), key) {
            return false
        }
    }

    // Insert node pointing to the existing entry
    newNode := &node[V]{entry: entry}
    for {
        oldHead := bucket.Load()
        newNode.next.Store(oldHead)
        if bucket.CompareAndSwap(oldHead, newNode) {
            return true
        }
        // If CAS failed, recheck for existence
        for n := bucket.Load(); n != nil; n = n.next.Load() {
            if bytesEqual(n.entry.Key(), key) {
                return false
            }
        }
    }
}

// Get finds an entry for the given key without creating a new one.
func (h *HashIndex[V]) Get(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hash(key)
	bucket := &h.buckets[bucketIdx]

	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqual(n.entry.Key(), key) {
			return n.entry
		}
	}
	return nil
}

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Size returns the number of buckets in the index.
func (h *HashIndex[V]) Size() uint64 {
	return h.size
}

// BucketCount returns the number of entries in a specific bucket (for debugging).
func (h *HashIndex[V]) BucketCount(bucketIdx uint64) int {
	if bucketIdx >= h.size {
		return 0
	}

	count := 0
	for n := h.buckets[bucketIdx].Load(); n != nil; n = n.next.Load() {
		count++
	}
	return count
}
