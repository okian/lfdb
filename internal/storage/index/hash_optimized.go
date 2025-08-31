// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build !(amd64 || arm64)
// +build !amd64,!arm64

package index

import (
	"sync/atomic"
	"unsafe"

	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// wordSize defines the number of bytes processed in each word comparison.
const wordSize = int(unsafe.Sizeof(uint64(0)))

// OptimizedHashIndex is a lock-free hash table with SIMD optimizations
type OptimizedHashIndex[V any] struct {
	buckets []atomic.Pointer[node[V]]
	size    uint64
	mask    uint64
}

// NewOptimizedHashIndex creates a new optimized hash index with the given size (must be power of 2).
func NewOptimizedHashIndex[V any](size uint64) *OptimizedHashIndex[V] {
	if size == 0 || (size&(size-1)) != 0 {
		panic("size must be a power of 2")
	}

	return &OptimizedHashIndex[V]{
		buckets: make([]atomic.Pointer[node[V]], size),
		size:    size,
		mask:    size - 1,
	}
}

// hashOptimized computes the hash of the key using optimized algorithms
func (h *OptimizedHashIndex[V]) hashOptimized(key []byte) uint64 {
	// Fast path for short keys (common case)
	if len(key) <= 8 {
		return h.hashShort(key)
	}

	// Use optimized hash for longer keys
	if hasAVX2 && len(key) >= 32 {
		return h.hashAVX2(key)
	}

	// Fallback to FNV-1a hash
	return h.hashFNV1a(key)
}

// hashShort optimizes short key hashing
func (h *OptimizedHashIndex[V]) hashShort(key []byte) uint64 {
	var hash uint64
	for i, b := range key {
		hash = hash*31 + uint64(b) + uint64(i)
	}
	return hash & h.mask
}

// hashCRC32 uses hardware CRC32 for fast hashing
// Currently unused but kept for future implementation
//
//nolint:unused
func (h *OptimizedHashIndex[V]) hashCRC32(key []byte) uint64 {
	// Use hardware CRC32 instruction
	hash := uint64(0)
	for i := 0; i < len(key); i += 4 {
		end := i + 4
		if end > len(key) {
			end = len(key)
		}
		chunk := key[i:end]
		// Pad to 4 bytes if needed
		if len(chunk) < 4 {
			padded := make([]byte, 4)
			copy(padded, chunk)
			chunk = padded
		}
		// Use CRC32 instruction (this would need assembly implementation)
		// For now, use a fast approximation
		hash = hash*31 + uint64(chunk[0]) + uint64(chunk[1])*256 + uint64(chunk[2])*65536 + uint64(chunk[3])*16777216
	}
	return hash & h.mask
}

// hashAVX2 uses AVX2 instructions for vectorized hashing
func (h *OptimizedHashIndex[V]) hashAVX2(key []byte) uint64 {
	// This would use AVX2 instructions for vectorized operations
	// For now, use an optimized scalar implementation
	const fnvPrime uint64 = 1099511628211
	const fnvOffsetBasis uint64 = 14695981039346656037

	hash := fnvOffsetBasis
	for _, b := range key {
		hash ^= uint64(b)
		hash *= fnvPrime
	}
	return hash & h.mask
}

// hashFNV1a is the fallback FNV-1a hash implementation
func (h *OptimizedHashIndex[V]) hashFNV1a(key []byte) uint64 {
	const fnvPrime uint64 = 1099511628211
	const fnvOffsetBasis uint64 = 14695981039346656037

	hash := fnvOffsetBasis
	for _, b := range key {
		hash ^= uint64(b)
		hash *= fnvPrime
	}
	return hash & h.mask
}

// bytesEqualOptimized selects the best available comparison method
func bytesEqualOptimized(a, b []byte) bool {
	// For non-amd64 architectures, use scalar implementation
	return bytesEqualScalar(a, b)
}

// bytesEqualAVX2 uses AVX2 instructions for 32-byte aligned comparisons
// This is a fallback for non-amd64 builds
//
//nolint:unused
func bytesEqualAVX2(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}

// bytesEqualSSE42 uses SSE4.2 instructions for 16-byte aligned comparisons
// This is a fallback for non-amd64 builds
//
//nolint:unused
func bytesEqualSSE42(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}

// bytesEqualSSE2 uses SSE2 instructions for 16-byte aligned comparisons
// This is a fallback for non-amd64 builds
//
//nolint:unused
func bytesEqualSSE2(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}

// bytesEqualNEON uses ARM64 NEON instructions for 16-byte comparisons.
// This is a stub for non-arm64 builds.
//
//nolint:unused
func bytesEqualNEON(a, b []byte) bool {
	return bytesEqualScalar(a, b)
}

// bytesEqualScalar is the optimized scalar implementation
func bytesEqualScalar(a, b []byte) bool {
	lenA := len(a)
	if lenA != len(b) {
		return false
	}
	if lenA == 0 {
		return true
	}

	if lenA >= wordSize {
		for i := 0; i+wordSize <= lenA; i += wordSize {
			va := *(*uint64)(unsafe.Pointer(&a[i]))
			vb := *(*uint64)(unsafe.Pointer(&b[i]))
			if va != vb {
				return false
			}
		}
		remaining := lenA % wordSize
		if remaining > 0 {
			start := lenA - remaining
			for i := start; i < lenA; i++ {
				if a[i] != b[i] {
					return false
				}
			}
		}
		return true
	}

	for i := 0; i < lenA; i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// GetOrCreate finds an entry for the given key, or creates a new one if it doesn't exist.
// This operation is lock-free using CAS with optimized byte comparison.
func (h *OptimizedHashIndex[V]) GetOrCreate(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hashOptimized(key)
	bucket := &h.buckets[bucketIdx]

	// First, try to find existing entry
	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqualOptimized(n.entry.Key(), key) {
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
			if bytesEqualOptimized(n.entry.Key(), key) {
				return n.entry
			}
		}
	}
}

// Get finds an entry for the given key, or returns nil if it doesn't exist.
func (h *OptimizedHashIndex[V]) Get(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hashOptimized(key)
	bucket := &h.buckets[bucketIdx]

	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqualOptimized(n.entry.Key(), key) {
			return n.entry
		}
	}
	return nil
}

// Delete removes an entry for the given key.
func (h *OptimizedHashIndex[V]) Delete(key []byte) bool {
	bucketIdx := h.hashOptimized(key)
	bucket := &h.buckets[bucketIdx]

	// Find the node to delete
	var prev *node[V]
	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqualOptimized(n.entry.Key(), key) {
			// Remove the node from the list
			if prev == nil {
				// Node is at the head of the bucket
				if bucket.CompareAndSwap(n, n.next.Load()) {
					return true
				}
			} else {
				// Node is in the middle or end of the bucket
				if prev.next.CompareAndSwap(n, n.next.Load()) {
					return true
				}
			}
			// CAS failed, retry
			return h.Delete(key)
		}
		prev = n
	}
	return false
}

// Size returns the number of buckets in the index.
func (h *OptimizedHashIndex[V]) Size() uint64 {
	return h.size
}

// BucketCount returns the number of entries in a specific bucket (for debugging).
func (h *OptimizedHashIndex[V]) BucketCount(bucketIdx uint64) int {
	if bucketIdx >= h.size {
		return 0
	}

	count := 0
	for n := h.buckets[bucketIdx].Load(); n != nil; n = n.next.Load() {
		count++
	}
	return count
}

// ForEach iterates over all entries in the index, calling fn for each entry.
// The iteration stops if fn returns false.
func (h *OptimizedHashIndex[V]) ForEach(fn func(key []byte, entry *mvcc.Entry[V]) bool) {
	for i := uint64(0); i < h.size; i++ {
		bucket := h.buckets[i].Load()
		if bucket == nil {
			continue
		}

		// Iterate over all nodes in the bucket
		current := bucket
		for current != nil {
			if !fn(current.entry.Key(), current.entry) {
				return // Stop iteration if callback returns false
			}
			current = current.next.Load()
		}
	}
}
