// Licensed under the MIT License. See LICENSE file in the project root for details.

//go:build arm64
// +build arm64

package index

import (
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/cpu"

	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// CPU feature flags for optimization
var (
	hasNEON = cpu.ARM64.HasASIMD
)

// OptimizedHashIndex is a lock-free hash table with SIMD optimizations
// identical to the amd64 variant but tailored for arm64.
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

// bytesEqualOptimized selects the best available comparison method.
func bytesEqualOptimized(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	if hasNEON && len(a) >= 16 {
		return bytesEqualNEON(a, b)
	}
	return bytesEqualScalar(a, b)
}

// bytesEqualNEON uses ARM64 NEON instructions for 16-byte comparisons.
// Assembly implementation is in bytes_equal_neon_arm64.s.
func bytesEqualNEON(a, b []byte) bool

// bytesEqualScalar is the optimized scalar implementation shared with other architectures.
func bytesEqualScalar(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	if len(a) >= 8 {
		for i := 0; i <= len(a)-8; i += 8 {
			va := *(*uint64)(unsafe.Pointer(&a[i]))
			vb := *(*uint64)(unsafe.Pointer(&b[i]))
			if va != vb {
				return false
			}
		}
		remaining := len(a) % 8
		if remaining > 0 {
			start := len(a) - remaining
			for i := start; i < len(a); i++ {
				if a[i] != b[i] {
					return false
				}
			}
		}
		return true
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// hashOptimized computes the hash of the key using optimized algorithms.
func (h *OptimizedHashIndex[V]) hashOptimized(key []byte) uint64 {
	if len(key) <= 8 {
		return h.hashShort(key)
	}
	if hasNEON && len(key) >= 32 {
		// Future NEON-optimized hash could be placed here.
	}
	return h.hashFNV1a(key)
}

// hashShort optimizes short key hashing.
func (h *OptimizedHashIndex[V]) hashShort(key []byte) uint64 {
	var hash uint64
	for i, b := range key {
		hash = hash*31 + uint64(b) + uint64(i)
	}
	return hash & h.mask
}

// hashFNV1a is the fallback FNV-1a hash implementation.
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

// GetOrCreate finds an entry for the given key, or creates a new one if it doesn't exist.
// This operation is lock-free using CAS with optimized byte comparison.
func (h *OptimizedHashIndex[V]) GetOrCreate(key []byte) *mvcc.Entry[V] {
	bucketIdx := h.hashOptimized(key)
	bucket := &h.buckets[bucketIdx]

	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqualOptimized(n.entry.Key(), key) {
			return n.entry
		}
	}

	entry := mvcc.NewEntry[V](key)
	newNode := &node[V]{
		entry: entry,
	}

	for {
		oldHead := bucket.Load()
		newNode.next.Store(oldHead)
		if bucket.CompareAndSwap(oldHead, newNode) {
			return entry
		}

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

	var prev *node[V]
	for n := bucket.Load(); n != nil; n = n.next.Load() {
		if bytesEqualOptimized(n.entry.Key(), key) {
			if prev == nil {
				if bucket.CompareAndSwap(n, n.next.Load()) {
					return true
				}
			} else {
				if prev.next.CompareAndSwap(n, n.next.Load()) {
					return true
				}
			}
			return h.Delete(key)
		}
		prev = n
	}
	return false
}
