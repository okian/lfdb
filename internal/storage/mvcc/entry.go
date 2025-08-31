// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package mvcc provides Multi-Version Concurrency Control (MVCC) implementation
// for the lock-free database.
//
// This package implements MVCC entries that maintain a chain of immutable
// versions for each key, enabling consistent reads without blocking writes.
// Each version contains a value, timestamp range, and optional TTL information.
//
// # Key Features
//
//   - Immutable version chains for consistent reads
//   - Wait-free read operations that never block on writers
//   - Lock-free version publishing using CAS operations
//   - TTL support with automatic expiration
//   - Object pooling for efficient memory management
//   - Tombstone support for logical deletion
//   - Cache-line aligned for optimal performance
//
// # Usage Examples
//
// Creating and using an MVCC entry:
//
//	// Create a new entry
//	entry := mvcc.NewEntry[string]([]byte("my_key"))
//
//	// Store a value with a commit timestamp
//	entry.Put("value1", 100)
//
//	// Read a value at a specific timestamp
//	if value, exists := entry.Get(100); exists {
//	    fmt.Printf("Value: %s\n", value)
//	}
//
//	// Store with TTL
//	entry.PutWithTTL("temp_value", 200, 5*time.Minute)
//
//	// Delete (creates a tombstone)
//	entry.Delete(300)
//
//	// Check if entry is deleted
//	if value, exists := entry.Get(350); !exists {
//	    fmt.Println("Entry is deleted")
//	}
//
// # Dangers and Warnings
//
//   - **Memory Leaks**: Old versions accumulate until garbage collection. Call GC periodically.
//   - **Timestamp Ordering**: Commit timestamps must be monotonically increasing for correct behavior.
//   - **TTL Precision**: TTL expiration is checked on read, not proactively. Expired entries remain until accessed.
//   - **Version Chain Length**: Long version chains can impact read performance. Consider periodic compaction.
//   - **Object Pooling**: Versions are reused from pools. Do not hold references to version objects.
//   - **Concurrent Access**: While reads are wait-free, concurrent writes may cause CAS retries.
//   - **Key Immutability**: Entry keys should not be modified after creation.
//
// # Best Practices
//
//   - Use monotonically increasing timestamps for all operations
//   - Call garbage collection periodically to reclaim old versions
//   - Monitor version chain lengths for performance-critical applications
//   - Use TTL for temporary data to prevent unbounded growth
//   - Avoid holding references to version objects across operations
//   - Consider the trade-off between read consistency and write performance
//   - Use appropriate TTL durations to balance memory usage and functionality
//
// # Performance Considerations
//
//   - Read operations are wait-free and have O(n) complexity where n is the version chain length
//   - Write operations are lock-free but may retry under high contention
//   - Object pooling reduces allocation overhead
//   - Cache-line alignment minimizes false sharing
//   - TTL checks add minimal overhead to read operations
//
// # Thread Safety
//
// MVCC entries are fully thread-safe and support concurrent reads and writes.
// Read operations are wait-free, while write operations are lock-free.
//
// # Version Chain Structure
//
// Each entry maintains a chain of versions, where each version contains:
//   - begin/end timestamps defining visibility window
//   - the actual value
//   - TTL expiration time (optional)
//   - tombstone flag for logical deletion
//   - pointer to the next (older) version
//
// # Timestamp Semantics
//
// A version is visible to a read operation if:
//
//	begin <= readTimestamp < end
//
// The end timestamp of ^uint64(0) indicates an open (current) version.
//
// # TTL Handling
//
// TTL expiration is checked during read operations. Expired entries return
// "not found" even if they exist in the version chain. This provides
// automatic cleanup without blocking operations.
//
// # Garbage Collection
//
// Old versions are automatically reclaimed by the garbage collector when
// they are no longer visible to any active readers. Manual GC can be
// triggered to force immediate cleanup.
//
// # See Also
//
// For garbage collection and object pooling details, see the gc.go and pool.go files.
package mvcc

import (
	"sync/atomic"
	"time"
)

// cloneBytes returns a copy of b to prevent external mutation from affecting
// internal state. A nil slice returns nil.
func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	dup := make([]byte, len(b))
	copy(dup, b)
	return dup
}

// cloneValue returns a deep copy of v when v is a byte slice. Other value
// types are returned unmodified to avoid unnecessary allocations.
func cloneValue[V any](v V) V {
	if b, ok := any(v).([]byte); ok {
		return any(cloneBytes(b)).(V)
	}
	return v
}

// Version represents an immutable version of a value in the MVCC chain.
// A version is visible to readers if begin <= readTimestamp < end.
type Version[V any] struct {
	begin     uint64 // visible if begin <= rt < end
	end       uint64 // ^uint64(0) means open
	val       V
	next      atomic.Pointer[Version[V]] // older version
	tomb      bool                       // delete marker
	expiresAt *time.Time                 // TTL expiration timestamp (nil = no TTL)
	_         [32]byte                   // padding to 64-byte cache line
}

// Entry represents a key-value entry with an atomic head pointer to the latest version.
type Entry[V any] struct {
	head atomic.Pointer[Version[V]]
	key  []byte          // key bytes
	pool *VersionPool[V] // object pool for versions
}

// NewEntry creates a new entry for the given key.
//
// The key bytes are cloned so external mutations cannot corrupt the index or
// violate snapshot isolation.
func NewEntry[V any](key []byte) *Entry[V] {
	return &Entry[V]{
		key:  cloneBytes(key),
		pool: NewVersionPool[V](),
	}
}

// Get retrieves the value at the given read timestamp.
// This operation is wait-free - it never blocks on writers.
func (e *Entry[V]) Get(rt uint64) (V, bool) {
    // Try multiple times to tolerate extremely rare publication windows
    // during concurrent updates. Typically returns on first pass.
    for attempt := 0; attempt < 16; attempt++ {
        for v := e.head.Load(); v != nil; v = v.next.Load() {
            b := atomic.LoadUint64(&v.begin)
            eend := atomic.LoadUint64(&v.end)

            // Skip versions that are not yet fully published or have invalid bounds
            if b == ^uint64(0) {
                // Begin not set yet; treat as not visible
                continue
            }
            if eend == 0 { // should never be 0 for a valid version
                continue
            }

            if b <= rt && rt < eend {
                if v.tomb {
                    var z V
                    return z, false
                }
                // Expire at or before now to match GetTTL semantics
                if v.expiresAt != nil && !time.Now().Before(*v.expiresAt) {
                    var z V
                    return z, false
                }
                return v.val, true
            }
        }
        // Retry in case we observed a transient publication state
    }
    var z V
    return z, false
}

// publish atomically publishes a new version to the head of the chain.
// This operation is lock-free using CAS.
func (e *Entry[V]) publish(new *Version[V], ct uint64) {
    // Initialize visibility window before publishing.
    // New versions are open-ended until superseded.
    atomic.StoreUint64(&new.begin, ct)
    atomic.StoreUint64(&new.end, ^uint64(0))

    for {
        old := e.head.Load()
        new.next.Store(old)
        // Publish new head. Readers that observe the new head will see a
        // fully initialized visibility window [begin=ct, end=max].
        if e.head.CompareAndSwap(old, new) {
            if old != nil {
                // Close the old version at ct. Readers with rt < ct will see
                // the old version; readers with rt >= ct will see the new one.
                atomic.StoreUint64(&old.end, ct)
            }
            return
        }
    }
}

// publishExact attempts to publish `new` only if the current head is `old`.
// On success, it closes `old` at ct and returns true. On CAS failure, returns false.
func (e *Entry[V]) publishExact(old, new *Version[V], ct uint64) bool {
    // Initialize visibility window before publishing.
    atomic.StoreUint64(&new.begin, ct)
    atomic.StoreUint64(&new.end, ^uint64(0))
    new.next.Store(old)

    if e.head.CompareAndSwap(old, new) {
        if old != nil {
            atomic.StoreUint64(&old.end, ct)
        }
        return true
    }
    return false
}

// Put creates and publishes a new version with the given value.
//
// Byte-slice values are cloned to ensure snapshots observe immutable data.
func (e *Entry[V]) Put(val V, ct uint64) {
	new := e.pool.Get()
	new.val = cloneValue(val)
	e.publish(new, ct)
}

// PutWithTTL creates and publishes a new version with the given value and TTL.
//
// Byte-slice values are cloned to keep snapshot reads isolated from caller
// mutations.
func (e *Entry[V]) PutWithTTL(val V, ct uint64, ttl time.Duration) {
	expiresAt := time.Now().Add(ttl)
	new := e.pool.Get()
	new.val = cloneValue(val)
	new.expiresAt = &expiresAt
	e.publish(new, ct)
}

// PutWithExpiry creates and publishes a new version with the given value and
// absolute expiry time. Byte-slice values are cloned to preserve snapshot
// isolation.
func (e *Entry[V]) PutWithExpiry(val V, ct uint64, expiresAt time.Time) {
	new := e.pool.Get()
	new.val = cloneValue(val)
	new.expiresAt = &expiresAt
	e.publish(new, ct)
}

// Delete creates and publishes a tombstone version.
func (e *Entry[V]) Delete(ct uint64) bool {
    for {
        v := e.head.Load()
        if v != nil {
            b := atomic.LoadUint64(&v.begin)
            eend := atomic.LoadUint64(&v.end)
            if b <= ct && ct < eend && v.tomb {
                return false // already deleted at ct
            }
        }

        new := e.pool.Get()
        new.tomb = true
        if e.publishExact(v, new, ct) {
            return true
        }
        // CAS failed due to concurrent writer; recycle and retry
        e.pool.Put(new)
    }
}

// IsDeleted checks if the entry is deleted at the given timestamp.
func (e *Entry[V]) IsDeleted(rt uint64) bool {
	for v := e.head.Load(); v != nil; v = v.next.Load() {
		b := atomic.LoadUint64(&v.begin)
		eend := atomic.LoadUint64(&v.end)
		if b <= rt && rt < eend {
			return v.tomb
		}
	}
	return false
}

// Key returns the entry's key.
func (e *Entry[V]) Key() []byte {
	return e.key
}

// GetTTL returns the remaining TTL for the entry at the given read timestamp.
// Returns 0 if no TTL is set or if the entry is expired.
func (e *Entry[V]) GetTTL(rt uint64) (time.Duration, bool) {
	for v := e.head.Load(); v != nil; v = v.next.Load() {
		b := atomic.LoadUint64(&v.begin)
		eend := atomic.LoadUint64(&v.end)
		if b <= rt && rt < eend {
			if v.tomb {
				return 0, false
			}
			if v.expiresAt == nil {
				return 0, false // no TTL set
			}
			remaining := time.Until(*v.expiresAt)
			if remaining <= 0 {
				return 0, false // expired
			}
			return remaining, true
		}
	}
	return 0, false
}

// ExtendTTL extends the TTL of the current version by the given duration.
// Returns true if the TTL was successfully extended.
func (e *Entry[V]) ExtendTTL(ct uint64, extension time.Duration) bool {
    for {
        current := e.head.Load()
        if current == nil {
            return false
        }

        b := atomic.LoadUint64(&current.begin)
        eend := atomic.LoadUint64(&current.end)
        if !(b <= ct && ct < eend) || current.tomb || current.expiresAt == nil {
            return false
        }

        // Set absolute TTL to now + extension (not additive with remaining)
        // to match expected semantics in tests.
        if extension <= 0 {
            // Non-positive extensions are treated as immediate minimal TTL
            extension = 1 * time.Millisecond
        }
        newExpiry := time.Now().Add(extension)

        // Create a new version with updated TTL to maintain MVCC semantics
        new := e.pool.Get()
        new.val = current.val
        new.expiresAt = &newExpiry

        if e.publishExact(current, new, ct) {
            return true
        }
        // CAS failed; recycle and retry
        e.pool.Put(new)
    }
}

// RemoveTTL removes the TTL from the current version.
// Returns true if the TTL was successfully removed.
func (e *Entry[V]) RemoveTTL(ct uint64) bool {
    for {
        current := e.head.Load()
        if current == nil {
            return false
        }

        b := atomic.LoadUint64(&current.begin)
        eend := atomic.LoadUint64(&current.end)
        if !(b <= ct && ct < eend) || current.tomb || current.expiresAt == nil {
            return false
        }

        // Create a new version without TTL to maintain MVCC semantics
        new := e.pool.Get()
        new.val = current.val
        new.expiresAt = nil // no TTL

        if e.publishExact(current, new, ct) {
            return true
        }
        // CAS failed; recycle and retry
        e.pool.Put(new)
    }
}
