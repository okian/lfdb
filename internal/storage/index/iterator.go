// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package index provides iterators for traversing hash index entries.
//
// This package implements thread-safe iterators that allow efficient traversal
// of all entries in a hash index. Iterators provide consistent views of the
// data at specific read timestamps, enabling snapshot-based iteration.
//
// # Key Features
//
//   - Thread-safe iteration over hash index entries
//   - Snapshot-consistent iteration using read timestamps
//   - Efficient bucket-by-bucket traversal
//   - Support for concurrent iteration and modification
//   - Reset capability for multiple passes
//   - Memory-efficient design with minimal overhead
//
// # Usage Examples
//
// Creating and using an iterator:
//
//	// Create an iterator with a read timestamp
//	iterator := index.NewIterator(100)
//
//	// Iterate over all entries
//	for iterator.Next() {
//	    key := iterator.Key()
//	    value, exists := iterator.Value()
//	    if exists {
//	        fmt.Printf("Key: %s, Value: %v\n", string(key), value)
//	    }
//	}
//
// Using with snapshots:
//
//	snapshot := db.Snapshot(ctx)
//	defer snapshot.Close(ctx)
//
//	iterator := index.NewIterator(snapshot.Timestamp())
//	for iterator.Next() {
//	    // Process entry at snapshot timestamp
//	}
//
// Reset and reuse:
//
//	iterator := index.NewIterator(100)
//
//	// First pass
//	for iterator.Next() {
//	    // Process entries
//	}
//
//	// Reset for second pass
//	iterator.Reset()
//	for iterator.Next() {
//	    // Process entries again
//	}
//
// # Dangers and Warnings
//
//   - **Concurrent Modification**: While iterators are thread-safe, concurrent modifications may affect iteration results.
//   - **Read Timestamp**: Using an invalid or future read timestamp may return inconsistent results.
//   - **Iterator Lifetime**: Iterators should not be used after the underlying index is modified significantly.
//   - **Memory Usage**: Large datasets may consume significant memory during iteration.
//   - **Performance Impact**: Iteration over large datasets can be expensive.
//   - **Snapshot Consistency**: Iterators provide consistency only at the specified read timestamp.
//   - **Bucket Distribution**: Uneven hash distribution can cause performance variations.
//
// # Best Practices
//
//   - Use appropriate read timestamps for your use case
//   - Avoid long-running iterations on frequently modified data
//   - Consider using snapshots for consistent iteration
//   - Monitor memory usage during large iterations
//   - Reset iterators for multiple passes rather than creating new ones
//   - Handle missing values appropriately (check exists flag)
//   - Use appropriate error handling for iteration failures
//   - Consider batch processing for large datasets
//
// # Performance Considerations
//
//   - Iteration is O(n) where n is the total number of entries
//   - Bucket traversal is efficient with minimal overhead
//   - Read timestamp lookups add minimal overhead per entry
//   - Memory usage scales with the number of entries
//   - Concurrent modifications may cause iteration to skip or duplicate entries
//
// # Thread Safety
//
// Iterators are thread-safe and can be used concurrently with index modifications.
// However, the iteration results may reflect a mix of old and new data depending
// on the read timestamp and concurrent modifications.
//
// # Iteration Strategy
//
// The iterator uses a bucket-by-bucket strategy:
//   - Starts from bucket 0 and progresses sequentially
//   - Within each bucket, traverses the linked list of entries
//   - Skips empty buckets for efficiency
//   - Provides consistent view at the specified read timestamp
//
// # Read Timestamp Semantics
//
// The read timestamp determines which version of each entry is visible:
//   - Only versions with begin <= readTimestamp < end are visible
//   - Deleted entries (tombstones) are filtered out
//   - Expired TTL entries are filtered out
//   - Provides snapshot consistency for the entire iteration
//
// # Memory Management
//
// Iterators have minimal memory overhead:
//   - Only stores current position and read timestamp
//   - Does not cache entries or values
//   - References to entries are temporary
//   - No additional allocations during iteration
//
// # Use Cases
//
// Common use cases for iterators:
//   - Data export and backup
//   - Analytics and reporting
//   - Consistency checking
//   - Bulk operations
//   - Debugging and inspection
//   - Performance monitoring
//
// # Integration with Database
//
// Iterators are typically used with database snapshots:
//
//	snapshot := db.Snapshot(ctx)
//	defer snapshot.Close(ctx)
//
//	// Use snapshot timestamp for consistent iteration
//	iterator := index.NewIterator(snapshot.Timestamp())
//
// # See Also
//
// For hash index implementation details, see hash.go.
// For MVCC entry details, see the mvcc package.
package index

import (
	"github.com/kianostad/lfdb/internal/storage/mvcc"
)

// Iterator provides iteration over entries in the hash index
type Iterator[V any] struct {
	index     *HashIndex[V]
	bucketIdx uint64
	node      *node[V]
	rt        uint64 // read timestamp for MVCC
}

// NewIterator creates a new iterator for the hash index
func (h *HashIndex[V]) NewIterator(rt uint64) *Iterator[V] {
	return &Iterator[V]{
		index: h,
		rt:    rt,
	}
}

// Next advances the iterator to the next entry
func (it *Iterator[V]) Next() bool {
	// If we have a current node, move to its next
	if it.node != nil {
		it.node = it.node.next.Load()
		if it.node != nil {
			return true
		}
	}

	// Find next bucket with entries
	for it.bucketIdx < it.index.size {
		it.bucketIdx++
		if it.bucketIdx >= it.index.size {
			return false
		}

		it.node = it.index.buckets[it.bucketIdx].Load()
		if it.node != nil {
			return true
		}
	}

	return false
}

// Entry returns the current entry
func (it *Iterator[V]) Entry() *mvcc.Entry[V] {
	if it.node == nil {
		return nil
	}
	return it.node.entry
}

// Key returns the current key
func (it *Iterator[V]) Key() []byte {
	if it.node == nil {
		return nil
	}
	return it.node.entry.Key()
}

// Value returns the current value at the read timestamp
func (it *Iterator[V]) Value() (interface{}, bool) {
	if it.node == nil {
		return nil, false
	}
	return it.node.entry.Get(it.rt)
}

// Reset resets the iterator to the beginning
func (it *Iterator[V]) Reset() {
	it.bucketIdx = 0
	it.node = nil
}
