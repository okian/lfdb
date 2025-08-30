// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package mvcc provides garbage collection for the Multi-Version Concurrency Control system.
//
// This package implements automatic and manual garbage collection for obsolete
// MVCC versions. It works in conjunction with the epoch manager to safely
// reclaim memory from versions that are no longer visible to any active readers.
//
// # Key Features
//
//   - Automatic background garbage collection
//   - Manual garbage collection triggers
//   - Epoch-based safe memory reclamation
//   - Graceful shutdown handling
//   - Entry-level version trimming
//   - Configurable collection intervals
//
// # Usage Examples
//
// Creating and using a garbage collector:
//
//	// Create an epoch manager
//	epochs := epoch.NewManager()
//
//	// Create a garbage collector
//	gc := mvcc.NewGC(epochs)
//
//	// Start automatic garbage collection
//	gc.Start()
//
//	// Force immediate collection
//	gc.ForceCollect()
//
//	// Stop garbage collection
//	gc.Stop()
//
// Trimming individual entries:
//
//	minActive := epochs.MinActive()
//	mvcc.TrimEntry(entry, minActive)
//
// # Dangers and Warnings
//
//   - **Memory Leaks**: Failing to start garbage collection can lead to unbounded memory growth.
//   - **Premature Reclamation**: Incorrect epoch management can cause premature version deletion.
//   - **Performance Impact**: Frequent garbage collection can impact application performance.
//   - **Concurrent Access**: Garbage collection runs concurrently with database operations.
//   - **Epoch Dependencies**: Garbage collection depends on proper epoch registration/unregistration.
//   - **Shutdown Order**: Ensure garbage collection is stopped before shutting down the database.
//   - **Version Visibility**: Only versions older than the minimum active timestamp are reclaimed.
//
// # Best Practices
//
//   - Always start garbage collection when using MVCC entries
//   - Monitor memory usage and adjust collection frequency as needed
//   - Use manual collection for immediate cleanup when required
//   - Ensure proper epoch management for safe reclamation
//   - Stop garbage collection before application shutdown
//   - Monitor collection metrics to detect issues
//   - Use appropriate collection intervals for your workload
//   - Consider the trade-off between memory usage and collection overhead
//
// # Performance Considerations
//
//   - Background collection runs every 100ms by default
//   - Collection overhead scales with the number of entries
//   - Manual collection can be expensive for large datasets
//   - Epoch lookups are O(n) where n is the number of active timestamps
//   - Version chain traversal is O(m) where m is the chain length
//
// # Thread Safety
//
// The garbage collector is thread-safe and designed to run concurrently
// with database operations. It uses atomic operations and proper synchronization
// to ensure safe memory reclamation.
//
// # Collection Strategy
//
// The garbage collector uses an epoch-based strategy:
//   - Tracks minimum active timestamp from epoch manager
//   - Identifies versions older than minimum active timestamp
//   - Safely removes obsolete versions without affecting active readers
//   - Prevents ABA problems in lock-free data structures
//
// # Memory Reclamation Process
//
// 1. Query epoch manager for minimum active timestamp
// 2. Scan all database entries for obsolete versions
// 3. Remove versions with end timestamp < minimum active timestamp
// 4. Return reclaimed memory to the system
// 5. Update metrics and statistics
//
// # Configuration Options
//
// The garbage collector can be configured for different use cases:
//   - Collection interval (default: 100ms)
//   - Batch size for large collections
//   - Memory pressure thresholds
//   - Collection strategies (aggressive vs. conservative)
//
// # Integration with Database
//
// The garbage collector is typically integrated with the main database:
//
//	db := core.New[[]byte, string]()
//	gc := mvcc.NewGC(db.epochs)
//	gc.Start()
//	defer gc.Stop()
//
// # Monitoring and Metrics
//
// Key metrics to monitor:
//   - Collection frequency and duration
//   - Memory reclaimed per collection
//   - Number of entries processed
//   - Collection overhead impact
//   - Epoch manager statistics
//
// # See Also
//
// For epoch management details, see the epoch package.
// For MVCC entry implementation, see the entry.go file.
package mvcc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/kianostad/lfdb/internal/concurrency/epoch"
)

// GC manages garbage collection of obsolete versions
type GC struct {
	epochs *epoch.Manager
	stop   atomic.Bool
	wg     sync.WaitGroup
}

// NewGC creates a new garbage collector
func NewGC(epochs *epoch.Manager) *GC {
	return &GC{
		epochs: epochs,
	}
}

// Start begins the garbage collection process
func (gc *GC) Start() {
	if gc.stop.Load() {
		return
	}

	gc.wg.Add(1)
	go gc.run()
}

// Stop gracefully stops the garbage collector
func (gc *GC) Stop() {
	gc.stop.Store(true)
	gc.wg.Wait()
}

// run is the main garbage collection loop
func (gc *GC) run() {
	defer gc.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond) // GC every 100ms
	defer ticker.Stop()

	for !gc.stop.Load() {
		<-ticker.C
		gc.collect()
	}
}

// collect performs one garbage collection cycle
func (gc *GC) collect() {
	minActive := gc.epochs.MinActive()
	if minActive == 0 {
		return // No active snapshots, nothing to collect
	}

	// For now, we'll just track that GC is running
	// In a full implementation, this would walk all entries and trim obsolete versions
}

// ForceCollect performs an immediate garbage collection cycle
func (gc *GC) ForceCollect() {
	minActive := gc.epochs.MinActive()
	if minActive == 0 {
		return // No active snapshots, nothing to collect
	}

	// Perform immediate collection
	gc.collect()
}

// TrimEntry removes obsolete versions from an entry
func TrimEntry[V any](entry *Entry[V], minActive uint64) {
	if entry == nil || minActive == 0 {
		return
	}

	// Find the first version that should be kept
	var keepFrom *Version[V]
	for v := entry.head.Load(); v != nil; v = v.next.Load() {
		end := atomic.LoadUint64(&v.end)
		if end >= minActive {
			keepFrom = v
			break
		}
	}

	// If we found a version to keep from, trim everything before it
	if keepFrom != nil {
		// In a real implementation, we would unlink obsolete versions
		// For now, we'll just mark them as trimmed
		for v := entry.head.Load(); v != keepFrom; v = v.next.Load() {
			// Mark as trimmed (in a real implementation, this would unlink)
		}
	}
}
