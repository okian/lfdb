// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package epoch provides epoch-based memory reclamation for the lock-free database.
//
// This package implements an epoch manager that tracks active snapshots and
// provides the minimum active timestamp for safe garbage collection. It enables
// the database to safely reclaim memory from old versions that are no longer
// visible to any active readers.
//
// # Key Features
//
//   - Tracks active snapshot timestamps for safe memory reclamation
//   - Provides minimum active timestamp for garbage collection
//   - Thread-safe registration and unregistration of snapshots
//   - Efficient timestamp tracking with reference counting
//   - Enables lock-free garbage collection without ABA problems
//
// # Usage Examples
//
// Creating and using an epoch manager:
//
//	// Create a new epoch manager
//	manager := epoch.NewManager()
//
//	// Register a snapshot with timestamp 100
//	manager.Register(100)
//
//	// Get the minimum active timestamp
//	minActive := manager.MinActive() // Returns 100
//
//	// Unregister the snapshot when done
//	manager.Unregister(100)
//
//	// Check active snapshot count
//	count := manager.ActiveCount() // Returns 0
//
// # Dangers and Warnings
//
//   - **Registration Order**: Each Register() call must have a corresponding Unregister() call.
//   - **Timestamp Validity**: Only valid, monotonically increasing timestamps should be registered.
//   - **Memory Leaks**: Failing to unregister snapshots will prevent garbage collection.
//   - **Concurrent Access**: While the manager is thread-safe, improper usage can lead to memory leaks.
//   - **Timestamp Reuse**: Reusing timestamps can lead to incorrect garbage collection decisions.
//   - **Performance Impact**: Frequent registration/unregistration can impact performance.
//
// # Best Practices
//
//   - Always unregister snapshots when they are no longer needed
//   - Use monotonically increasing timestamps for all snapshots
//   - Minimize the number of active snapshots to improve garbage collection efficiency
//   - Consider using a defer statement for unregistration in error-prone code
//   - Monitor active snapshot count to detect potential memory leaks
//   - Use appropriate timeout mechanisms for long-running snapshots
//   - Avoid creating snapshots with very old timestamps
//
// # Performance Considerations
//
//   - Registration and unregistration are O(1) operations
//   - MinActive() is O(n) where n is the number of active timestamps
//   - ActiveCount() is O(1) operation
//   - Memory usage scales with the number of active timestamps
//   - Lock contention increases with frequent concurrent access
//
// # Thread Safety
//
// The epoch manager is fully thread-safe and supports concurrent registration
// and unregistration from multiple goroutines. All operations are protected
// by appropriate read/write locks.
//
// # Memory Reclamation Strategy
//
// The epoch manager enables safe memory reclamation by:
//   - Tracking all active snapshot timestamps
//   - Providing the minimum active timestamp to garbage collectors
//   - Allowing garbage collectors to safely reclaim versions older than the minimum
//   - Preventing ABA problems in lock-free data structures
//
// # Garbage Collection Integration
//
// The minimum active timestamp is used by garbage collectors to determine
// which versions can be safely reclaimed:
//
//	minActive := manager.MinActive()
//	// Reclaim all versions with end timestamp < minActive
//
// # Reference Counting
//
// The manager uses reference counting to handle multiple snapshots with
// the same timestamp. This is important for scenarios where multiple
// operations might use the same timestamp.
//
// # See Also
//
// For garbage collection implementation details, see the mvcc package.
package epoch

import (
	"sync"
)

// Manager tracks active snapshots and provides the minimum active timestamp
// for garbage collection purposes.
type Manager struct {
	activeTS map[uint64]int // timestamp -> count of active snapshots
	mu       sync.RWMutex
}

// NewManager creates a new epoch manager.
func NewManager() *Manager {
	return &Manager{
		activeTS: make(map[uint64]int),
	}
}

// Register adds a timestamp to the active set.
func (m *Manager) Register(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activeTS[ts]++
}

// Unregister removes a timestamp from the active set.
func (m *Manager) Unregister(ts uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if count, exists := m.activeTS[ts]; exists {
		if count <= 1 {
			delete(m.activeTS, ts)
		} else {
			m.activeTS[ts] = count - 1
		}
	}
}

// MinActive returns the minimum active timestamp.
// If no snapshots are active, returns 0.
func (m *Manager) MinActive() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.activeTS) == 0 {
		return 0
	}

	min := ^uint64(0)
	for ts := range m.activeTS {
		if ts < min {
			min = ts
		}
	}
	return min
}

// ActiveCount returns the number of active snapshots.
func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.activeTS)
}
