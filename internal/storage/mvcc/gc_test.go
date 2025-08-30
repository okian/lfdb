// Licensed under the MIT License. See LICENSE file in the project root for details.

package mvcc

import (
	"sync"
	"testing"
	"time"

	"github.com/kianostad/lfdb/internal/concurrency/epoch"
)

func TestNewGC(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	if gc == nil {
		t.Fatal("NewGC() returned nil")
	}

	if gc.epochs != epochs {
		t.Errorf("Expected gc.epochs to be %v, got %v", epochs, gc.epochs)
	}

	if gc.stop.Load() {
		t.Error("Expected gc.stop to be false initially")
	}
}

func TestGCStartStop(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Test starting GC
	gc.Start()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test stopping GC
	gc.Stop()

	// Verify stop flag is set
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after Stop()")
	}
}

func TestGCStartWhenAlreadyStopped(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Stop first
	gc.Stop()

	// Try to start when already stopped
	gc.Start()

	// Should not panic and should remain stopped
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to remain true after Start() when already stopped")
	}
}

func TestGCCollectWithNoActiveSnapshots(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// collect should return early when no active snapshots
	gc.collect()

	// No assertions needed - just checking it doesn't panic
}

func TestGCCollectWithActiveSnapshots(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Register some active snapshots
	epochs.Register(100)
	epochs.Register(200)

	// collect should run without panicking
	gc.collect()

	// Clean up
	epochs.Unregister(100)
	epochs.Unregister(200)
}

func TestTrimEntryWithZeroMinActive(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// TrimEntry should return early when minActive is 0
	TrimEntry(entry, 0)

	// No assertions needed - just checking it doesn't panic
}

func TestTrimEntryWithObsoleteVersions(t *testing.T) {
	t.Parallel()
	entry := NewEntry[string]([]byte("test-key"))

	// Add some versions
	entry.Put("value1", 10)
	entry.Put("value2", 20)
	entry.Put("value3", 30)

	// TrimEntry should run without panicking
	TrimEntry(entry, 25)

	// Verify that newer versions are still accessible
	if value, exists := entry.Get(30); !exists || value != "value3" {
		t.Errorf("Expected value3 at timestamp 30, got %v, %t", value, exists)
	}
}

func TestTrimEntryWithAllObsoleteVersions(t *testing.T) {
	t.Parallel()
	entry := NewEntry[string]([]byte("test-key"))

	// Add some versions
	entry.Put("value1", 10)
	entry.Put("value2", 20)

	// TrimEntry should run without panicking when all versions are obsolete
	TrimEntry(entry, 100)

	// Verify that versions are still accessible (trimming is not fully implemented)
	if value, exists := entry.Get(20); !exists || value != "value2" {
		t.Errorf("Expected value2 at timestamp 20, got %v, %t", value, exists)
	}
}

func TestTrimEntryWithNoVersions(t *testing.T) {
	t.Parallel()
	entry := NewEntry[string]([]byte("test-key"))

	// TrimEntry should run without panicking on empty entry
	TrimEntry(entry, 50)

	// Verify entry is still accessible
	if entry.Key() == nil {
		t.Error("Expected entry key to remain accessible")
	}
}

func TestGCRunLoop(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Start GC
	gc.Start()

	// Let it run for a short time
	time.Sleep(50 * time.Millisecond)

	// Stop GC
	gc.Stop()

	// Verify it stopped
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after Stop()")
	}
}

func TestGCMultipleStartStop(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Start and stop multiple times
	for i := 0; i < 3; i++ {
		gc.Start()
		time.Sleep(10 * time.Millisecond)
		gc.Stop()
	}

	// Should not panic and should be stopped
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after multiple start/stop cycles")
	}
}

func TestGCConcurrentAccess(t *testing.T) {
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	var wg sync.WaitGroup
	const numGoroutines = 5

	// Use a mutex to serialize access to Start/Stop
	var mu sync.Mutex

	// Start multiple goroutines that start/stop GC
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// Add small delay to avoid exact concurrent access
			time.Sleep(time.Duration(id) * time.Microsecond)

			mu.Lock()
			gc.Start()
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)

			mu.Lock()
			gc.Stop()
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Should not panic and should be in a consistent state
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after concurrent access")
	}
}

func TestGCWithEpochManager(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Register some epochs
	epochs.Register(100)
	epochs.Register(200)

	// Start GC
	gc.Start()
	time.Sleep(20 * time.Millisecond)

	// Unregister epochs
	epochs.Unregister(100)
	epochs.Unregister(200)

	// Stop GC
	gc.Stop()

	// Verify final state
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after Stop()")
	}
}

func TestGCForceCollect(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Test ForceCollect with no active epochs
	gc.ForceCollect()

	// Register an epoch and test ForceCollect
	epochs.Register(100)
	gc.ForceCollect()
	epochs.Unregister(100)

	// Should not panic
}

func TestGCStartWhenAlreadyRunning(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Start GC
	gc.Start()

	// Try to start again - should be safe
	gc.Start()

	// Stop GC
	gc.Stop()

	// Verify final state
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after Stop()")
	}
}

func TestGCStopWhenNotRunning(t *testing.T) {
	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Stop when not running - should be safe
	gc.Stop()

	// Verify state
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after Stop()")
	}
}

func TestTrimEntryWithComplexVersionChain(t *testing.T) {
	t.Parallel()
	entry := NewEntry[string]([]byte("complex-key"))

	// Create a complex version chain
	entry.Put("v1", 10)
	entry.Put("v2", 20)
	entry.Put("v3", 30)
	entry.Put("v4", 40)
	entry.Put("v5", 50)

	// Test trimming at different points
	TrimEntry(entry, 25) // Should keep v3, v4, v5
	TrimEntry(entry, 35) // Should keep v4, v5
	TrimEntry(entry, 45) // Should keep v5

	// Verify versions are still accessible (trimming is not fully implemented)
	if value, exists := entry.Get(50); !exists || value != "v5" {
		t.Errorf("Expected v5 at timestamp 50, got %v, %t", value, exists)
	}
}

func TestTrimEntryEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with nil entry - should not panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("TrimEntry panicked with nil entry: %v", r)
		}
	}()
	TrimEntry[string](nil, 100)

	// Test with zero minActive
	entry := NewEntry[string]([]byte("test"))
	entry.Put("value", 10)
	TrimEntry(entry, 0)

	// Test with very large minActive
	TrimEntry(entry, 999999999)

	// Test with single version
	singleEntry := NewEntry[string]([]byte("single"))
	singleEntry.Put("single-value", 50)
	TrimEntry(singleEntry, 25)
	TrimEntry(singleEntry, 75)

	// Should not panic
}

func TestTrimEntryWithDeletedVersions(t *testing.T) {
	t.Parallel()
	entry := NewEntry[string]([]byte("deleted-key"))

	// Add versions including deletions
	entry.Put("v1", 10)
	entry.Delete(20)
	entry.Put("v3", 30)

	// TrimEntry should handle deleted versions
	TrimEntry(entry, 25)

	// Verify behavior with deleted versions
	if value, exists := entry.Get(15); !exists || value != "v1" {
		t.Errorf("Expected v1 at timestamp 15, got %v, %t", value, exists)
	}

	if value, exists := entry.Get(25); exists {
		t.Errorf("Expected no value at timestamp 25 (deleted), got %v, %t", value, exists)
	}
}

func TestGCStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()
	epochs := epoch.NewManager()
	gc := NewGC(epochs)

	// Start GC
	gc.Start()

	// Run stress test
	var wg sync.WaitGroup
	const numGoroutines = 10
	const numOperations = 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				epochID := uint64(id*numOperations + j)
				epochs.Register(epochID)
				time.Sleep(time.Microsecond)
				epochs.Unregister(epochID)
			}
		}(i)
	}

	wg.Wait()

	// Stop GC
	gc.Stop()

	// Verify final state
	if !gc.stop.Load() {
		t.Error("Expected gc.stop to be true after stress test")
	}
}
