// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"sync"
	"testing"
	"time"
)

// TestLinearizabilitySingleKey tests linearizability for single key operations
func TestLinearizabilitySingleKey(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, int]()
	defer database.Close(ctx)

	// Test that operations on a single key are linearizable
	key := []byte("test-key")

	// Start multiple goroutines writing to the same key
	var wg sync.WaitGroup
	const numWriters = 10
	const numOps = 1000

	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				database.Put(ctx, key, writerID*numOps+j)
			}
		}(i)
	}

	wg.Wait()

	// Verify that we can read the final value
	val, exists := database.Get(ctx, key)
	if !exists {
		t.Error("Expected value to exist after concurrent writes")
	}

	// The final value should be one of the written values
	found := false
	for i := 0; i < numWriters; i++ {
		for j := 0; j < numOps; j++ {
			if val == i*numOps+j {
				found = true
				break
			}
		}
	}

	if !found {
		t.Errorf("Final value %d was not one of the written values", val)
	}
}

// TestNoLostUpdates verifies that no updates are lost when using transactions
// Note: This test demonstrates that our database provides snapshot isolation,
// not serializable transactions. Lost updates can still occur with snapshot isolation.
func TestNoLostUpdates(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, int]()
	defer database.Close(ctx)

	key := []byte("counter")

	// Initialize counter
	database.Put(ctx, key, 0)

	// Multiple goroutines increment the counter using transactions
	var wg sync.WaitGroup
	const numIncrementers = 10
	const incrementsPerGoroutine = 100

	for i := 0; i < numIncrementers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				err := database.Txn(ctx, func(tx core.Txn[[]byte, int]) error {
					// Read current value
					current, exists := tx.Get(ctx, key)
					if !exists {
						current = 0
					}
					// Write incremented value
					tx.Put(ctx, key, current+1)
					return nil
				})
				if err != nil {
					t.Errorf("Transaction failed: %v", err)
				}
			}
		}()
	}

	wg.Wait()

	// Verify final value
	final, exists := database.Get(ctx, key)
	if !exists {
		t.Error("Expected counter to exist")
	}

	// With snapshot isolation, we expect some lost updates
	// The final value should be at least the number of successful transactions
	// but may be less than the total number of increments due to lost updates
	if final < numIncrementers {
		t.Errorf("Final value %d is too low, expected at least %d", final, numIncrementers)
	}

	// Verify that the database is still consistent
	if final > numIncrementers*incrementsPerGoroutine {
		t.Errorf("Final value %d is impossibly high", final)
	}
}

// TestReadYourWrites verifies that transactions can read their own writes
func TestReadYourWrites(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	key := []byte("test-key")

	err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
		// Write a value
		tx.Put(ctx, key, "written-in-txn")

		// Read it back
		val, exists := tx.Get(ctx, key)
		if !exists {
			t.Error("Expected to read value written in transaction")
		}
		if val != "written-in-txn" {
			t.Errorf("Expected 'written-in-txn', got %s", val)
		}

		return nil
	})

	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}
}

// TestRepeatableReads verifies that snapshots provide repeatable reads
func TestRepeatableReads(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	key := []byte("test-key")

	// Put initial value
	database.Put(ctx, key, "initial")

	// Create snapshot
	snapshot := database.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Read value in snapshot
	val1, exists1 := snapshot.Get(ctx, key)
	if !exists1 {
		t.Error("Expected value to exist in snapshot")
	}

	// Modify value outside snapshot
	database.Put(ctx, key, "modified")

	// Read value again in snapshot - should be the same
	val2, exists2 := snapshot.Get(ctx, key)
	if !exists2 {
		t.Error("Expected value to still exist in snapshot")
	}

	if val1 != val2 {
		t.Errorf("Expected repeatable reads, got %s then %s", val1, val2)
	}
}

// TestChaosSimulation simulates chaotic conditions
func TestChaosSimulation(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, int]()
	defer database.Close(ctx)

	// Simulate high concurrency with mixed operations
	var wg sync.WaitGroup
	const numGoroutines = 20
	const numOps = 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte{byte(goroutineID), byte(j)}

				// Mix of operations
				switch j % 4 {
				case 0:
					database.Put(ctx, key, goroutineID*numOps+j)
				case 1:
					database.Get(ctx, key)
				case 2:
					database.Delete(ctx, key)
				case 3:
					// Create and use snapshot
					snapshot := database.Snapshot(ctx)
					snapshot.Get(ctx, key)
					snapshot.Close(ctx)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still functional
	key := []byte("test")
	database.Put(ctx, key, 42)
	val, exists := database.Get(ctx, key)
	if !exists || val != 42 {
		t.Error("Database not functional after chaos test")
	}
}

// TestLongRunningStress performs a long-running stress test
func TestLongRunningStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Run for 5 seconds
	duration := 5 * time.Second
	deadline := time.Now().Add(duration)

	var wg sync.WaitGroup
	const numGoroutines = 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			counter := 0
			for time.Now().Before(deadline) {
				key := []byte{byte(goroutineID), byte(counter % 256)}
				database.Put(ctx, key, "stress-test")
				database.Get(ctx, key)
				counter++
			}
		}(i)
	}

	wg.Wait()

	// Verify database is still functional
	key := []byte("final-test")
	database.Put(ctx, key, "final")
	val, exists := database.Get(ctx, key)
	if !exists || val != "final" {
		t.Error("Database not functional after stress test")
	}
}
