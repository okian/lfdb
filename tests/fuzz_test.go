// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"bytes"
	"testing"
)

// FuzzBasicOperations fuzzes basic database operations
func FuzzBasicOperations(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("key1"), []byte("value1"))
	f.Add([]byte("key2"), []byte("value2"))
	f.Add([]byte(""), []byte("empty-key"))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		ctx := context.Background()
		database := core.New[[]byte, []byte]()
		defer database.Close(ctx)

		// Test Put followed by Get
		database.Put(ctx, key, value)
		retrieved, exists := database.Get(ctx, key)

		// Invariant: Put followed by Get should return the value
		if !exists {
			t.Fatalf("Put followed by Get should return value for key %v", key)
		}
		if !bytes.Equal(retrieved, value) {
			t.Fatalf("Put followed by Get should return same value for key %v: put=%v, get=%v", key, value, retrieved)
		}

		// Test Delete
		deleted := database.Delete(ctx, key)
		if !deleted {
			t.Fatalf("Delete should return true for existing key %v", key)
		}

		// Invariant: After Delete, Get should return false
		_, exists = database.Get(ctx, key)
		if exists {
			t.Fatalf("Delete followed by Get should return false for key %v", key)
		}
	})
}

// FuzzConcurrentOperations fuzzes concurrent operations
func FuzzConcurrentOperations(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("test-key"), uint32(10))

	f.Fuzz(func(t *testing.T, key []byte, numOps uint32) {
		if numOps == 0 {
			return
		}

		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		// Limit the number of operations to prevent test from running too long
		if numOps > 1000 {
			numOps = 1000
		}

		// Perform concurrent operations
		done := make(chan bool, numOps)
		for i := uint32(0); i < numOps; i++ {
			go func(opID uint32) {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("Panic in concurrent operation %d: %v", opID, r)
					}
					done <- true
				}()

				// Mix of operations
				switch opID % 3 {
				case 0:
					database.Put(ctx, key, int(opID))
				case 1:
					database.Get(ctx, key)
				case 2:
					database.Delete(ctx, key)
				}
			}(i)
		}

		// Wait for all operations to complete
		for i := uint32(0); i < numOps; i++ {
			<-done
		}

		// Invariant: Database should still be functional after concurrent operations
		database.Put(ctx, key, 42)
		val, exists := database.Get(ctx, key)
		if !exists || val != 42 {
			t.Fatalf("Database not functional after concurrent operations")
		}
	})
}

// FuzzTransactionOperations fuzzes transaction operations
func FuzzTransactionOperations(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("txn-key"), []byte("initial"), []byte("modified"))

	f.Fuzz(func(t *testing.T, key, initialVal, modifiedVal []byte) {
		ctx := context.Background()
		database := core.New[[]byte, []byte]()
		defer database.Close(ctx)

		// Put initial value
		database.Put(ctx, key, initialVal)

		// Start transaction
		err := database.Txn(ctx, func(tx core.Txn[[]byte, []byte]) error {
			// Read initial value
			val, exists := tx.Get(ctx, key)
			if !exists {
				t.Fatalf("Transaction should see initial value for key %v", key)
			}
			if !bytes.Equal(val, initialVal) {
				t.Fatalf("Transaction should see correct initial value for key %v", key)
			}

			// Modify value
			tx.Put(ctx, key, modifiedVal)

			// Read modified value
			val, exists = tx.Get(ctx, key)
			if !exists {
				t.Fatalf("Transaction should see modified value for key %v", key)
			}
			if !bytes.Equal(val, modifiedVal) {
				t.Fatalf("Transaction should see correct modified value for key %v", key)
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}

		// Verify final state
		currentVal, currentExists := database.Get(ctx, key)
		if !currentExists {
			t.Fatalf("Final state should have value for key %v", key)
		}
		if !bytes.Equal(currentVal, modifiedVal) {
			t.Fatalf("Final state should have modified value for key %v", key)
		}
	})
}

// FuzzSnapshotOperations fuzzes snapshot operations
func FuzzSnapshotOperations(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("snapshot-key"), []byte("snapshot-value"))

	f.Fuzz(func(t *testing.T, key, value []byte) {
		ctx := context.Background()
		database := core.New[[]byte, []byte]()
		defer database.Close(ctx)

		// Put value
		database.Put(ctx, key, value)

		// Create snapshot
		snapshot := database.Snapshot(ctx)
		defer snapshot.Close(ctx)

		// Verify snapshot sees the value
		retrieved, exists := snapshot.Get(ctx, key)
		if !exists {
			t.Fatalf("Snapshot should see value for key %v", key)
		}
		if !bytes.Equal(retrieved, value) {
			t.Fatalf("Snapshot should see correct value for key %v", key)
		}

		// Modify value after snapshot
		newValue := append(value, []byte("-modified")...)
		database.Put(ctx, key, newValue)

		// Snapshot should still see old value
		retrieved, exists = snapshot.Get(ctx, key)
		if !exists {
			t.Fatalf("Snapshot should still see old value for key %v", key)
		}
		if !bytes.Equal(retrieved, value) {
			t.Fatalf("Snapshot should still see old value for key %v", key)
		}
	})
}

// FuzzEdgeCases fuzzes edge cases
func FuzzEdgeCases(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("edge-key"), byte(42))

	f.Fuzz(func(t *testing.T, key []byte, valueByte byte) {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		// Test with various edge cases
		testCases := []struct {
			name  string
			key   []byte
			value int
		}{
			{"nil key", nil, int(valueByte)},
			{"empty key", []byte{}, int(valueByte)},
			{"large key", make([]byte, 1000), int(valueByte)},
			{"normal key", key, int(valueByte)},
		}

		for _, tc := range testCases {
			// Test Put
			database.Put(ctx, tc.key, tc.value)

			// Test Get
			database.Get(ctx, tc.key)

			// Test Delete
			database.Delete(ctx, tc.key)
		}

		// Invariant: Database should still be functional after edge cases
		testKey := []byte("test-key")
		database.Put(ctx, testKey, 42)
		val, exists := database.Get(ctx, testKey)
		if !exists || val != 42 {
			t.Fatalf("Database not functional after edge cases")
		}
	})
}

// FuzzLargeData fuzzes large data operations
func FuzzLargeData(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("large-key"), uint32(100))

	f.Fuzz(func(t *testing.T, key []byte, size uint32) {
		if size == 0 || size > 10000 {
			return // Skip invalid sizes
		}

		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Create large value
		largeValue := make([]byte, size)
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}

		// Test with large data
		database.Put(ctx, key, string(largeValue))

		// Verify large data
		retrieved, exists := database.Get(ctx, key)
		if !exists {
			t.Fatalf("Large data should be retrievable for key %v", key)
		}
		if len(retrieved) != int(size) {
			t.Fatalf("Large data size mismatch for key %v: expected %d, got %d", key, size, len(retrieved))
		}

		// Test with multiple large entries
		for i := 0; i < 5; i++ {
			multiKey := append(key, byte(i))
			database.Put(ctx, multiKey, string(largeValue))
		}

		// Verify all entries
		for i := 0; i < 5; i++ {
			multiKey := append(key, byte(i))
			retrieved, exists := database.Get(ctx, multiKey)
			if !exists {
				t.Fatalf("Multiple large data should be retrievable for key %v", multiKey)
			}
			if len(retrieved) != int(size) {
				t.Fatalf("Multiple large data size mismatch for key %v", multiKey)
			}
		}
	})
}

// FuzzStressTest fuzzes stress test scenarios
func FuzzStressTest(f *testing.F) {
	// Add seed corpus
	f.Add([]byte("stress-key"), uint32(50))

	f.Fuzz(func(t *testing.T, key []byte, numOps uint32) {
		if numOps == 0 || numOps > 100 {
			return // Limit operations to prevent test from running too long
		}

		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Perform many operations
		for i := uint32(0); i < numOps; i++ {
			// Put
			database.Put(ctx, key, "value")

			// Get
			database.Get(ctx, key)

			// Delete
			database.Delete(ctx, key)

			// Put again
			database.Put(ctx, key, "value")
		}

		// Final verification
		database.Put(ctx, key, "test-value")
		val, exists := database.Get(ctx, key)
		if !exists || val != "test-value" {
			t.Fatalf("Database not functional after stress test")
		}
	})
}
