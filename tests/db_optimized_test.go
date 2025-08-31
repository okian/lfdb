// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"sync"
	"testing"

	core "github.com/kianostad/lfdb/internal/core"
)

func TestOptimizedDBBasicOperations(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Test Put and Get
	key := []byte("test-key")
	value := "test-value"
	database.Put(key, value)

	retrieved, exists := database.Get(key)
	if !exists {
		t.Fatal("Expected value to exist")
	}
	if retrieved != value {
		t.Errorf("Expected %s, got %s", value, retrieved)
	}

	// Test Delete
	deleted := database.Delete(key)
	if !deleted {
		t.Error("Expected delete to return true")
	}

	_, exists = database.Get(key)
	if exists {
		t.Error("Expected value to not exist after delete")
	}
}

func TestOptimizedDBConcurrentAccess(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, int]()
	defer database.Close()

	var wg sync.WaitGroup
	const numGoroutines = 10
	const numOperations = 100

	// Test concurrent Put operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := []byte{byte(id), byte(j)}
				value := id*numOperations + j
				database.Put(key, value)
			}
		}(i)
	}

	wg.Wait()

	// Verify all values can be retrieved
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			key := []byte{byte(i), byte(j)}
			expectedValue := i*numOperations + j
			value, exists := database.Get(key)
			if !exists {
				t.Errorf("Expected value to exist for key %v", key)
			}
			if value != expectedValue {
				t.Errorf("Expected %d, got %d for key %v", expectedValue, value, key)
			}
		}
	}
}

func TestOptimizedDBSnapshot(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Put some initial data
	database.Put([]byte("key1"), "value1")
	database.Put([]byte("key2"), "value2")

	// Create snapshot
	snapshot := database.Snapshot()
	defer snapshot.Close()

	// Modify data after snapshot
	database.Put([]byte("key1"), "new-value1")
	database.Put([]byte("key3"), "value3")

	// Snapshot should see old data
	val1, exists := snapshot.Get([]byte("key1"))
	if !exists {
		t.Fatal("Expected key1 to exist in snapshot")
	}
	if val1 != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val1)
	}

	val2, exists := snapshot.Get([]byte("key2"))
	if !exists {
		t.Fatal("Expected key2 to exist in snapshot")
	}
	if val2 != "value2" {
		t.Errorf("Expected 'value2', got '%s'", val2)
	}

	// Snapshot should not see new data
	_, exists = snapshot.Get([]byte("key3"))
	if exists {
		t.Error("Expected key3 to not exist in snapshot")
	}
}

func TestOptimizedDBAtomicOperations(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, int]()
	defer database.Close()

	key := []byte("counter")

	// Test Increment
	database.Put(key, 10)
	newVal, success := database.Increment(key)
	if !success {
		t.Error("Expected increment to succeed")
	}
	if newVal != 11 {
		t.Errorf("Expected 11, got %d", newVal)
	}

	// Test Decrement
	newVal, success = database.Decrement(key)
	if !success {
		t.Error("Expected decrement to succeed")
	}
	if newVal != 10 {
		t.Errorf("Expected 10, got %d", newVal)
	}

	// Test Add
	newVal, success = database.Add(key, 5)
	if !success {
		t.Error("Expected add to succeed")
	}
	if newVal != 15 {
		t.Errorf("Expected 15, got %d", newVal)
	}
}

func TestOptimizedDBMetrics(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Add some data
	database.Put([]byte("key1"), "value1")
	database.Put([]byte("key2"), "value2")

	metrics := database.GetMetrics()

	// Check that metrics has the expected structure
	if metrics.Operations.Get == 0 && metrics.Operations.Put == 0 {
		t.Fatal("Expected operations metrics to be recorded")
	}

	// Check that memory metrics are available
	if metrics.Memory.ActiveSnapshots != 0 {
		t.Fatal("Expected active snapshots to be 0 for optimized DB")
	}
}

func TestOptimizedDBTruncate(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Add some data
	database.Put([]byte("key1"), "value1")
	database.Put([]byte("key2"), "value2")

	// Verify data exists
	_, exists := database.Get([]byte("key1"))
	if !exists {
		t.Fatal("Expected key1 to exist before truncate")
	}

	// Truncate
	err := database.Truncate()
	if err != nil {
		t.Fatalf("Expected truncate to succeed, got error: %v", err)
	}

	// Verify data is gone
	_, exists = database.Get([]byte("key1"))
	if exists {
		t.Error("Expected key1 to not exist after truncate")
	}

	_, exists = database.Get([]byte("key2"))
	if exists {
		t.Error("Expected key2 to not exist after truncate")
	}
}

func TestOptimizedDBEdgeCases(t *testing.T) {
	t.Parallel()
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Test with nil key
	database.Put(nil, "nil-value")
	val, exists := database.Get(nil)
	if !exists {
		t.Error("Expected nil key to exist")
	}
	if val != "nil-value" {
		t.Errorf("Expected 'nil-value', got '%s'", val)
	}

	// Test with empty key
	database.Put([]byte{}, "empty-value")
	val, exists = database.Get([]byte{})
	if !exists {
		t.Error("Expected empty key to exist")
	}
	if val != "empty-value" {
		t.Errorf("Expected 'empty-value', got '%s'", val)
	}

	// Test with large key
	largeKey := make([]byte, 1000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	database.Put(largeKey, "large-value")
	val, exists = database.Get(largeKey)
	if !exists {
		t.Error("Expected large key to exist")
	}
	if val != "large-value" {
		t.Errorf("Expected 'large-value', got '%s'", val)
	}
}

// Benchmark tests to compare performance
func BenchmarkOptimizedDBGet(b *testing.B) {
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Pre-populate with data
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i)}
		keys[i] = key
		database.Put(key, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := keys[counter%len(keys)]
			database.Get(key)
			counter++
		}
	})
}

func BenchmarkOptimizedDBPut(b *testing.B) {
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter)}
			database.Put(key, "value")
			counter++
		}
	})
}

func BenchmarkOptimizedDBConcurrentMixed(b *testing.B) {
	database := core.NewOptimized[[]byte, string]()
	defer database.Close()

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(key, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter % 256)}
			switch counter % 3 {
			case 0:
				database.Get(key)
			case 1:
				database.Put(key, "new-value")
			case 2:
				database.Delete(key)
			}
			counter++
		}
	})
}
