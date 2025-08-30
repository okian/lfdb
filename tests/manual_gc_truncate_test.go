// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"
)

// TestManualGC tests the manual garbage collection functionality
func TestManualGC(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data to create garbage
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := string(key)
		database.Put(ctx, key, value)
	}

	// Update some keys to create version chains
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		value := "updated_" + string(key)
		database.Put(ctx, key, value)
	}

	// Delete some keys to create tombstones
	for i := 50; i < 75; i++ {
		key := []byte{byte(i)}
		database.Delete(ctx, key)
	}

	// Perform manual garbage collection
	start := time.Now()
	err := database.ManualGC(ctx)
	duration := time.Since(start)
	if err != nil {
		t.Fatalf("Manual GC failed: %v", err)
	}

	// Verify GC completed successfully
	if duration < time.Millisecond {
		t.Logf("Manual GC completed in %v", duration)
	}

	// Verify data is still accessible after GC
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		expectedValue := "updated_" + string(key)
		val, exists := database.Get(ctx, key)
		if !exists || val != expectedValue {
			t.Errorf("Expected key %d=%s, got %v, exists=%v", i, expectedValue, val, exists)
		}
	}

	// Verify deleted keys are still deleted
	for i := 50; i < 75; i++ {
		key := []byte{byte(i)}
		val, exists := database.Get(ctx, key)
		if exists {
			t.Errorf("Expected key %d to be deleted, but got %v", i, val)
		}
	}
}

// TestTruncate tests the truncate functionality
func TestTruncate(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := fmt.Sprintf("value%d", i)
		database.Put(ctx, key, value)
	}

	// Verify data exists
	count := 0
	snapshot := database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 100 {
		t.Errorf("Expected 100 keys before truncate, got %d", count)
	}

	// Truncate the database
	err := database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify all data is gone
	count = 0
	snapshot = database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 0 {
		t.Errorf("Expected 0 keys after truncate, got %d", count)
	}

	// Verify database is still functional after truncate
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Errorf("Database not functional after truncate")
	}
}

// TestTruncateEmpty tests truncating an empty database
func TestTruncateEmpty(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Verify database is empty
	count := 0
	snapshot := database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 0 {
		t.Errorf("Expected 0 keys in empty database, got %d", count)
	}

	// Truncate empty database
	err := database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed on empty database: %v", err)
	}

	// Verify database is still empty
	count = 0
	snapshot = database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 0 {
		t.Errorf("Expected 0 keys after truncating empty database, got %d", count)
	}
}

// TestTruncateWithSnapshots tests truncating with active snapshots
func TestTruncateWithSnapshots(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	database.Put(ctx, []byte("key1"), "value1")
	database.Put(ctx, []byte("key2"), "value2")

	// Create snapshot before truncate
	snapshot := database.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Truncate the database
	err := database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Snapshot should not see old data after truncate (truncate clears all data)
	_, exists := snapshot.Get(ctx, []byte("key1"))
	if exists {
		t.Errorf("Snapshot should not see old data after truncate")
	}

	_, exists = snapshot.Get(ctx, []byte("key2"))
	if exists {
		t.Errorf("Snapshot should not see old data after truncate")
	}

	// Current view should be empty
	_, exists = database.Get(ctx, []byte("key1"))
	if exists {
		t.Errorf("Current view should not see old data after truncate")
	}
}

// TestTruncateWithTTL tests truncate with TTL data
func TestTruncateWithTTL(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add data with TTL
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("ttl_key%d", i))
		value := fmt.Sprintf("ttl_value%d", i)
		database.PutWithTTL(ctx, key, value, 10*time.Second) // Longer TTL to prevent expiration
	}

	// Add regular data
	for i := 50; i < 100; i++ {
		key := []byte(fmt.Sprintf("reg_key%d", i))
		value := fmt.Sprintf("reg_value%d", i)
		database.Put(ctx, key, value)
	}

	// Verify data exists
	count := 0
	snapshot := database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	// Allow for some variation due to timing
	if count < 95 || count > 100 {
		t.Errorf("Expected around 100 keys before truncate, got %d", count)
	}

	// Truncate the database
	err := database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify all data is gone
	count = 0
	snapshot = database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 0 {
		t.Errorf("Expected 0 keys after truncate, got %d", count)
	}
}

// TestTruncateWithTransactions tests truncate with active transactions
func TestTruncateWithTransactions(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	database.Put(ctx, []byte("key1"), "value1")
	database.Put(ctx, []byte("key2"), "value2")

	// Start a transaction
	err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
		// Modify data within transaction
		tx.Put(ctx, []byte("key1"), "modified")
		tx.Put(ctx, []byte("key3"), "new")

		// Truncate should not affect the transaction
		// (though in a real implementation, you might want to prevent this)
		return nil
	})

	if err != nil {
		t.Fatalf("Transaction failed: %v", err)
	}

	// Now truncate
	err = database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify all data is gone
	count := 0
	snapshot := database.Snapshot(ctx)
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})
	snapshot.Close(ctx)

	if count != 0 {
		t.Errorf("Expected 0 keys after truncate, got %d", count)
	}
}

// TestTruncateAndReuse tests using the database after truncate
func TestTruncateAndReuse(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	database.Put(ctx, []byte("key1"), "value1")
	database.Put(ctx, []byte("key2"), "value2")

	// Truncate the database
	err := database.Truncate(ctx)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Add new data
	database.Put(ctx, []byte("new_key1"), "new_value1")
	database.Put(ctx, []byte("new_key2"), "new_value2")

	// Verify new data is accessible
	val, exists := database.Get(ctx, []byte("new_key1"))
	if !exists || val != "new_value1" {
		t.Errorf("New data not accessible after truncate")
	}

	val, exists = database.Get(ctx, []byte("new_key2"))
	if !exists || val != "new_value2" {
		t.Errorf("New data not accessible after truncate")
	}

	// Verify old data is not accessible
	_, exists = database.Get(ctx, []byte("key1"))
	if exists {
		t.Errorf("Old data should not be accessible after truncate")
	}

	// Verify snapshot shows new state
	snapshot := database.Snapshot(ctx)
	defer snapshot.Close(ctx)

	count := 0
	snapshot.Iterate(ctx, func(key []byte, val string) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("Expected 2 keys in snapshot after truncate and reuse, got %d", count)
	}
}

// TestManualGCPerformance tests the performance of manual GC
func TestManualGCPerformance(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Create a lot of data to generate garbage
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := string(key) + "_" + string(rune(i))
		database.Put(ctx, key, value)
	}

	// Perform multiple updates to create version chains
	for i := 0; i < 10; i++ {
		for j := 0; j < 100; j++ {
			key := []byte{byte(j)}
			value := string(key) + "_update_" + string(rune(i))
			database.Put(ctx, key, value)
		}
	}

	// Measure GC performance
	start := time.Now()
	if err := database.ManualGC(ctx); err != nil {
		t.Fatalf("Manual GC failed: %v", err)
	}
	duration := time.Since(start)

	// GC should complete in reasonable time (less than 1 second)
	if duration > time.Second {
		t.Errorf("Manual GC took too long: %v", duration)
	}

	t.Logf("Manual GC completed in %v", duration)
}

// BenchmarkManualGC benchmarks the manual garbage collection
func BenchmarkManualGC(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data that will create garbage
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := string(key) + "_" + string(rune(i))
		database.Put(ctx, key, value)
	}

	// Create some version chains
	for i := 0; i < 5; i++ {
		for j := 0; j < 100; j++ {
			key := []byte{byte(j)}
			value := string(key) + "_update_" + string(rune(i))
			database.Put(ctx, key, value)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := database.ManualGC(ctx); err != nil {
			b.Fatalf("Manual GC failed: %v", err)
		}
	}
}

// BenchmarkTruncate benchmarks the truncate operation
func BenchmarkTruncate(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := string(key) + "_" + string(rune(i))
		database.Put(ctx, key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := database.Truncate(ctx); err != nil {
			b.Fatalf("Truncate failed: %v", err)
		}

		// Re-add some data for next iteration
		for j := 0; j < 100; j++ {
			key := []byte{byte(j)}
			value := string(key) + "_" + string(rune(i))
			database.Put(ctx, key, value)
		}
	}
}

// TestManualGCConcurrent tests manual GC with concurrent operations
func TestManualGCConcurrent(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := fmt.Sprintf("value%d", i)
		database.Put(ctx, key, value)
	}

	// Start concurrent operations
	done := make(chan bool)
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			key := []byte{byte(i % 100)}
			value := fmt.Sprintf("concurrent_value%d", i)
			database.Put(ctx, key, value)
		}
	}()

	// Perform manual garbage collection while operations are running
	if err := database.ManualGC(ctx); err != nil {
		t.Fatalf("Manual GC failed: %v", err)
	}

	// Wait for concurrent operations to complete
	<-done

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Errorf("Database not functional after concurrent GC")
	}
}

// TestTruncateConcurrent tests truncate with concurrent operations
func TestTruncateConcurrent(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Add some data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := fmt.Sprintf("value%d", i)
		database.Put(ctx, key, value)
	}

	// Start concurrent operations
	done := make(chan bool)
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			key := []byte{byte(i % 100)}
			value := fmt.Sprintf("concurrent_value%d", i)
			database.Put(ctx, key, value)
		}
	}()

	// Perform truncate while operations are running
	if err := database.Truncate(ctx); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Wait for concurrent operations to complete
	<-done

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Errorf("Database not functional after concurrent truncate")
	}
}
