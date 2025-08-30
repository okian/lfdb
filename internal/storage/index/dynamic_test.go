// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"sync"
	"testing"
	"time"
)

func TestNewDynamicHashIndex(t *testing.T) {
	t.Parallel()

	// Test with default parameters
	index := NewDynamicHashIndex[string](0, 0)
	if index == nil {
		t.Fatal("Expected non-nil index")
	}
	if index.loadFactor != 0.75 {
		t.Errorf("Expected load factor 0.75, got %f", index.loadFactor)
	}
	if index.size != 1024 {
		t.Errorf("Expected size 1024, got %d", index.size)
	}

	// Test with custom parameters
	index2 := NewDynamicHashIndex[string](2048, 0.5)
	if index2 == nil {
		t.Fatal("Expected non-nil index")
	}
	if index2.loadFactor != 0.5 {
		t.Errorf("Expected load factor 0.5, got %f", index2.loadFactor)
	}
	if index2.size != 2048 {
		t.Errorf("Expected size 2048, got %d", index2.size)
	}
}

func TestCalculateOptimalBuckets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dataSize int
		expected uint64
	}{
		{"zero size", 0, 1024},
		{"negative size", -1, 1024},
		{"small size", 100, 1024},
		{"medium size", 1000, 4096},
		{"large size", 10000, 65536},
		{"very large size", 1000000, 1048576}, // 1M max
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := calculateOptimalBuckets(tt.dataSize)
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestDynamicHashIndexBasicOperations(t *testing.T) {
	t.Parallel()

	index := NewDynamicHashIndex[string](1024, 0.75)

	// Test GetOrCreate
	key := []byte("test-key")
	entry := index.GetOrCreate(key)
	if entry == nil {
		t.Fatal("Expected non-nil entry")
	}

	// Test entry count
	if index.GetEntryCount() != 1 {
		t.Errorf("Expected entry count 1, got %d", index.GetEntryCount())
	}

	// Test GetOrCreate with same key
	entry2 := index.GetOrCreate(key)
	if entry2 != entry {
		t.Error("Expected same entry for same key")
	}

	// Test entry count remains same
	if index.GetEntryCount() != 1 {
		t.Errorf("Expected entry count 1, got %d", index.GetEntryCount())
	}
}

func TestDynamicHashIndexResize(t *testing.T) {
	t.Parallel()

	// Create index with very low load factor to trigger resize
	index := NewDynamicHashIndex[string](4, 0.1) // 4 buckets, 10% load factor

	initialSize := index.GetBucketCount()

	// Add entries to trigger resize
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		index.GetOrCreate(key)
	}

	// Check if resize occurred
	newSize := index.GetBucketCount()
	if newSize <= initialSize {
		t.Errorf("Expected resize, but size remained %d", newSize)
	}

	// Verify all entries are still accessible
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		entry := index.Get(key)
		if entry == nil {
			t.Errorf("Expected entry for key %d after resize", i)
		}
	}
}

func TestDynamicHashIndexConcurrentAccess(t *testing.T) {
	t.Parallel()

	index := NewDynamicHashIndex[string](1024, 0.75)
	const numGoroutines = 10
	const entriesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				key := []byte{byte(id), byte(j)}
				index.GetOrCreate(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify total entries
	expectedEntries := int64(numGoroutines * entriesPerGoroutine)
	if index.GetEntryCount() != expectedEntries {
		t.Errorf("Expected %d entries, got %d", expectedEntries, index.GetEntryCount())
	}
}

func TestDynamicHashIndexLoadFactor(t *testing.T) {
	t.Parallel()

	index := NewDynamicHashIndex[string](128, 0.5)

	// Add entries and check load factor
	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		index.GetOrCreate(key)
	}

	load := index.GetCurrentLoad()
	if load <= 0 {
		t.Errorf("Expected positive load factor, got %f", load)
	}

	// Load should be approximately 0.39 (50 entries / 128 buckets)
	if load < 0.35 || load > 0.45 {
		t.Errorf("Expected load factor around 0.39, got %f", load)
	}
}

func TestNewAdaptiveHashIndex(t *testing.T) {
	t.Parallel()

	index := NewAdaptiveHashIndex[string](1024)
	if index == nil {
		t.Fatal("Expected non-nil index")
	}
	if index.accessCounts == nil {
		t.Fatal("Expected non-nil access counts map")
	}
	if index.DynamicHashIndex == nil {
		t.Fatal("Expected non-nil dynamic hash index")
	}
}

func TestAdaptiveHashIndexGetOrCreate(t *testing.T) {
	t.Parallel()

	index := NewAdaptiveHashIndex[string](1024)

	// Test GetOrCreate
	key := []byte("test-key")
	entry := index.GetOrCreate(key)
	if entry == nil {
		t.Fatal("Expected non-nil entry")
	}

	// Test access tracking
	index.mu.RLock()
	accessCount, exists := index.accessCounts[index.hash(key)]
	index.mu.RUnlock()

	if !exists {
		t.Error("Expected access count to be tracked")
	}
	if accessCount != 1 {
		t.Errorf("Expected access count 1, got %d", accessCount)
	}
}

func TestAdaptiveHashIndexGetHotBuckets(t *testing.T) {
	t.Parallel()

	index := NewAdaptiveHashIndex[string](1024)

	// Add some entries with different access patterns
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}

	// Access key1 multiple times
	for i := 0; i < 10; i++ {
		index.GetOrCreate(keys[0])
	}

	// Access key2 a few times
	for i := 0; i < 5; i++ {
		index.GetOrCreate(keys[1])
	}

	// Access others once
	for i := 2; i < len(keys); i++ {
		index.GetOrCreate(keys[i])
	}

	// Get hot buckets
	hotBuckets := index.GetHotBuckets(3)
	if len(hotBuckets) == 0 {
		t.Error("Expected hot buckets to be found")
	}

	// Verify we get some buckets back
	if len(hotBuckets) > 0 {
		t.Logf("Found %d hot buckets", len(hotBuckets))
	}
}

func TestAdaptiveHashIndexOptimizeForAccessPatterns(t *testing.T) {
	t.Parallel()

	index := NewAdaptiveHashIndex[string](128)

	// Create a hot bucket by accessing the same key many times
	hotKey := []byte("hot-key")
	for i := 0; i < 100; i++ {
		index.GetOrCreate(hotKey)
	}

	// Add some other keys
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		index.GetOrCreate(key)
	}

	// Run optimization
	index.OptimizeForAccessPatterns()

	// Verify the index still works
	entry := index.GetOrCreate(hotKey)
	if entry == nil {
		t.Error("Expected entry to still be accessible after optimization")
	}
}

func TestDynamicHashIndexEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with nil key
	index := NewDynamicHashIndex[string](1024, 0.75)
	entry := index.GetOrCreate(nil)
	if entry == nil {
		t.Fatal("Expected non-nil entry for nil key")
	}

	// Test with empty key
	entry2 := index.GetOrCreate([]byte{})
	if entry2 == nil {
		t.Fatal("Expected non-nil entry for empty key")
	}

	// Test with very large key
	largeKey := make([]byte, 10000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	entry3 := index.GetOrCreate(largeKey)
	if entry3 == nil {
		t.Fatal("Expected non-nil entry for large key")
	}
}

func TestDynamicHashIndexStressTest(t *testing.T) {
	t.Parallel()

	index := NewDynamicHashIndex[string](1024, 0.75)
	const numEntries = 10000

	// Add many entries
	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		for j := range key {
			key[j] = byte(i >> (j * 8))
		}
		index.GetOrCreate(key)
	}

	// Verify we can retrieve entries
	foundCount := 0
	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		for j := range key {
			key[j] = byte(i >> (j * 8))
		}
		if entry := index.Get(key); entry != nil {
			foundCount++
		}
	}

	// Allow for some hash collisions
	if foundCount < numEntries*90/100 {
		t.Errorf("Expected at least 90%% of entries to be found, got %d/%d", foundCount, numEntries)
	}
}

func TestDynamicHashIndexConcurrentResize(t *testing.T) {
	t.Parallel()

	index := NewDynamicHashIndex[string](4, 0.1) // Small size to trigger resize
	const numGoroutines = 5
	const entriesPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				key := []byte{byte(id), byte(j)}
				index.GetOrCreate(key)
				time.Sleep(time.Microsecond) // Small delay to increase chance of concurrent resize
			}
		}(i)
	}

	wg.Wait()

	// Verify the index is still functional
	expectedEntries := int64(numGoroutines * entriesPerGoroutine)
	if index.GetEntryCount() != expectedEntries {
		t.Errorf("Expected %d entries, got %d", expectedEntries, index.GetEntryCount())
	}
}
