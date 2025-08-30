// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"fmt"
	"sync"
	"testing"
)

func TestHashIndexBasicOperations(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Test GetOrCreate
	entry1 := index.GetOrCreate([]byte("key1"))
	if entry1 == nil {
		t.Fatal("Expected entry to be created")
	}

	// Test Get
	entry2 := index.Get([]byte("key1"))
	if entry2 != entry1 {
		t.Error("Expected same entry for same key")
	}

	// Test Get for non-existent key
	entry3 := index.Get([]byte("key2"))
	if entry3 != nil {
		t.Error("Expected nil for non-existent key")
	}

	// Test GetOrCreate for same key
	entry4 := index.GetOrCreate([]byte("key1"))
	if entry4 != entry1 {
		t.Error("Expected same entry for same key")
	}
}

func TestHashIndexConcurrentAccess(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[int](64)

	var wg sync.WaitGroup
	const numGoroutines = 10
	const numKeys = 100

	// Test concurrent GetOrCreate
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numKeys; j++ {
				key := []byte{byte(goroutineID), byte(j)}
				entry := index.GetOrCreate(key)
				if entry == nil {
					t.Errorf("Expected entry to be created for key %v", key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries exist
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numKeys; j++ {
			key := []byte{byte(i), byte(j)}
			entry := index.Get(key)
			if entry == nil {
				t.Errorf("Expected entry to exist for key %v", key)
			}
		}
	}
}

func TestHashIndexBucketDistribution(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Insert some keys and check bucket distribution
	keys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"),
		[]byte("key4"), []byte("key5"), []byte("key6"),
	}

	for _, key := range keys {
		index.GetOrCreate(key)
	}

	// Check that buckets are being used
	totalEntries := 0
	for i := uint64(0); i < index.Size(); i++ {
		count := index.BucketCount(i)
		totalEntries += count
	}

	if totalEntries != len(keys) {
		t.Errorf("Expected %d total entries, got %d", len(keys), totalEntries)
	}
}

func TestHashIndexSize(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	if size := index.Size(); size != 16 {
		t.Errorf("Expected size 16, got %d", size)
	}

	index = NewHashIndex[string](32)
	if size := index.Size(); size != 32 {
		t.Errorf("Expected size 32, got %d", size)
	}
}

func TestHashIndexBucketCount(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Test empty bucket
	if count := index.BucketCount(0); count != 0 {
		t.Errorf("Expected bucket 0 to have 0 entries, got %d", count)
	}

	// Test invalid bucket index
	if count := index.BucketCount(20); count != 0 {
		t.Errorf("Expected invalid bucket to return 0, got %d", count)
	}

	// Add some entries
	entry1 := index.GetOrCreate([]byte("key1"))
	entry2 := index.GetOrCreate([]byte("key2"))

	// Verify entries were created
	if entry1 == nil || entry2 == nil {
		t.Fatal("Expected entries to be created")
	}

	// Count total entries across all buckets
	totalEntries := 0
	for i := uint64(0); i < index.Size(); i++ {
		totalEntries += index.BucketCount(i)
	}

	// Should have at least 2 entries total
	if totalEntries < 2 {
		t.Errorf("Expected at least 2 total entries, got %d", totalEntries)
	}
}

func TestHashIndexBucketCountMultipleBuckets(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](4) // Small size to force collisions

	// Add entries that should hash to different buckets
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		entry := index.GetOrCreate([]byte(key))
		if entry == nil {
			t.Fatalf("Failed to create entry for key %s", key)
		}
	}

	// Check that at least some buckets have entries
	totalEntries := 0
	for i := uint64(0); i < index.Size(); i++ {
		count := index.BucketCount(i)
		totalEntries += count
	}

	if totalEntries < len(keys) {
		t.Errorf("Expected at least %d total entries, got %d", len(keys), totalEntries)
	}
}

func TestHashIndexBytesEqual(t *testing.T) {
	t.Parallel()
	// Test equal byte slices
	if !bytesEqual([]byte("test"), []byte("test")) {
		t.Error("Expected equal byte slices to be equal")
	}

	// Test different byte slices
	if bytesEqual([]byte("test"), []byte("other")) {
		t.Error("Expected different byte slices to not be equal")
	}

	// Test different lengths
	if bytesEqual([]byte("test"), []byte("testing")) {
		t.Error("Expected byte slices of different lengths to not be equal")
	}

	// Test empty byte slices
	if !bytesEqual([]byte{}, []byte{}) {
		t.Error("Expected empty byte slices to be equal")
	}

	// Test nil byte slices
	if !bytesEqual(nil, nil) {
		t.Error("Expected nil byte slices to be equal")
	}

	// Test nil vs empty - both have length 0, so they're considered equal
	if !bytesEqual(nil, []byte{}) {
		t.Error("Expected nil and empty byte slices to be equal (both have length 0)")
	}

	// Test empty vs nil - both have length 0, so they're considered equal
	if !bytesEqual([]byte{}, nil) {
		t.Error("Expected empty and nil byte slices to be equal (both have length 0)")
	}
}

func TestHashIndexGetOrCreateConcurrent(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	const numGoroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup

	// Start multiple goroutines creating entries concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				entry := index.GetOrCreate([]byte(key))
				if entry == nil {
					t.Errorf("Failed to create entry for key %s", key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries can be retrieved
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < opsPerGoroutine; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
			entry := index.Get([]byte(key))
			if entry == nil {
				t.Errorf("Failed to retrieve entry for key %s", key)
			}
		}
	}
}

func TestHashIndexGetOrCreateSameKey(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	key := []byte("test-key")

	// Create entry first time
	entry1 := index.GetOrCreate(key)
	if entry1 == nil {
		t.Fatal("Failed to create first entry")
	}

	// Try to create same key again
	entry2 := index.GetOrCreate(key)
	if entry2 == nil {
		t.Fatal("Failed to get existing entry")
	}

	// Should return the same entry
	if entry1 != entry2 {
		t.Error("Expected GetOrCreate to return same entry for same key")
	}
}

func TestHashIndexGetNonExistent(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Try to get non-existent key
	entry := index.Get([]byte("non-existent"))
	if entry != nil {
		t.Error("Expected Get to return nil for non-existent key")
	}
}

func TestHashIndexHashDistribution(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Add many entries to test hash distribution
	const numEntries = 1000
	bucketCounts := make(map[uint64]int)

	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := index.GetOrCreate([]byte(key))
		if entry == nil {
			t.Fatalf("Failed to create entry for key %s", key)
		}

		// Count entries per bucket
		for bucketIdx := uint64(0); bucketIdx < index.Size(); bucketIdx++ {
			count := index.BucketCount(bucketIdx)
			bucketCounts[bucketIdx] = count
		}
	}

	// Check that entries are distributed across buckets
	nonEmptyBuckets := 0
	for _, count := range bucketCounts {
		if count > 0 {
			nonEmptyBuckets++
		}
	}

	// With 1000 entries and 16 buckets, we should have entries in most buckets
	if nonEmptyBuckets < 8 {
		t.Errorf("Expected at least 8 non-empty buckets, got %d", nonEmptyBuckets)
	}
}

func TestHashIndexPowerOfTwoSize(t *testing.T) {
	t.Parallel()
	// Test valid power of 2 sizes
	validSizes := []uint64{1, 2, 4, 8, 16, 32, 64, 128, 256}
	for _, size := range validSizes {
		index := NewHashIndex[string](size)
		if index.Size() != size {
			t.Errorf("Expected size %d, got %d", size, index.Size())
		}
	}
}

func TestHashIndexZeroSize(t *testing.T) {
	t.Parallel()
	// Test that zero size panics
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for zero size")
		}
	}()

	NewHashIndex[string](0)
}

func TestHashIndexEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with very large size
	largeIndex := NewHashIndex[string](1024)
	if largeIndex.Size() != 1024 {
		t.Errorf("Expected size 1024, got %d", largeIndex.Size())
	}

	// Test with nil keys
	index := NewHashIndex[string](16)
	entry := index.GetOrCreate(nil)
	if entry == nil {
		t.Error("Expected entry to be created for nil key")
	}

	// Test Get with nil key
	entry2 := index.Get(nil)
	if entry2 != entry {
		t.Error("Expected same entry for nil key")
	}

	// Test with empty keys
	emptyEntry := index.GetOrCreate([]byte{})
	if emptyEntry == nil {
		t.Error("Expected entry to be created for empty key")
	}

	// Test Get with empty key
	emptyEntry2 := index.Get([]byte{})
	if emptyEntry2 != emptyEntry {
		t.Error("Expected same entry for empty key")
	}
}

func TestHashIndexStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()
	index := NewHashIndex[string](256)

	var wg sync.WaitGroup
	const numGoroutines = 20
	const numOperations = 500

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("stress-key-%d-%d", id, j)
				entry := index.GetOrCreate([]byte(key))
				if entry == nil {
					t.Errorf("Failed to create entry for key %s", key)
				}

				// Also test Get
				entry2 := index.Get([]byte(key))
				if entry2 != entry {
					t.Errorf("Get returned different entry for key %s", key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries can be retrieved
	for i := 0; i < numGoroutines; i++ {
		for j := 0; j < numOperations; j++ {
			key := fmt.Sprintf("stress-key-%d-%d", i, j)
			entry := index.Get([]byte(key))
			if entry == nil {
				t.Errorf("Failed to retrieve entry for key %s", key)
			}
		}
	}
}

func TestHashIndexBucketCountEdgeCases(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)

	// Test bucket count for out of bounds indices
	if count := index.BucketCount(100); count != 0 {
		t.Errorf("Expected 0 for out of bounds bucket, got %d", count)
	}

	if count := index.BucketCount(^uint64(0)); count != 0 {
		t.Errorf("Expected 0 for max uint64 bucket, got %d", count)
	}

	// Test bucket count for all valid buckets
	for i := uint64(0); i < index.Size(); i++ {
		count := index.BucketCount(i)
		if count < 0 {
			t.Errorf("Expected non-negative count for bucket %d, got %d", i, count)
		}
	}
}
