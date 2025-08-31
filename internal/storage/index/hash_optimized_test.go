// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"golang.org/x/sys/cpu"
)

func TestOptimizedHashIndexBasicOperations(t *testing.T) {
	t.Parallel()
	index := NewOptimizedHashIndex[string](16)

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

func TestOptimizedHashIndexConcurrentAccess(t *testing.T) {
	t.Parallel()
	index := NewOptimizedHashIndex[int](64)

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

func TestOptimizedHashIndexBytesEqualOptimized(t *testing.T) {
	t.Parallel()

	// Test equal byte slices
	if !bytesEqualOptimized([]byte("test"), []byte("test")) {
		t.Error("Expected equal byte slices to be equal")
	}

	// Test different byte slices
	if bytesEqualOptimized([]byte("test"), []byte("other")) {
		t.Error("Expected different byte slices to not be equal")
	}

	// Test different lengths
	if bytesEqualOptimized([]byte("test"), []byte("testing")) {
		t.Error("Expected byte slices of different lengths to not be equal")
	}

	// Test empty byte slices
	if !bytesEqualOptimized([]byte{}, []byte{}) {
		t.Error("Expected empty byte slices to be equal")
	}

	// Test nil byte slices
	if !bytesEqualOptimized(nil, nil) {
		t.Error("Expected nil byte slices to be equal")
	}

	// Test nil vs empty - both have length 0, so they're considered equal
	if !bytesEqualOptimized(nil, []byte{}) {
		t.Error("Expected nil and empty byte slices to be equal (both have length 0)")
	}

	// Test empty vs nil - both have length 0, so they're considered equal
	if !bytesEqualOptimized([]byte{}, nil) {
		t.Error("Expected empty and nil byte slices to be equal (both have length 0)")
	}

	// Test large byte slices (should trigger SIMD optimizations)
	largeKey1 := make([]byte, 1000)
	largeKey2 := make([]byte, 1000)
	for i := range largeKey1 {
		largeKey1[i] = byte(i % 256)
		largeKey2[i] = byte(i % 256)
	}
	if !bytesEqualOptimized(largeKey1, largeKey2) {
		t.Error("Expected large equal byte slices to be equal")
	}

	// Test large byte slices with difference
	largeKey2[500] = 0xFF
	if bytesEqualOptimized(largeKey1, largeKey2) {
		t.Error("Expected large different byte slices to not be equal")
	}
}

func TestOptimizedHashIndexHashDistribution(t *testing.T) {
	t.Parallel()
	index := NewOptimizedHashIndex[string](16)

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

func TestOptimizedHashIndexEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with very large size
	largeIndex := NewOptimizedHashIndex[string](1024)
	if largeIndex.Size() != 1024 {
		t.Errorf("Expected size 1024, got %d", largeIndex.Size())
	}

	// Test with nil keys
	index := NewOptimizedHashIndex[string](16)
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

func TestBytesEqualSIMDVariants(t *testing.T) {
	t.Parallel()

	type variant struct {
		name      string
		fn        func([]byte, []byte) bool
		supported bool
	}

	var variants []variant
	switch runtime.GOARCH {
	case "amd64":
		variants = []variant{
			{"AVX2", bytesEqualAVX2, cpu.X86.HasAVX2},
			{"SSE42", bytesEqualSSE42, cpu.X86.HasSSE42},
			{"SSE2", bytesEqualSSE2, cpu.X86.HasSSE2},
		}
	case "arm64":
		variants = []variant{
			{"NEON", bytesEqualNEON, cpu.ARM64.HasASIMD},
		}
	}

	for _, v := range variants {
		v := v
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			if !v.supported {
				t.Skipf("%s not supported", v.name)
			}
			if !v.fn(nil, nil) {
				t.Error("nil slices should be equal")
			}
			if !v.fn([]byte{}, []byte{}) {
				t.Error("empty slices should be equal")
			}
			if v.fn([]byte{1}, []byte{2}) {
				t.Error("different slices reported equal")
			}
		})
	}
}

func TestOptimizedHashIndexStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()
	index := NewOptimizedHashIndex[string](256)

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

// Benchmark tests to compare performance
func BenchmarkOptimizedHashIndexGetOrCreate(b *testing.B) {
	index := NewOptimizedHashIndex[string](1024)

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		index.GetOrCreate([]byte(key))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := fmt.Sprintf("bench-key-%d", counter)
			index.GetOrCreate([]byte(key))
			counter++
		}
	})
}

func BenchmarkOptimizedHashIndexGet(b *testing.B) {
	index := NewOptimizedHashIndex[string](1024)

	// Pre-populate with data
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[i] = []byte(key)
		index.GetOrCreate(keys[i])
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := keys[counter%len(keys)]
			index.Get(key)
			counter++
		}
	})
}

func BenchmarkBytesEqualOptimized(b *testing.B) {
	// Test data
	smallKey := []byte("small-key")
	mediumKey := make([]byte, 64)
	largeKey := make([]byte, 1024)

	for i := range mediumKey {
		mediumKey[i] = byte(i % 256)
	}
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}

	// Create copies for comparison
	smallKey2 := make([]byte, len(smallKey))
	copy(smallKey2, smallKey)
	mediumKey2 := make([]byte, len(mediumKey))
	copy(mediumKey2, mediumKey)
	largeKey2 := make([]byte, len(largeKey))
	copy(largeKey2, largeKey)

	b.Run("Small", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bytesEqualOptimized(smallKey, smallKey2)
		}
	})

	b.Run("Medium", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bytesEqualOptimized(mediumKey, mediumKey2)
		}
	})

	b.Run("Large", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			bytesEqualOptimized(largeKey, largeKey2)
		}
	})
}
