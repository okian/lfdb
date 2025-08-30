// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"sync"
	"testing"
	"time"
)

func TestNewReadAheadIterator(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewReadAheadIterator(index, 1000, 50)
	if iterator == nil {
		t.Fatal("Expected non-nil iterator")
	}

	// Wait for initial prefetch to complete to avoid race conditions
	iterator.Close()

	// Test basic functionality instead of accessing internal fields
	count := 0
	for iterator.Next() {
		count++
	}
	// Should have no entries in empty index
	if count != 0 {
		t.Errorf("Expected 0 entries in empty index, got %d", count)
	}
}

func TestNewReadAheadIteratorDefaultBufferSize(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewReadAheadIterator(index, 1000, 0) // 0 should use default
	if iterator == nil {
		t.Fatal("Expected non-nil iterator")
	}
	if iterator.bufferSize != 100 {
		t.Errorf("Expected default buffer size 100, got %d", iterator.bufferSize)
	}
}

func TestReadAheadIteratorBasicOperations(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add some entries
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	for i, key := range keys {
		entry := index.GetOrCreate(key)
		entry.Put("value"+string(rune('1'+i)), 1000)
	}

	iterator := NewReadAheadIterator(index, 1000, 10)
	defer iterator.Close()

	// Test iteration
	foundKeys := make(map[string]bool)
	for iterator.Next() {
		key := iterator.Key()
		if key != nil {
			foundKeys[string(key)] = true
		}
	}

	// Verify all keys were found
	for _, key := range keys {
		if !foundKeys[string(key)] {
			t.Errorf("Expected to find key %s", key)
		}
	}
}

func TestReadAheadIteratorEmptyIndex(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewReadAheadIterator(index, 1000, 10)
	defer iterator.Close()

	// Should not have any entries
	if iterator.Next() {
		t.Error("Expected no entries in empty index")
	}

	// Entry should be nil
	if entry := iterator.Entry(); entry != nil {
		t.Error("Expected nil entry for empty index")
	}

	// Key should be nil
	if key := iterator.Key(); key != nil {
		t.Error("Expected nil key for empty index")
	}

	// Value should return zero value and false
	if _, found := iterator.Value(); found {
		t.Error("Expected no value found for empty index")
	}
	if value, _ := iterator.Value(); value != "" {
		t.Errorf("Expected empty string value, got %s", value)
	}
}

func TestReadAheadIteratorReset(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add some entries
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	for _, key := range keys {
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	iterator := NewReadAheadIterator(index, 1000, 10)
	defer iterator.Close()

	// Iterate once
	count := 0
	for iterator.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 entries, got %d", count)
	}

	// Reset and iterate again
	iterator.Reset()
	count = 0
	for iterator.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 entries after reset, got %d", count)
	}
}

func TestReadAheadIteratorConcurrentAccess(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add many entries
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		key := []byte{byte(i)}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	// Count actual entries (accounting for hash collisions)
	actualEntries := 0
	for i := range index.buckets {
		bucket := &index.buckets[i]
		for node := bucket.Load(); node != nil; node = node.next.Load() {
			if node.entry != nil {
				actualEntries++
			}
		}
	}

	// Create multiple iterators concurrently
	const numIterators = 5
	var wg sync.WaitGroup
	wg.Add(numIterators)

	for i := 0; i < numIterators; i++ {
		go func(id int) {
			defer wg.Done()
			iterator := NewReadAheadIterator(index, 1000, 50)
			defer iterator.Close()

			count := 0
			for iterator.Next() {
				count++
			}

			// Each iterator should find all actual entries
			if count != actualEntries {
				t.Errorf("Iterator %d: Expected %d entries, got %d", id, actualEntries, count)
			}
		}(i)
	}

	wg.Wait()
}

func TestReadAheadIteratorDebug(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add entries
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		key := []byte{byte(i)}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	// Count entries manually
	totalEntries := 0
	nonEmptyBuckets := 0
	for i := range index.buckets {
		bucket := &index.buckets[i]
		bucketEntries := 0
		for node := bucket.Load(); node != nil; node = node.next.Load() {
			if node.entry != nil {
				totalEntries++
				bucketEntries++
			}
		}
		if bucketEntries > 0 {
			nonEmptyBuckets++
			t.Logf("Bucket %d: %d entries", i, bucketEntries)
		}
	}
	t.Logf("Total entries in index: %d, non-empty buckets: %d", totalEntries, nonEmptyBuckets)

	// Test iterator
	iterator := NewReadAheadIterator(index, 1000, 50)
	defer iterator.Close()

	count := 0
	for iterator.Next() {
		count++
	}

	t.Logf("Iterator found: %d entries", count)
	if count != totalEntries {
		t.Errorf("Expected %d entries, got %d", totalEntries, count)
	}
}

func TestReadAheadIteratorStressTest(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add many entries
	const numEntries = 10000
	for i := 0; i < numEntries; i++ {
		key := make([]byte, 8)
		for j := range key {
			key[j] = byte(i >> (j * 8))
		}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	iterator := NewReadAheadIterator(index, 1000, 100)
	defer iterator.Close()

	count := 0
	for iterator.Next() {
		count++
	}

	// Should find all entries
	if count != numEntries {
		t.Errorf("Expected %d entries, got %d", numEntries, count)
	}
}

func TestNewOptimizedIterator(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewOptimizedIterator(index, 1000)
	if iterator == nil {
		t.Fatal("Expected non-nil iterator")
	}
	if iterator.ReadAheadIterator == nil {
		t.Fatal("Expected non-nil read-ahead iterator")
	}
	defer iterator.Close()
}

func TestOptimizedIteratorForEach(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add some entries
	expected := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for key, value := range expected {
		entry := index.GetOrCreate([]byte(key))
		entry.Put(value, 1000)
	}

	iterator := NewOptimizedIterator(index, 1000)
	defer iterator.Close()

	// Test ForEach
	found := make(map[string]string)
	iterator.ForEach(func(key []byte, value string) bool {
		found[string(key)] = value
		return true
	})

	// Verify all entries were found
	for key, value := range expected {
		if found[key] != value {
			t.Errorf("Expected %s=%s, got %s=%s", key, value, key, found[key])
		}
	}
}

func TestOptimizedIteratorForEachEarlyExit(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add some entries
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	iterator := NewOptimizedIterator(index, 1000)
	defer iterator.Close()

	// Test early exit
	count := 0
	iterator.ForEach(func(key []byte, value string) bool {
		count++
		return count < 5 // Exit after 5 entries
	})

	if count != 5 {
		t.Errorf("Expected 5 iterations, got %d", count)
	}
}

func TestOptimizedIteratorCollect(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add some entries
	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		entry := index.GetOrCreate(key)
		entry.Put(values[i], 1000)
	}

	iterator := NewOptimizedIterator(index, 1000)
	defer iterator.Close()

	// Test Collect
	flatKeys, collectedValues := iterator.Collect()

	// Verify flat keys
	expectedFlatKeys := []byte("key1key2key3")
	if string(flatKeys) != string(expectedFlatKeys) {
		t.Errorf("Expected flat keys %s, got %s", expectedFlatKeys, flatKeys)
	}

	// Verify values
	if len(collectedValues) != len(values) {
		t.Errorf("Expected %d values, got %d", len(values), len(collectedValues))
	}

	for i, value := range values {
		if collectedValues[i] != value {
			t.Errorf("Expected value %s at index %d, got %s", value, i, collectedValues[i])
		}
	}
}

func TestReadAheadIteratorEdgeCases(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Test with empty key (nil and empty hash to same bucket, so test only one)
	entry := index.GetOrCreate([]byte{})
	entry.Put("empty-value", 1000)

	iterator := NewReadAheadIterator(index, 1000, 10)
	defer iterator.Close()

	// Should find the entry
	found := false
	for iterator.Next() {
		key := iterator.Key()
		value, exists := iterator.Value()
		if exists && len(key) == 0 {
			found = true
			if value != "empty-value" {
				t.Errorf("Expected empty-value, got %s", value)
			}
		}
	}

	if !found {
		t.Error("Expected to find empty key entry")
	}
}

func TestReadAheadIteratorLargeBuffer(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add many entries
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i)}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	// Count actual entries (accounting for hash collisions)
	actualEntries := 0
	for i := range index.buckets {
		bucket := &index.buckets[i]
		for node := bucket.Load(); node != nil; node = node.next.Load() {
			if node.entry != nil {
				actualEntries++
			}
		}
	}

	// Use large buffer
	iterator := NewReadAheadIterator(index, 1000, 2000)
	defer iterator.Close()

	count := 0
	for iterator.Next() {
		count++
	}

	if count != actualEntries {
		t.Errorf("Expected %d entries, got %d", actualEntries, count)
	}
}

func TestReadAheadIteratorConcurrentModification(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)

	// Add initial entries
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		entry := index.GetOrCreate(key)
		entry.Put("value", 1000)
	}

	iterator := NewReadAheadIterator(index, 1000, 10)
	defer iterator.Close()

	// Start iteration
	go func() {
		for iterator.Next() {
			// Just iterate
		}
	}()

	// Add more entries concurrently
	go func() {
		for i := 100; i < 200; i++ {
			key := []byte{byte(i)}
			entry := index.GetOrCreate(key)
			entry.Put("value", 1000)
			time.Sleep(time.Microsecond)
		}
	}()

	// Wait a bit for concurrent operations
	time.Sleep(time.Millisecond)
}

func TestReadAheadIteratorClose(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewReadAheadIterator(index, 1000, 10)

	// Close should not panic
	iterator.Close()

	// Multiple closes should not panic
	iterator.Close()
}

func TestOptimizedIteratorEmptyIndex(t *testing.T) {
	t.Parallel()

	index := NewHashIndex[string](1024)
	iterator := NewOptimizedIterator(index, 1000)
	defer iterator.Close()

	// Test ForEach on empty index
	count := 0
	iterator.ForEach(func(key []byte, value string) bool {
		count++
		return true
	})

	if count != 0 {
		t.Errorf("Expected 0 iterations on empty index, got %d", count)
	}

	// Test Collect on empty index
	flatKeys, values := iterator.Collect()
	if len(flatKeys) != 0 {
		t.Errorf("Expected empty flat keys, got %d bytes", len(flatKeys))
	}
	if len(values) != 0 {
		t.Errorf("Expected empty values, got %d values", len(values))
	}
}
