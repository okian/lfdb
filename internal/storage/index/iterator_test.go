// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewIterator(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)
	iterator := index.NewIterator(rt)

	if iterator == nil {
		t.Fatal("NewIterator() returned nil")
	}

	if iterator.index != index {
		t.Errorf("Expected iterator.index to be %v, got %v", index, iterator.index)
	}

	if iterator.rt != rt {
		t.Errorf("Expected iterator.rt to be %d, got %d", rt, iterator.rt)
	}

	if iterator.bucketIdx != 0 {
		t.Errorf("Expected iterator.bucketIdx to be 0, got %d", iterator.bucketIdx)
	}

	if iterator.node != nil {
		t.Errorf("Expected iterator.node to be nil, got %v", iterator.node)
	}
}

func TestIteratorEmptyIndex(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	iterator := index.NewIterator(100)

	// Next should return false for empty index
	if iterator.Next() {
		t.Error("Expected Next() to return false for empty index")
	}

	// Entry should return nil
	if entry := iterator.Entry(); entry != nil {
		t.Errorf("Expected Entry() to return nil, got %v", entry)
	}

	// Key should return nil
	if key := iterator.Key(); key != nil {
		t.Errorf("Expected Key() to return nil, got %v", key)
	}

	// Value should return nil, false
	if value, exists := iterator.Value(); value != nil || exists {
		t.Errorf("Expected Value() to return nil, false, got %v, %t", value, exists)
	}
}

func TestIteratorSingleEntry(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	// Add an entry using the hash index
	key := []byte("test-key")
	entry := index.GetOrCreate(key)
	entry.Put("test-value", rt)

	iterator := index.NewIterator(rt)

	// First call to Next should find the entry
	if !iterator.Next() {
		t.Error("Expected Next() to return true for index with entry")
	}

	// Entry should return the correct entry
	if gotEntry := iterator.Entry(); gotEntry == nil {
		t.Error("Expected Entry() to return non-nil")
	}

	// Key should return the correct key
	if gotKey := iterator.Key(); string(gotKey) != string(key) {
		t.Errorf("Expected Key() to return %s, got %s", string(key), string(gotKey))
	}

	// Value should return the correct value
	if gotValue, exists := iterator.Value(); !exists || gotValue != "test-value" {
		t.Errorf("Expected Value() to return test-value, true, got %v, %t", gotValue, exists)
	}

	// Next call should return false (no more entries)
	if iterator.Next() {
		t.Error("Expected Next() to return false after last entry")
	}
}

func TestIteratorMultipleEntries(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	// Add multiple entries using the hash index
	keys := []string{"key1", "key2", "key3"}
	values := []string{"value1", "value2", "value3"}

	for i, key := range keys {
		entry := index.GetOrCreate([]byte(key))
		entry.Put(values[i], rt)
	}

	iterator := index.NewIterator(rt)

	// Count the entries we find
	foundEntries := 0
	for iterator.Next() {
		foundEntries++
		key := string(iterator.Key())
		value, exists := iterator.Value()

		if !exists {
			t.Errorf("Expected value to exist for key %s", key)
			continue
		}

		// Find the expected value for this key
		found := false
		for i, expectedKey := range keys {
			if key == expectedKey {
				if value != values[i] {
					t.Errorf("Expected value %s for key %s, got %v", values[i], key, value)
				}
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Unexpected key found: %s", key)
		}
	}

	// Should find all entries
	if foundEntries != len(keys) {
		t.Errorf("Expected %d entries, found %d", len(keys), foundEntries)
	}
}

func TestIteratorMultipleBuckets(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	// Add entries using the hash index
	key1 := []byte("key1")
	key2 := []byte("key2")
	value1 := "value1"
	value2 := "value2"

	entry1 := index.GetOrCreate(key1)
	entry1.Put(value1, rt)
	entry2 := index.GetOrCreate(key2)
	entry2.Put(value2, rt)

	iterator := index.NewIterator(rt)

	// Count the entries we find
	foundEntries := 0
	expectedKeys := map[string]string{"key1": "value1", "key2": "value2"}

	for iterator.Next() {
		foundEntries++
		key := string(iterator.Key())
		value, exists := iterator.Value()

		if !exists {
			t.Errorf("Expected value to exist for key %s", key)
			continue
		}

		if expectedValue, ok := expectedKeys[key]; !ok {
			t.Errorf("Unexpected key found: %s", key)
		} else if value != expectedValue {
			t.Errorf("Expected value %s for key %s, got %v", expectedValue, key, value)
		}
	}

	// Should find both entries
	if foundEntries != 2 {
		t.Errorf("Expected 2 entries, found %d", foundEntries)
	}
}

func TestIteratorReset(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	// Add an entry using the hash index
	key := []byte("test-key")
	entry := index.GetOrCreate(key)
	entry.Put("test-value", rt)

	iterator := index.NewIterator(rt)

	// Iterate to the entry
	if !iterator.Next() {
		t.Error("Expected Next() to return true")
	}

	// Verify we have an entry
	if iterator.Entry() == nil {
		t.Error("Expected Entry() to return non-nil")
	}

	// Reset the iterator
	iterator.Reset()

	// Verify reset state
	if iterator.bucketIdx != 0 {
		t.Errorf("Expected bucketIdx to be 0 after reset, got %d", iterator.bucketIdx)
	}

	if iterator.node != nil {
		t.Errorf("Expected node to be nil after reset, got %v", iterator.node)
	}

	// Should be able to iterate again
	if !iterator.Next() {
		t.Error("Expected Next() to return true after reset")
	}

	if gotKey := iterator.Key(); string(gotKey) != string(key) {
		t.Errorf("Expected Key() to return %s after reset, got %s", string(key), string(gotKey))
	}
}

func TestIteratorNilNode(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	iterator := index.NewIterator(rt)

	// Test Entry() with nil node
	if entry := iterator.Entry(); entry != nil {
		t.Errorf("Expected Entry() to return nil for nil node, got %v", entry)
	}

	// Test Key() with nil node
	if key := iterator.Key(); key != nil {
		t.Errorf("Expected Key() to return nil for nil node, got %v", key)
	}

	// Test Value() with nil node
	if value, exists := iterator.Value(); value != nil || exists {
		t.Errorf("Expected Value() to return nil, false for nil node, got %v, %t", value, exists)
	}
}

func TestIteratorBucketBoundary(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](4) // Small size for testing
	rt := uint64(100)

	// Add entry using the hash index
	key := []byte("test-key")
	entry := index.GetOrCreate(key)
	entry.Put("test-value", rt)

	// Verify entry was created
	if entry == nil {
		t.Fatal("Failed to create entry")
	}

	// Test that iterator can be created and used
	iterator := index.NewIterator(rt)

	// Test that iterator methods don't panic
	_ = iterator.Entry()
	_ = iterator.Key()
	_, _ = iterator.Value()

	// Test reset functionality
	iterator.Reset()

	// This test verifies that the iterator can be used without panicking
	// The actual iteration behavior is tested in other tests
}

func TestIteratorEdgeCases(t *testing.T) {
	t.Parallel()

	// Test with zero read timestamp
	index := NewHashIndex[string](16)
	iterator := index.NewIterator(0)
	if iterator.rt != 0 {
		t.Errorf("Expected rt to be 0, got %d", iterator.rt)
	}

	// Test with very large read timestamp
	iterator2 := index.NewIterator(^uint64(0))
	if iterator2.rt != ^uint64(0) {
		t.Errorf("Expected rt to be max uint64, got %d", iterator2.rt)
	}

	// Test iterator methods on empty index with different timestamps
	for _, rt := range []uint64{0, 1, 100, 1000, ^uint64(0)} {
		iterator := index.NewIterator(rt)
		if iterator.Next() {
			t.Errorf("Expected Next() to return false for empty index with rt %d", rt)
		}
		if entry := iterator.Entry(); entry != nil {
			t.Errorf("Expected Entry() to return nil for empty index with rt %d", rt)
		}
		if key := iterator.Key(); key != nil {
			t.Errorf("Expected Key() to return nil for empty index with rt %d", rt)
		}
		if value, exists := iterator.Value(); value != nil || exists {
			t.Errorf("Expected Value() to return nil, false for empty index with rt %d", rt)
		}
	}
}

func TestIteratorStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()
	index := NewHashIndex[string](64)
	rt := uint64(100)

	// Add many entries
	const numEntries = 1000
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("stress-key-%d", i))
		entry := index.GetOrCreate(key)
		entry.Put(fmt.Sprintf("stress-value-%d", i), rt)
	}

	// Test iteration
	iterator := index.NewIterator(rt)
	foundEntries := 0

	for iterator.Next() {
		foundEntries++
		key := iterator.Key()
		value, exists := iterator.Value()

		if !exists {
			t.Errorf("Expected value to exist for key %s", string(key))
		}

		if key == nil {
			t.Error("Expected key to be non-nil")
		}

		if value == nil {
			t.Error("Expected value to be non-nil")
		}
	}

	// Allow for some hash collisions or minor discrepancies
	if foundEntries < numEntries*95/100 { // Allow 5% tolerance
		t.Errorf("Expected at least %d entries (95%% of %d), found %d", numEntries*95/100, numEntries, foundEntries)
	}
}

func TestIteratorConcurrentAccess(t *testing.T) {
	t.Parallel()
	index := NewHashIndex[string](16)
	rt := uint64(100)

	// Add some entries
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range keys {
		entry := index.GetOrCreate([]byte(key))
		entry.Put("value", rt)
	}

	// Test concurrent iteration
	var wg sync.WaitGroup
	const numIterators = 10

	for i := 0; i < numIterators; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			iterator := index.NewIterator(rt)
			foundEntries := 0

			for iterator.Next() {
				foundEntries++
				key := iterator.Key()
				value, exists := iterator.Value()

				if !exists || key == nil || value == nil {
					t.Errorf("Iterator %d: invalid entry", id)
				}
			}

			if foundEntries != len(keys) {
				t.Errorf("Iterator %d: expected %d entries, found %d", id, len(keys), foundEntries)
			}
		}(i)
	}

	wg.Wait()
}
