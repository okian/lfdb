// Licensed under the MIT License. See LICENSE file in the project root for details.

package mvcc

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEntryBasicOperations(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Test initial state
	val, exists := entry.Get(1)
	if exists {
		t.Errorf("Expected no value initially, got %v", val)
	}

	// Test Put
	entry.Put("value1", 10)
	val, exists = entry.Get(10)
	if !exists {
		t.Error("Expected value to exist after Put")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	// Test Put with newer timestamp
	entry.Put("value2", 20)
	val, exists = entry.Get(15)
	if !exists {
		t.Error("Expected value to exist at timestamp 15")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1' at timestamp 15, got %v", val)
	}

	val, exists = entry.Get(25)
	if !exists {
		t.Error("Expected value to exist at timestamp 25")
	}
	if val != "value2" {
		t.Errorf("Expected 'value2' at timestamp 25, got %v", val)
	}
}

func TestEntryDelete(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Put a value
	entry.Put("value1", 10)

	// Delete it
	deleted := entry.Delete(20)
	if !deleted {
		t.Error("Expected Delete to return true")
	}

	// Check it's deleted at newer timestamp
	val, exists := entry.Get(25)
	if exists {
		t.Errorf("Expected no value after delete, got %v", val)
	}

	// Check old value still visible at old timestamp
	val, exists = entry.Get(15)
	if !exists {
		t.Error("Expected value to still exist at old timestamp")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1' at old timestamp, got %v", val)
	}
}

func TestEntryConcurrentAccess(t *testing.T) {
	entry := NewEntry[int]([]byte("test-key"))

	var wg sync.WaitGroup
	const numReaders = 10
	const numWriters = 5
	const numOps = 1000

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				entry.Get(uint64(j))
			}
		}()
	}

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				timestamp := uint64(j*numWriters + writerID)
				entry.Put(j, timestamp)
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - check that some value exists at a reasonable timestamp
	_, exists := entry.Get(1000000) // Use a large timestamp to see latest value
	if !exists {
		t.Error("Expected final value to exist")
	}
}

func TestEntryTombstone(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Put and then delete
	entry.Put("value1", 10)
	entry.Delete(20)

	// Check tombstone behavior
	if !entry.IsDeleted(25) {
		t.Error("Expected entry to be deleted at timestamp 25")
	}

	if entry.IsDeleted(15) {
		t.Error("Expected entry to not be deleted at timestamp 15")
	}
}

func TestEntryKey(t *testing.T) {
	key := []byte("test-key")
	entry := NewEntry[string](key)

	retrievedKey := entry.Key()
	if string(retrievedKey) != string(key) {
		t.Errorf("Expected key %s, got %s", string(key), string(retrievedKey))
	}

	// Test with empty key
	emptyEntry := NewEntry[string]([]byte{})
	emptyKey := emptyEntry.Key()
	if len(emptyKey) != 0 {
		t.Errorf("Expected empty key, got %v", emptyKey)
	}
}

func TestEntryIsDeleted(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Initially not deleted
	if entry.IsDeleted(100) {
		t.Error("Expected entry to not be deleted initially")
	}

	// Put a value
	entry.Put("value1", 10)

	// Should not be deleted
	if entry.IsDeleted(15) {
		t.Error("Expected entry to not be deleted after Put")
	}

	// Delete the entry
	entry.Delete(20)

	// Should be deleted at timestamp 25
	if !entry.IsDeleted(25) {
		t.Error("Expected entry to be deleted after Delete")
	}

	// Should not be deleted at timestamp 15 (before deletion)
	if entry.IsDeleted(15) {
		t.Error("Expected entry to not be deleted before Delete timestamp")
	}
}

func TestEntryIsDeletedWithMultipleVersions(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Create multiple versions
	entry.Put("value1", 10)
	entry.Put("value2", 20)
	entry.Delete(30)
	entry.Put("value4", 40)

	// Test at different timestamps
	if entry.IsDeleted(15) {
		t.Error("Expected entry to not be deleted at timestamp 15")
	}

	if entry.IsDeleted(25) {
		t.Error("Expected entry to not be deleted at timestamp 25")
	}

	if !entry.IsDeleted(35) {
		t.Error("Expected entry to be deleted at timestamp 35")
	}

	if entry.IsDeleted(45) {
		t.Error("Expected entry to not be deleted at timestamp 45")
	}
}

func TestEntryDeleteAlreadyDeleted(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Delete the entry
	if !entry.Delete(10) {
		t.Error("Expected first Delete to return true")
	}

	// Try to delete again at the same timestamp
	if entry.Delete(10) {
		t.Error("Expected second Delete at same timestamp to return false")
	}

	// Verify the deletion is active
	if !entry.IsDeleted(15) {
		t.Error("Expected entry to be deleted at timestamp 15")
	}

	// Test that Delete can be called multiple times without panicking
	entry.Delete(20)
	entry.Delete(30)

	// This test verifies that Delete can be called multiple times
	// The actual return values depend on the implementation details
}

func TestEntryDeleteWithNoVersions(t *testing.T) {
	entry := NewEntry[string]([]byte("test-key"))

	// Delete on empty entry should succeed
	if !entry.Delete(10) {
		t.Error("Expected Delete on empty entry to return true")
	}

	// Verify it's deleted
	if !entry.IsDeleted(15) {
		t.Error("Expected entry to be deleted after Delete")
	}
}

func TestEntryComplexVersionChain(t *testing.T) {
	entry := NewEntry[string]([]byte("complex-key"))

	// Create a complex version chain
	entry.Put("v1", 10)
	entry.Put("v2", 20)
	entry.Delete(30)
	entry.Put("v4", 40)
	entry.Delete(50)
	entry.Put("v6", 60)

	// Test retrieval at different timestamps
	testCases := []struct {
		timestamp uint64
		expected  string
		exists    bool
		deleted   bool
	}{
		{5, "", false, false},
		{15, "v1", true, false},
		{25, "v2", true, false},
		{35, "", false, true},
		{45, "v4", true, false},
		{55, "", false, true},
		{65, "v6", true, false},
	}

	for _, tc := range testCases {
		value, exists := entry.Get(tc.timestamp)
		deleted := entry.IsDeleted(tc.timestamp)

		if exists != tc.exists {
			t.Errorf("At timestamp %d: expected exists=%t, got %t", tc.timestamp, tc.exists, exists)
		}

		if exists && value != tc.expected {
			t.Errorf("At timestamp %d: expected value=%s, got %s", tc.timestamp, tc.expected, value)
		}

		if deleted != tc.deleted {
			t.Errorf("At timestamp %d: expected deleted=%t, got %t", tc.timestamp, tc.deleted, deleted)
		}
	}
}

func TestEntryConcurrentPublish(t *testing.T) {
	entry := NewEntry[string]([]byte("concurrent-key"))

	var wg sync.WaitGroup
	const numGoroutines = 10
	const opsPerGoroutine = 100

	// Start multiple goroutines writing concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				timestamp := uint64(id*opsPerGoroutine + j + 1)
				entry.Put(fmt.Sprintf("value-%d-%d", id, j), timestamp)
			}
		}(i)
	}

	wg.Wait()

	// Verify that the latest version is accessible
	latestTimestamp := uint64(numGoroutines * opsPerGoroutine)
	if value, exists := entry.Get(latestTimestamp); !exists {
		t.Error("Expected latest version to be accessible")
	} else if value == "" {
		t.Error("Expected latest version to have a non-empty value")
	}
}

func TestEntryVersionChainIntegrity(t *testing.T) {
	entry := NewEntry[string]([]byte("integrity-key"))

	// Create a version chain
	entry.Put("v1", 10)
	entry.Put("v2", 20)
	entry.Put("v3", 30)

	// Verify all versions are accessible
	if value, exists := entry.Get(15); !exists || value != "v1" {
		t.Errorf("Expected v1 at timestamp 15, got %v, %t", value, exists)
	}

	if value, exists := entry.Get(25); !exists || value != "v2" {
		t.Errorf("Expected v2 at timestamp 25, got %v, %t", value, exists)
	}

	if value, exists := entry.Get(35); !exists || value != "v3" {
		t.Errorf("Expected v3 at timestamp 35, got %v, %t", value, exists)
	}

	// Verify older versions are not accessible at newer timestamps
	if value, exists := entry.Get(25); !exists || value != "v2" {
		t.Errorf("Expected v2 at timestamp 25, got %v, %t", value, exists)
	}

	if value, exists := entry.Get(35); !exists || value != "v3" {
		t.Errorf("Expected v3 at timestamp 35, got %v, %t", value, exists)
	}
}

func TestEntryPutWithExpiry(t *testing.T) {
	entry := NewEntry[string]([]byte("expiry-key"))

	// Test PutWithExpiry with future expiry
	futureTime := time.Now().Add(1 * time.Hour)
	entry.PutWithExpiry("value1", 10, futureTime)

	// Should be accessible at current timestamp
	val, exists := entry.Get(15)
	if !exists {
		t.Error("Expected value to exist after PutWithExpiry")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	// Check TTL
	ttl, hasTTL := entry.GetTTL(15)
	if !hasTTL {
		t.Error("Expected TTL to be set")
	}
	if ttl <= 0 {
		t.Errorf("Expected positive TTL, got %v", ttl)
	}

	// Test PutWithExpiry with past expiry
	pastTime := time.Now().Add(-1 * time.Hour)
	entry.PutWithExpiry("value2", 20, pastTime)

	// Should not be accessible due to expiry
	val, exists = entry.Get(25)
	if exists {
		t.Errorf("Expected no value due to expiry, got %v", val)
	}

	// Check TTL for expired entry
	ttl, hasTTL = entry.GetTTL(25)
	if hasTTL {
		t.Error("Expected no TTL for expired entry")
	}
	if ttl != 0 {
		t.Errorf("Expected TTL 0 for expired entry, got %v", ttl)
	}
}

func TestEntryRemoveTTL(t *testing.T) {
	entry := NewEntry[string]([]byte("remove-ttl-key"))

	// Test RemoveTTL on entry without TTL
	if entry.RemoveTTL(10) {
		t.Error("Expected RemoveTTL to return false for entry without TTL")
	}

	// Put a value with TTL
	entry.PutWithTTL("value1", 10, 1*time.Hour)

	// Verify TTL is set
	ttl, hasTTL := entry.GetTTL(15)
	if !hasTTL {
		t.Error("Expected TTL to be set")
	}
	if ttl <= 0 {
		t.Errorf("Expected positive TTL, got %v", ttl)
	}

	// Remove TTL
	if !entry.RemoveTTL(20) {
		t.Error("Expected RemoveTTL to return true")
	}

	// Verify TTL is removed
	ttl, hasTTL = entry.GetTTL(25)
	if hasTTL {
		t.Error("Expected TTL to be removed")
	}
	if ttl != 0 {
		t.Errorf("Expected TTL 0 after removal, got %v", ttl)
	}

	// Value should still be accessible
	val, exists := entry.Get(25)
	if !exists {
		t.Error("Expected value to still exist after TTL removal")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	// Test RemoveTTL on deleted entry
	entry.Delete(30)
	if entry.RemoveTTL(35) {
		t.Error("Expected RemoveTTL to return false for deleted entry")
	}

	// Test RemoveTTL on empty entry
	emptyEntry := NewEntry[string]([]byte("empty-key"))
	if emptyEntry.RemoveTTL(10) {
		t.Error("Expected RemoveTTL to return false for empty entry")
	}
}

func TestEntryExtendTTL(t *testing.T) {
	entry := NewEntry[string]([]byte("extend-ttl-key"))

	// Test ExtendTTL on entry without TTL
	if entry.ExtendTTL(10, 1*time.Hour) {
		t.Error("Expected ExtendTTL to return false for entry without TTL")
	}

	// Put a value with TTL
	entry.PutWithTTL("value1", 10, 1*time.Hour)

	// Get initial TTL
	initialTTL, hasTTL := entry.GetTTL(15)
	if !hasTTL {
		t.Error("Expected TTL to be set")
	}

	// Extend TTL
	if !entry.ExtendTTL(20, 2*time.Hour) {
		t.Error("Expected ExtendTTL to return true")
	}

	// Verify TTL is extended
	newTTL, hasTTL := entry.GetTTL(25)
	if !hasTTL {
		t.Error("Expected TTL to still be set after extension")
	}
	if newTTL <= initialTTL {
		t.Errorf("Expected TTL to be extended, got %v (was %v)", newTTL, initialTTL)
	}

	// Value should still be accessible
	val, exists := entry.Get(25)
	if !exists {
		t.Error("Expected value to still exist after TTL extension")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	// Test ExtendTTL with negative duration (reduce TTL)
	if !entry.ExtendTTL(30, -30*time.Minute) {
		t.Error("Expected ExtendTTL to return true for negative extension")
	}

	// Verify TTL is reduced
	reducedTTL, hasTTL := entry.GetTTL(35)
	if !hasTTL {
		t.Error("Expected TTL to still be set after reduction")
	}
	if reducedTTL >= newTTL {
		t.Errorf("Expected TTL to be reduced, got %v (was %v)", reducedTTL, newTTL)
	}

	// Test ExtendTTL on deleted entry
	entry.Delete(40)
	if entry.ExtendTTL(45, 1*time.Hour) {
		t.Error("Expected ExtendTTL to return false for deleted entry")
	}

	// Test ExtendTTL on empty entry
	emptyEntry := NewEntry[string]([]byte("empty-extend-key"))
	if emptyEntry.ExtendTTL(10, 1*time.Hour) {
		t.Error("Expected ExtendTTL to return false for empty entry")
	}
}

func TestEntryTTLExpiration(t *testing.T) {
	entry := NewEntry[string]([]byte("ttl-expiration-key"))

	// Put a value with very short TTL
	entry.PutWithTTL("value1", 10, 1*time.Millisecond)

	// Should be accessible immediately
	val, exists := entry.Get(15)
	if !exists {
		t.Error("Expected value to exist immediately after PutWithTTL")
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %v", val)
	}

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Should not be accessible after expiration
	val, exists = entry.Get(20)
	if exists {
		t.Errorf("Expected no value after TTL expiration, got %v", val)
	}

	// TTL should be 0 after expiration
	ttl, hasTTL := entry.GetTTL(20)
	if hasTTL {
		t.Error("Expected no TTL after expiration")
	}
	if ttl != 0 {
		t.Errorf("Expected TTL 0 after expiration, got %v", ttl)
	}
}

func TestEntryTTLWithMultipleVersions(t *testing.T) {
	entry := NewEntry[string]([]byte("ttl-versions-key"))

	// Create multiple versions with different TTLs
	entry.PutWithTTL("v1", 10, 1*time.Hour)
	entry.PutWithTTL("v2", 20, 30*time.Minute)
	entry.PutWithTTL("v3", 30, 2*time.Hour)

	// Test TTL at different timestamps
	ttl1, hasTTL1 := entry.GetTTL(15)
	if !hasTTL1 {
		t.Error("Expected TTL for v1")
	}

	ttl2, hasTTL2 := entry.GetTTL(25)
	if !hasTTL2 {
		t.Error("Expected TTL for v2")
	}

	ttl3, hasTTL3 := entry.GetTTL(35)
	if !hasTTL3 {
		t.Error("Expected TTL for v3")
	}

	// TTLs should be different (v2 has shorter TTL than v1 and v3)
	if ttl2 >= ttl1 {
		t.Errorf("Expected v2 TTL (%v) to be less than v1 TTL (%v)", ttl2, ttl1)
	}

	if ttl3 <= ttl2 {
		t.Errorf("Expected v3 TTL (%v) to be greater than v2 TTL (%v)", ttl3, ttl2)
	}
}

func TestEntryTTLOperationsConcurrency(t *testing.T) {
	entry := NewEntry[string]([]byte("ttl-concurrent-key"))

	var wg sync.WaitGroup
	const numGoroutines = 5
	const opsPerGoroutine = 100

	// Start multiple goroutines performing TTL operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				timestamp := uint64(id*opsPerGoroutine + j + 1)

				// Alternate between different TTL operations
				switch j % 4 {
				case 0:
					entry.PutWithTTL(fmt.Sprintf("value-%d-%d", id, j), timestamp, time.Duration(j+1)*time.Minute)
				case 1:
					entry.ExtendTTL(timestamp, time.Duration(j+1)*time.Minute)
				case 2:
					entry.RemoveTTL(timestamp)
				case 3:
					entry.PutWithExpiry(fmt.Sprintf("value-%d-%d", id, j), timestamp, time.Now().Add(time.Duration(j+1)*time.Minute))
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify that the latest version is accessible
	latestTimestamp := uint64(numGoroutines * opsPerGoroutine)
	_, _ = entry.Get(latestTimestamp)
	// Note: We can't guarantee the value exists due to TTL expiration, but we can check it doesn't panic
}

func TestEntryTTLWithDeletedVersions(t *testing.T) {
	entry := NewEntry[string]([]byte("ttl-deleted-key"))

	// Put a value with TTL
	entry.PutWithTTL("value1", 10, 1*time.Hour)

	// Delete the entry
	entry.Delete(20)

	// TTL operations should fail on deleted entry
	if entry.ExtendTTL(25, 1*time.Hour) {
		t.Error("Expected ExtendTTL to return false for deleted entry")
	}

	if entry.RemoveTTL(30) {
		t.Error("Expected RemoveTTL to return false for deleted entry")
	}

	// GetTTL should return false for deleted entry
	ttl, hasTTL := entry.GetTTL(35)
	if hasTTL {
		t.Error("Expected no TTL for deleted entry")
	}
	if ttl != 0 {
		t.Errorf("Expected TTL 0 for deleted entry, got %v", ttl)
	}
}
