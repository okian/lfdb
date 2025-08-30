// Licensed under the MIT License. See LICENSE file in the project root for details.

package mvcc

import (
	"sync"
	"testing"
	"time"
)

func TestVersionPool(t *testing.T) {
	t.Run("NewVersionPool", func(t *testing.T) {
		pool := NewVersionPool[string]()
		if pool == nil {
			t.Fatal("NewVersionPool returned nil")
		}
	})

	t.Run("GetAndPut", func(t *testing.T) {
		pool := NewVersionPool[string]()

		// Get a version from the pool
		version := pool.Get()
		if version == nil {
			t.Fatal("Get returned nil")
		}

		// Check initial state
		if version.begin != ^uint64(0) {
			t.Errorf("Expected begin to be ^uint64(0), got %d", version.begin)
		}
		if version.end != ^uint64(0) {
			t.Errorf("Expected end to be ^uint64(0), got %d", version.end)
		}
		if version.tomb {
			t.Error("Expected tomb to be false")
		}
		if version.expiresAt != nil {
			t.Error("Expected expiresAt to be nil")
		}

		// Set some values
		version.val = "test value"
		version.begin = 100
		version.end = 200
		version.tomb = true
		expiresAt := time.Now().Add(time.Hour)
		version.expiresAt = &expiresAt

		// Put it back in the pool
		pool.Put(version)

		// Get another version (should be the same object, reset)
		version2 := pool.Get()
		if version2 == nil {
			t.Fatal("Get returned nil after Put")
		}

		// Check that it was reset
		if version2.begin != ^uint64(0) {
			t.Errorf("Expected begin to be reset to ^uint64(0), got %d", version2.begin)
		}
		if version2.end != ^uint64(0) {
			t.Errorf("Expected end to be reset to ^uint64(0), got %d", version2.end)
		}
		if version2.tomb {
			t.Error("Expected tomb to be reset to false")
		}
		if version2.expiresAt != nil {
			t.Error("Expected expiresAt to be reset to nil")
		}
		var zero string
		if version2.val != zero {
			t.Errorf("Expected val to be reset to zero value, got %v", version2.val)
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		pool := NewVersionPool[int]()
		var wg sync.WaitGroup
		const numGoroutines = 10
		const operationsPerGoroutine = 100

		// Start multiple goroutines that get and put versions
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < operationsPerGoroutine; j++ {
					version := pool.Get()
					version.val = id*1000 + j
					version.begin = uint64(j)
					version.end = uint64(j + 1)
					pool.Put(version)
				}
			}(i)
		}

		wg.Wait()

		// Verify we can still get versions from the pool
		version := pool.Get()
		if version == nil {
			t.Fatal("Get returned nil after concurrent access")
		}

		// Check that it's in a clean state
		if version.begin != ^uint64(0) {
			t.Errorf("Expected begin to be ^uint64(0), got %d", version.begin)
		}
		if version.end != ^uint64(0) {
			t.Errorf("Expected end to be ^uint64(0), got %d", version.end)
		}
		if version.tomb {
			t.Error("Expected tomb to be false")
		}
		if version.expiresAt != nil {
			t.Error("Expected expiresAt to be nil")
		}
		var zero int
		if version.val != zero {
			t.Errorf("Expected val to be zero value, got %v", version.val)
		}
	})

	t.Run("MultiplePuts", func(t *testing.T) {
		pool := NewVersionPool[float64]()

		// Create multiple versions and put them back
		versions := make([]*Version[float64], 10)
		for i := 0; i < 10; i++ {
			version := pool.Get()
			version.val = float64(i)
			version.begin = uint64(i)
			version.end = uint64(i + 1)
			versions[i] = version
		}

		// Put them all back
		for _, version := range versions {
			pool.Put(version)
		}

		// Get them all back and verify they're reset
		for i := 0; i < 10; i++ {
			version := pool.Get()
			if version == nil {
				t.Fatalf("Get returned nil for iteration %d", i)
			}

			// Check reset state
			if version.begin != ^uint64(0) {
				t.Errorf("Expected begin to be ^uint64(0), got %d", version.begin)
			}
			if version.end != ^uint64(0) {
				t.Errorf("Expected end to be ^uint64(0), got %d", version.end)
			}
			if version.tomb {
				t.Error("Expected tomb to be false")
			}
			if version.expiresAt != nil {
				t.Error("Expected expiresAt to be nil")
			}
			var zero float64
			if version.val != zero {
				t.Errorf("Expected val to be zero value, got %v", version.val)
			}
		}
	})

	t.Run("ComplexType", func(t *testing.T) {
		type ComplexType struct {
			ID   int
			Name string
			Data []byte
		}

		pool := NewVersionPool[ComplexType]()

		// Get a version and set complex data
		version := pool.Get()
		version.val = ComplexType{
			ID:   123,
			Name: "test",
			Data: []byte("test data"),
		}
		version.begin = 100
		version.end = 200
		version.tomb = true

		// Put it back
		pool.Put(version)

		// Get it back and verify reset
		version2 := pool.Get()
		if version2 == nil {
			t.Fatal("Get returned nil")
		}

		// Check reset state
		if version2.begin != ^uint64(0) {
			t.Errorf("Expected begin to be ^uint64(0), got %d", version2.begin)
		}
		if version2.end != ^uint64(0) {
			t.Errorf("Expected end to be ^uint64(0), got %d", version2.end)
		}
		if version2.tomb {
			t.Error("Expected tomb to be false")
		}
		if version2.expiresAt != nil {
			t.Error("Expected expiresAt to be nil")
		}
		var zero ComplexType
		if version2.val.ID != zero.ID || version2.val.Name != zero.Name || len(version2.val.Data) != 0 {
			t.Errorf("Expected val to be zero value, got %v", version2.val)
		}
	})
}

func TestEntryWithPool(t *testing.T) {
	t.Run("EntryUsesPool", func(t *testing.T) {
		entry := NewEntry[string]([]byte("testkey"))

		// Put a value using the pool
		entry.Put("test value", 100)

		// Verify the value was stored
		value, found := entry.Get(100)
		if !found {
			t.Error("Value not found after Put")
		}
		if value != "test value" {
			t.Errorf("Expected 'test value', got '%s'", value)
		}

		// Put another value (should reuse from pool)
		entry.Put("test value 2", 200)

		// Verify the new value
		value, found = entry.Get(200)
		if !found {
			t.Error("Second value not found after Put")
		}
		if value != "test value 2" {
			t.Errorf("Expected 'test value 2', got '%s'", value)
		}

		// Verify old value is still accessible
		value, found = entry.Get(150)
		if !found {
			t.Error("Old value not found at intermediate timestamp")
		}
		if value != "test value" {
			t.Errorf("Expected 'test value', got '%s'", value)
		}
	})

	t.Run("EntryWithTTLUsesPool", func(t *testing.T) {
		entry := NewEntry[string]([]byte("testkey"))

		// Put with TTL using the pool
		entry.PutWithTTL("test value", 100, time.Hour)

		// Verify the value was stored
		value, found := entry.Get(100)
		if !found {
			t.Error("Value not found after PutWithTTL")
		}
		if value != "test value" {
			t.Errorf("Expected 'test value', got '%s'", value)
		}

		// Check TTL
		ttl, found := entry.GetTTL(100)
		if !found {
			t.Error("TTL not found")
		}
		if ttl <= 0 {
			t.Error("TTL should be positive")
		}
	})

	t.Run("EntryDeleteUsesPool", func(t *testing.T) {
		entry := NewEntry[string]([]byte("testkey"))

		// Put a value first
		entry.Put("test value", 100)

		// Delete using the pool
		deleted := entry.Delete(200)
		if !deleted {
			t.Error("Delete should return true for new deletion")
		}

		// Verify the value is deleted
		_, found := entry.Get(250)
		if found {
			t.Error("Value should be deleted")
		}

		// Verify the value still exists at intermediate timestamp
		value, found := entry.Get(150)
		if !found {
			t.Error("Value should exist at intermediate timestamp")
		}
		if value != "test value" {
			t.Errorf("Expected 'test value', got '%s'", value)
		}
	})
}
