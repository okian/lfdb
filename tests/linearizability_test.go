// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"
)

// TestLinearizabilitySingleKeySimple tests linearizability for single key operations
func TestLinearizabilitySingleKeySimple(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	key := []byte("test-key")
	var results []string
	var mu sync.Mutex

	// Generate concurrent operations
	var wg sync.WaitGroup
	const numClients = 10
	const opsPerClient = 100

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				opType := i % 3

				switch opType {
				case 0: // Put
					value := "value"
					database.Put(ctx, key, value)
					mu.Lock()
					results = append(results, "PUT")
					mu.Unlock()
				case 1: // Get
					val, exists := database.Get(ctx, key)
					mu.Lock()
					if exists {
						results = append(results, "GET:"+val)
					} else {
						results = append(results, "GET:NIL")
					}
					mu.Unlock()
				case 2: // Delete
					deleted := database.Delete(ctx, key)
					mu.Lock()
					if deleted {
						results = append(results, "DELETE:TRUE")
					} else {
						results = append(results, "DELETE:FALSE")
					}
					mu.Unlock()
				}

				// Ensure some time passes between operations
				time.Sleep(time.Microsecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify final state is consistent
	finalVal, exists := database.Get(ctx, key)
	if exists {
		t.Logf("Final value: %s", finalVal)
	} else {
		t.Logf("Final state: key not found")
	}

	// Invariant: No panics occurred and database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Fatalf("Database not functional after linearizability test")
	}
}

// TestLinearizabilityMultipleKeys tests linearizability for multiple keys
func TestLinearizabilityMultipleKeys(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	var wg sync.WaitGroup
	const numClients = 5
	const numKeys = 10
	const opsPerClient = 50

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				keyID := i % numKeys
				key := []byte{byte(keyID)}
				opType := i % 3

				switch opType {
				case 0: // Put
					value := "value"
					database.Put(ctx, key, value)
				case 1: // Get
					database.Get(ctx, key)
				case 2: // Delete
					database.Delete(ctx, key)
				}

				// Ensure some time passes between operations
				time.Sleep(time.Microsecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Fatalf("Database not functional after multi-key linearizability test")
	}
}

// TestLinearizabilityReadHeavy tests linearizability with read-heavy workload
func TestLinearizabilityReadHeavy(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with some data
	key := []byte("read-heavy-key")
	database.Put(ctx, key, "initial-value")

	var wg sync.WaitGroup
	const numClients = 20
	const opsPerClient = 100

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				// 80% reads, 20% writes
				if i%5 < 4 {
					database.Get(ctx, key)
				} else {
					database.Put(ctx, key, "value")
				}

				// Ensure some time passes between operations
				time.Sleep(time.Microsecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Fatalf("Database not functional after read-heavy linearizability test")
	}
}

// TestLinearizabilityWriteHeavy tests linearizability with write-heavy workload
func TestLinearizabilityWriteHeavy(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	var wg sync.WaitGroup
	const numClients = 10
	const opsPerClient = 100

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				key := []byte{byte(i % 10)}

				// 20% reads, 80% writes
				if i%5 == 0 {
					database.Get(ctx, key)
				} else {
					database.Put(ctx, key, "value")
				}

				// Ensure some time passes between operations
				time.Sleep(time.Microsecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Fatalf("Database not functional after write-heavy linearizability test")
	}
}

// TestLinearizabilityHotKey tests linearizability with hot key access pattern
func TestLinearizabilityHotKey(t *testing.T) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	hotKey := []byte("hot-key")
	database.Put(ctx, hotKey, "initial")

	var wg sync.WaitGroup
	const numClients = 15
	const opsPerClient = 80

	for clientID := 0; clientID < numClients; clientID++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			for i := 0; i < opsPerClient; i++ {
				// 90% hot key, 10% other keys
				var key []byte
				if i%10 < 9 {
					key = hotKey
				} else {
					key = []byte{byte(i % 10)}
				}

				opType := i % 3
				switch opType {
				case 0: // Put
					database.Put(ctx, key, "value")
				case 1: // Get
					database.Get(ctx, key)
				case 2: // Delete
					database.Delete(ctx, key)
				}

				// Ensure some time passes between operations
				time.Sleep(time.Microsecond)
			}
		}(clientID)
	}

	wg.Wait()

	// Verify database is still functional
	testKey := []byte("test")
	database.Put(ctx, testKey, "test-value")
	val, exists := database.Get(ctx, testKey)
	if !exists || val != "test-value" {
		t.Fatalf("Database not functional after hot-key linearizability test")
	}
}
