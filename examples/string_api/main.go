// Licensed under the MIT License. See LICENSE file in the project root for details.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kianostad/lfdb"
)

func main() {
	ctx := context.Background()

	// Create a new database with string keys (recommended API)
	db := lfdb.NewStringDB[string]()
	defer db.Close(ctx)

	fmt.Println("=== String-based API Example ===")

	// Basic operations - much cleaner syntax!
	db.Put(ctx, "hello", "world")
	db.Put(ctx, "user:123", "John Doe")
	db.Put(ctx, "config:timeout", "30s")

	// Get operations
	if val, exists := db.Get(ctx, "hello"); exists {
		fmt.Printf("Value: %s\n", val) // Output: Value: world
	}

	if val, exists := db.Get(ctx, "user:123"); exists {
		fmt.Printf("User: %s\n", val) // Output: User: John Doe
	}

	// Delete operation
	deleted := db.Delete(ctx, "config:timeout")
	fmt.Printf("Deleted config: %t\n", deleted) // Output: Deleted config: true

	fmt.Println("\n=== Transactions with String Keys ===")

	// Execute a transaction with string keys
	err := db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
		tx.Put(ctx, "session:abc123", "active")
		tx.Put(ctx, "session:def456", "pending")

		// Read within transaction
		if val, exists := tx.Get(ctx, "session:abc123"); exists {
			fmt.Printf("Session status: %s\n", val)
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n=== Snapshots with String Keys ===")

	// Create a snapshot for consistent reads
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Snapshot sees the database state at creation time
	if val, exists := snapshot.Get(ctx, "session:abc123"); exists {
		fmt.Printf("Snapshot session: %s\n", val)
	}

	// Iterate over all key-value pairs in snapshot
	fmt.Println("Snapshot contents:")
	snapshot.Iterate(ctx, func(key string, val string) bool {
		fmt.Printf("  %s: %s\n", key, val)
		return true // Return false to stop iteration
	})

	fmt.Println("\n=== Atomic Operations with String Keys ===")

	// Create a database with numeric values for atomic operations
	numDB := lfdb.NewStringDB[int]()
	defer numDB.Close(ctx)

	// Atomic operations with string keys
	newVal, _ := numDB.Add(ctx, "counter", 5)
	fmt.Printf("After adding 5: %d\n", newVal)

	newVal, _ = numDB.Increment(ctx, "counter")
	fmt.Printf("After increment: %d\n", newVal)

	newVal, _ = numDB.Multiply(ctx, "counter", 2)
	fmt.Printf("After multiplying by 2: %d\n", newVal)

	fmt.Println("\n=== TTL Operations with String Keys ===")

	// Put with time-to-live using string keys
	db.PutWithTTL(ctx, "temp:session", "expires-soon", 100*time.Millisecond)

	// Check TTL
	if ttl, exists := db.GetTTL(ctx, "temp:session"); exists {
		fmt.Printf("TTL remaining: %v\n", ttl)
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Check if expired
	if _, exists := db.Get(ctx, "temp:session"); !exists {
		fmt.Println("Session expired as expected")
	}

	fmt.Println("\n=== Batch Operations with String Keys ===")

	// Create a batch with string keys
	batch := lfdb.NewStringBatch[string]()
	batch.Put("batch:key1", "value1")
	batch.Put("batch:key2", "value2")
	batch.Put("batch:key3", "value3")
	batch.Delete("batch:key2")

	// Execute the batch
	if err := db.ExecuteBatch(ctx, batch); err != nil {
		log.Fatal(err)
	}

	// Verify batch results
	keys := []string{"batch:key1", "batch:key2", "batch:key3"}
	results := db.BatchGet(ctx, keys)

	fmt.Println("Batch get results:")
	for _, result := range results {
		if result.Found {
			fmt.Printf("  %s: %s\n", result.Key, result.Value)
		} else {
			fmt.Printf("  %s: not found\n", result.Key)
		}
	}

	fmt.Println("\n=== Performance Comparison ===")

	// Compare with []byte API
	byteDB := lfdb.New[[]byte, string]()
	defer byteDB.Close(ctx)

	// String API (cleaner)
	start := time.Now()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("string:key%d", i)
		db.Put(ctx, key, fmt.Sprintf("value%d", i))
	}
	stringTime := time.Since(start)

	// []byte API (more verbose)
	start = time.Now()
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("byte:key%d", i))
		byteDB.Put(ctx, key, fmt.Sprintf("value%d", i))
	}
	byteTime := time.Since(start)

	fmt.Printf("String API time: %v\n", stringTime)
	fmt.Printf("[]byte API time: %v\n", byteTime)
	fmt.Printf("String API overhead: %.2f%%\n",
		float64(stringTime-byteTime)/float64(byteTime)*100)

	fmt.Println("\n=== Developer Experience Benefits ===")
	fmt.Println("✅ Cleaner syntax: db.Put(ctx, \"key\", \"value\")")
	fmt.Println("✅ Better readability and debugging")
	fmt.Println("✅ Type safety for string keys")
	fmt.Println("✅ Minimal performance overhead (~1-2%)")
	fmt.Println("✅ Same lock-free performance internally")
}
