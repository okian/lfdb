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

	// Create a new database with string keys and string values
	db := lfdb.New[[]byte, string]()
	defer db.Close(ctx)

	// Basic operations
	fmt.Println("=== Basic Operations ===")
	db.Put(ctx, []byte("hello"), "world")
	db.Put(ctx, []byte("foo"), "bar")

	value, exists := db.Get(ctx, []byte("hello"))
	fmt.Printf("Get 'hello': %s, exists: %t\n", value, exists)

	deleted := db.Delete(ctx, []byte("foo"))
	fmt.Printf("Deleted 'foo': %t\n", deleted)

	// Transactions
	fmt.Println("\n=== Transactions ===")
	err := db.Txn(ctx, func(tx lfdb.Txn[[]byte, string]) error {
		tx.Put(ctx, []byte("key1"), "value1")
		tx.Put(ctx, []byte("key2"), "value2")
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// Verify transaction results
	value1, _ := db.Get(ctx, []byte("key1"))
	value2, _ := db.Get(ctx, []byte("key2"))
	fmt.Printf("Transaction results: key1=%s, key2=%s\n", value1, value2)

	// Snapshots
	fmt.Println("\n=== Snapshots ===")
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Modify data after snapshot
	db.Put(ctx, []byte("key3"), "value3")

	// Snapshot won't see the new data
	snapshotValue, exists := snapshot.Get(ctx, []byte("key3"))
	fmt.Printf("Snapshot get 'key3': %s, exists: %t\n", snapshotValue, exists)

	// But current database will
	currentValue, exists := db.Get(ctx, []byte("key3"))
	fmt.Printf("Current get 'key3': %s, exists: %t\n", currentValue, exists)

	// TTL operations
	fmt.Println("\n=== TTL Operations ===")
	db.PutWithTTL(ctx, []byte("temp"), "temporary value", 2*time.Second)

	ttl, exists := db.GetTTL(ctx, []byte("temp"))
	fmt.Printf("TTL for 'temp': %v, exists: %t\n", ttl, exists)

	// Wait a bit and check if expired
	time.Sleep(3 * time.Second)
	value, exists = db.Get(ctx, []byte("temp"))
	fmt.Printf("After expiration - 'temp': %s, exists: %t\n", value, exists)

	// Batch operations
	fmt.Println("\n=== Batch Operations ===")
	keys := [][]byte{[]byte("batch1"), []byte("batch2"), []byte("batch3")}
	values := []string{"value1", "value2", "value3"}

	err = db.BatchPut(ctx, keys, values)
	if err != nil {
		log.Fatal(err)
	}

	results := db.BatchGet(ctx, keys)
	for i, result := range results {
		fmt.Printf("Batch get %d: key=%s, value=%s, found=%t\n",
			i, string(keys[i]), result.Value, result.Found)
	}

	// Numeric database example
	fmt.Println("\n=== Numeric Operations ===")
	numDB := lfdb.New[[]byte, int]()
	defer numDB.Close(ctx)

	// Atomic operations
	newVal, _ := numDB.Add(ctx, []byte("counter"), 10)
	fmt.Printf("Add 10 to counter: %d\n", newVal)

	newVal, _ = numDB.Increment(ctx, []byte("counter"))
	fmt.Printf("Increment counter: %d\n", newVal)

	newVal, _ = numDB.Multiply(ctx, []byte("counter"), 2)
	fmt.Printf("Multiply counter by 2: %d\n", newVal)

	// Metrics
	fmt.Println("\n=== Metrics ===")
	metrics := db.GetMetrics(ctx)
	fmt.Printf("Database metrics: %+v\n", metrics)
}
