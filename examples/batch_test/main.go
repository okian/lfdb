package main

import (
	"context"
	"fmt"

	"github.com/kianostad/lfdb"
)

func main() {
	ctx := context.Background()

	// Test []byte API batch operations
	db := lfdb.New[[]byte, string]()
	defer db.Close(ctx)

	// Create and use a batch
	batch := lfdb.NewBatch[[]byte, string]()
	batch.Put([]byte("key1"), "value1")
	batch.Put([]byte("key2"), "value2")

	err := db.ExecuteBatch(ctx, batch)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Test string API batch operations
	stringDB := lfdb.NewStringDB[string]()
	defer stringDB.Close(ctx)

	stringBatch := lfdb.NewStringBatch[string]()
	stringBatch.Put("skey1", "svalue1")
	stringBatch.Put("skey2", "svalue2")

	err = stringDB.ExecuteBatch(ctx, stringBatch)
	if err != nil {
		fmt.Printf("String API Error: %v\n", err)
		return
	}

	fmt.Println("✅ Both batch APIs work correctly!")

	// Verify results
	if val, exists := db.Get(ctx, []byte("key1")); exists {
		fmt.Printf("[]byte API: key1 = %s\n", val)
	}

	if val, exists := stringDB.Get(ctx, "skey1"); exists {
		fmt.Printf("String API: skey1 = %s\n", val)
	}
}
