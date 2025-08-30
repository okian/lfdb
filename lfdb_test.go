// Licensed under the MIT License. See LICENSE file in the project root for details.

package lfdb

import (
	"context"
	"testing"
	"time"
)

func TestPublicAPI(t *testing.T) {
	ctx := context.Background()

	// Test basic database creation
	db := New[[]byte, string]()
	defer db.Close(ctx)

	// Test basic operations
	db.Put(ctx, []byte("key1"), "value1")
	value, exists := db.Get(ctx, []byte("key1"))
	if !exists || value != "value1" {
		t.Errorf("Expected value1, got %s, exists: %t", value, exists)
	}

	// Test transactions
	err := db.Txn(ctx, func(tx Txn[[]byte, string]) error {
		tx.Put(ctx, []byte("key2"), "value2")
		tx.Put(ctx, []byte("key3"), "value3")
		return nil
	})
	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}

	// Verify transaction results
	value, exists = db.Get(ctx, []byte("key2"))
	if !exists || value != "value2" {
		t.Errorf("Expected value2, got %s, exists: %t", value, exists)
	}

	// Test snapshots
	snapshot := db.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Modify data after snapshot
	db.Put(ctx, []byte("key4"), "value4")

	// Snapshot shouldn't see new data
	value, exists = snapshot.Get(ctx, []byte("key4"))
	if exists {
		t.Errorf("Snapshot should not see new data, but got %s", value)
	}

	// Test TTL
	db.PutWithTTL(ctx, []byte("temp"), "temp_value", 100*time.Millisecond)
	ttl, exists := db.GetTTL(ctx, []byte("temp"))
	if !exists || ttl <= 0 {
		t.Errorf("Expected positive TTL, got %v, exists: %t", ttl, exists)
	}

	// Test batch operations
	keys := [][]byte{[]byte("batch1"), []byte("batch2")}
	values := []string{"batch_value1", "batch_value2"}
	err = db.BatchPut(ctx, keys, values)
	if err != nil {
		t.Errorf("Batch put failed: %v", err)
	}

	results := db.BatchGet(ctx, keys)
	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// Test convenience constructors
	stringDB := NewStringDBLegacy()
	defer stringDB.Close(ctx)
	stringDB.Put(ctx, []byte("convenience"), "test")

	intDB := NewIntDB()
	defer intDB.Close(ctx)
	newVal, _ := intDB.Add(ctx, []byte("counter"), 5)
	if newVal != 5 {
		t.Errorf("Expected 5, got %d", newVal)
	}
}

func TestConvenienceTypes(t *testing.T) {
	ctx := context.Background()

	// Test all convenience types
	stringDB := NewStringDBLegacy()
	defer stringDB.Close(ctx)
	stringDB.Put(ctx, []byte("test"), "value")

	intDB := NewIntDB()
	defer intDB.Close(ctx)
	intDB.Put(ctx, []byte("test"), 42)

	int64DB := NewInt64DB()
	defer int64DB.Close(ctx)
	int64DB.Put(ctx, []byte("test"), int64(42))

	float64DB := NewFloat64DB()
	defer float64DB.Close(ctx)
	float64DB.Put(ctx, []byte("test"), 3.14)

	bytesDB := NewBytesDB()
	defer bytesDB.Close(ctx)
	bytesDB.Put(ctx, []byte("test"), []byte("value"))

	// Verify they work
	value, exists := stringDB.Get(ctx, []byte("test"))
	if !exists || value != "value" {
		t.Errorf("StringDB failed: got %s, exists: %t", value, exists)
	}

	intVal, exists := intDB.Get(ctx, []byte("test"))
	if !exists || intVal != 42 {
		t.Errorf("IntDB failed: got %d, exists: %t", intVal, exists)
	}
}

func TestNewStringDB(t *testing.T) {
	ctx := context.Background()

	// Test NewStringDB with string values
	stringDB := NewStringDB[string]()
	if stringDB == nil {
		t.Fatal("NewStringDB[string]() returned nil")
	}
	defer stringDB.Close(ctx)

	// Test basic operations
	stringDB.Put(ctx, "key1", "value1")

	value, exists := stringDB.Get(ctx, "key1")
	if !exists || value != "value1" {
		t.Errorf("Expected value1, got %s, exists: %t", value, exists)
	}

	// Test NewStringDB with int values
	intDB := NewStringDB[int]()
	if intDB == nil {
		t.Fatal("NewStringDB[int]() returned nil")
	}
	defer intDB.Close(ctx)

	intDB.Put(ctx, "counter", 42)

	intValue, exists := intDB.Get(ctx, "counter")
	if !exists || intValue != 42 {
		t.Errorf("Expected 42, got %d, exists: %t", intValue, exists)
	}

	// Test NewStringDB with custom struct
	type User struct {
		Name  string
		Age   int
		Email string
	}

	userDB := NewStringDB[User]()
	if userDB == nil {
		t.Fatal("NewStringDB[User]() returned nil")
	}
	defer userDB.Close(ctx)

	user := User{Name: "John", Age: 30, Email: "john@example.com"}
	userDB.Put(ctx, "user:1", user)

	retrievedUser, exists := userDB.Get(ctx, "user:1")
	if !exists {
		t.Fatal("User not found")
	}
	if retrievedUser.Name != user.Name || retrievedUser.Age != user.Age || retrievedUser.Email != user.Email {
		t.Errorf("Expected %+v, got %+v", user, retrievedUser)
	}

	// Test transactions
	err := stringDB.Txn(ctx, func(tx StringKeyTxn[string]) error {
		tx.Put(ctx, "txn_key1", "txn_value1")
		tx.Put(ctx, "txn_key2", "txn_value2")
		return nil
	})
	if err != nil {
		t.Errorf("Transaction failed: %v", err)
	}

	// Verify transaction results
	value, exists = stringDB.Get(ctx, "txn_key1")
	if !exists || value != "txn_value1" {
		t.Errorf("Expected txn_value1, got %s, exists: %t", value, exists)
	}

	// Test snapshots
	snapshot := stringDB.Snapshot(ctx)
	defer snapshot.Close(ctx)

	// Modify data after snapshot
	stringDB.Put(ctx, "snapshot_test", "new_value")

	// Snapshot shouldn't see new data
	value, exists = snapshot.Get(ctx, "snapshot_test")
	if exists {
		t.Errorf("Snapshot should not see new data, but got %s", value)
	}
}

func TestNewBatch(t *testing.T) {
	// Test NewBatch with []byte keys and string values
	batch := NewBatch[[]byte, string]()
	if batch == nil {
		t.Fatal("NewBatch[[]byte, string]() returned nil")
	}

	// Test batch operations - add operations to batch
	batch.Put([]byte("batch_key1"), "batch_value1")
	batch.Put([]byte("batch_key2"), "batch_value2")
	batch.Put([]byte("batch_key3"), "batch_value3")

	// Verify batch size
	if batch.Size() != 3 {
		t.Errorf("Expected batch size 3, got %d", batch.Size())
	}

	// Test NewBatch with []byte keys and []byte values
	bytesBatch := NewBatch[[]byte, []byte]()
	if bytesBatch == nil {
		t.Fatal("NewBatch[[]byte, []byte]() returned nil")
	}

	bytesBatch.Put([]byte("bytes_key"), []byte("bytes_value"))
	if bytesBatch.Size() != 1 {
		t.Errorf("Expected batch size 1, got %d", bytesBatch.Size())
	}

	// Test batch with int values
	intBatch := NewBatch[[]byte, int]()
	if intBatch == nil {
		t.Fatal("NewBatch[[]byte, int]() returned nil")
	}

	intBatch.Put([]byte("int_key"), 42)
	if intBatch.Size() != 1 {
		t.Errorf("Expected batch size 1, got %d", intBatch.Size())
	}

	// Test batch clear
	intBatch.Clear()
	if intBatch.Size() != 0 {
		t.Errorf("Expected batch size 0 after clear, got %d", intBatch.Size())
	}

	// Test batch with delete operations
	deleteBatch := NewBatch[[]byte, string]()
	deleteBatch.Delete([]byte("delete_key"))
	if deleteBatch.Size() != 1 {
		t.Errorf("Expected batch size 1, got %d", deleteBatch.Size())
	}
}

func TestNewStringBatch(t *testing.T) {
	// Test NewStringBatch with string values
	stringBatch := NewStringBatch[string]()
	if stringBatch == nil {
		t.Fatal("NewStringBatch[string]() returned nil")
	}

	// Test batch operations - add operations to batch
	stringBatch.Put("string_batch_key1", "string_batch_value1")
	stringBatch.Put("string_batch_key2", "string_batch_value2")
	stringBatch.Put("string_batch_key3", "string_batch_value3")

	// Test NewStringBatch with int values
	intStringBatch := NewStringBatch[int]()
	if intStringBatch == nil {
		t.Fatal("NewStringBatch[int]() returned nil")
	}

	intStringBatch.Put("int_string_key1", 100)
	intStringBatch.Put("int_string_key2", 200)
	intStringBatch.Put("int_string_key3", 300)

	// Test NewStringBatch with custom struct
	type Product struct {
		ID    string
		Name  string
		Price float64
	}

	productBatch := NewStringBatch[Product]()
	if productBatch == nil {
		t.Fatal("NewStringBatch[Product]() returned nil")
	}

	product1 := Product{ID: "P1", Name: "Laptop", Price: 999.99}
	product2 := Product{ID: "P2", Name: "Mouse", Price: 29.99}

	productBatch.Put("product:1", product1)
	productBatch.Put("product:2", product2)

	// Test batch with delete operations
	deleteStringBatch := NewStringBatch[string]()
	deleteStringBatch.Delete("delete_string_key")

	// Test batch with TTL operations
	ttlBatch := NewStringBatch[string]()
	ttlBatch.PutWithTTL("ttl_key", "ttl_value", 5*time.Minute)
}

func TestConstructorFunctionsIntegration(t *testing.T) {
	ctx := context.Background()

	// Test integration between NewStringDB and NewStringBatch
	stringDB := NewStringDB[string]()
	defer stringDB.Close(ctx)

	stringBatch := NewStringBatch[string]()

	// Add data to batch
	stringBatch.Put("integration_key1", "integration_value1")
	stringBatch.Put("integration_key2", "integration_value2")

	// Test integration between New[[]byte, string] and NewBatch[[]byte, string]
	byteDB := New[[]byte, string]()
	defer byteDB.Close(ctx)

	byteBatch := NewBatch[[]byte, string]()

	// Add data to batch
	byteBatch.Put([]byte("byte_integration_key1"), "byte_integration_value1")
	byteBatch.Put([]byte("byte_integration_key2"), "byte_integration_value2")

	// Test that all constructors return non-nil values
	if NewStringDB[string]() == nil {
		t.Error("NewStringDB[string]() returned nil")
	}
	if NewStringDB[int]() == nil {
		t.Error("NewStringDB[int]() returned nil")
	}
	if NewBatch[[]byte, string]() == nil {
		t.Error("NewBatch[[]byte, string]() returned nil")
	}
	if NewBatch[[]byte, int]() == nil {
		t.Error("NewBatch[[]byte, int]() returned nil")
	}
	if NewStringBatch[string]() == nil {
		t.Error("NewStringBatch[string]() returned nil")
	}
	if NewStringBatch[int]() == nil {
		t.Error("NewStringBatch[int]() returned nil")
	}
}
