// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"testing"

	"pgregory.net/rapid"
)

// operation represents a database operation
type operation struct {
	Op   string `json:"op"`
	Key  []byte `json:"key"`
	Val  string `json:"val"`
	Time uint64 `json:"time"`
}

// model represents the reference implementation (simple map)
type model struct {
	data map[string]string
}

func newModel() *model {
	return &model{
		data: make(map[string]string),
	}
}

func (m *model) put(key []byte, val string, time uint64) {
	m.data[string(key)] = val
}

func (m *model) get(key []byte) (string, bool) {
	val, exists := m.data[string(key)]
	return val, exists
}

func (m *model) delete(key []byte) bool {
	_, exists := m.data[string(key)]
	if exists {
		delete(m.data, string(key))
	}
	return exists
}

// TestPropertyBasedSequentialOperations tests that the database behaves like a simple map
// for sequential operations using property-based testing
func TestPropertyBasedSequentialOperations(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Generate a sequence of operations
		ops := rapid.SliceOf(rapid.Custom(func(t *rapid.T) operation {
			op := rapid.OneOf(
				rapid.Just("put"),
				rapid.Just("get"),
				rapid.Just("delete"),
			).Draw(t, "op")

			key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
			val := rapid.String().Draw(t, "val")
			time := rapid.Uint64().Draw(t, "time")

			return operation{Op: op, Key: key, Val: val, Time: time}
		})).Draw(t, "operations")

		// Create database and model
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)
		model := newModel()

		// Apply operations to both
		for _, op := range ops {
			switch op.Op {
			case "put":
				database.Put(ctx, op.Key, op.Val)
				model.put(op.Key, op.Val, op.Time)
			case "get":
				dbVal, dbExists := database.Get(ctx, op.Key)
				modelVal, modelExists := model.get(op.Key)

				if dbExists != modelExists {
					t.Fatalf("Get existence mismatch for key %v: db=%v, model=%v", op.Key, dbExists, modelExists)
				}
				if dbExists && dbVal != modelVal {
					t.Fatalf("Get value mismatch for key %v: db=%v, model=%v", op.Key, dbVal, modelVal)
				}
			case "delete":
				dbDeleted := database.Delete(ctx, op.Key)
				modelDeleted := model.delete(op.Key)

				if dbDeleted != modelDeleted {
					t.Fatalf("Delete result mismatch for key %v: db=%v, model=%v", op.Key, dbDeleted, modelDeleted)
				}
			}
		}
	})
}

// TestPropertyBasedInvariants tests various invariants that should always hold
func TestPropertyBasedInvariants(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Generate random operations
		numOps := rapid.IntRange(10, 100).Draw(t, "numOps")

		for i := 0; i < numOps; i++ {
			op := rapid.OneOf(
				rapid.Just("put"),
				rapid.Just("get"),
				rapid.Just("delete"),
			).Draw(t, "op")

			key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
			val := rapid.String().Draw(t, "val")

			switch op {
			case "put":
				database.Put(ctx, key, val)
				// Invariant: After Put, Get should return the value
				retrieved, exists := database.Get(ctx, key)
				if !exists {
					t.Fatalf("Put followed by Get should return value for key %v", key)
				}
				if retrieved != val {
					t.Fatalf("Put followed by Get should return same value for key %v: put=%v, get=%v", key, val, retrieved)
				}
			case "get":
				database.Get(ctx, key)
				// Invariant: Get should not panic
			case "delete":
				database.Delete(ctx, key)
				// Invariant: After Delete, Get should return false
				_, exists := database.Get(ctx, key)
				if exists {
					t.Fatalf("Delete followed by Get should return false for key %v", key)
				}
			}
		}
	})
}

// TestPropertyBasedSnapshotIsolation tests snapshot isolation properties
func TestPropertyBasedSnapshotIsolation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Put initial value
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		initialVal := rapid.String().Draw(t, "initialVal")
		database.Put(ctx, key, initialVal)

		// Create snapshot
		snapshot := database.Snapshot(ctx)
		defer snapshot.Close(ctx)

		// Modify value after snapshot
		newVal := rapid.String().Draw(t, "newVal")
		database.Put(ctx, key, newVal)

		// Snapshot should see old value
		snapshotVal, snapshotExists := snapshot.Get(ctx, key)
		if !snapshotExists {
			t.Fatalf("Snapshot should see value for key %v", key)
		}
		if snapshotVal != initialVal {
			t.Fatalf("Snapshot should see old value for key %v: expected=%v, got=%v", key, initialVal, snapshotVal)
		}

		// Current view should see new value
		currentVal, currentExists := database.Get(ctx, key)
		if !currentExists {
			t.Fatalf("Current view should see value for key %v", key)
		}
		if currentVal != newVal {
			t.Fatalf("Current view should see new value for key %v: expected=%v, got=%v", key, newVal, currentVal)
		}
	})
}

// TestPropertyBasedTransactionReadYourWrites tests that transactions can read their own writes
func TestPropertyBasedTransactionReadYourWrites(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Generate random key-value pairs
		numWrites := rapid.IntRange(1, 5).Draw(t, "numWrites")
		writes := make(map[string]string)

		for i := 0; i < numWrites; i++ {
			key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
			val := rapid.String().Draw(t, "val")
			writes[string(key)] = val
		}

		// Execute transaction
		err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
			// Write all values
			for key, val := range writes {
				tx.Put(ctx, []byte(key), val)
			}

			// Read all values back
			for key, expectedVal := range writes {
				val, exists := tx.Get(ctx, []byte(key))
				if !exists {
					t.Fatalf("Transaction should be able to read its own write for key %v", key)
				}
				if val != expectedVal {
					t.Fatalf("Transaction read-your-writes mismatch for key %v: expected=%v, got=%v", key, expectedVal, val)
				}
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})
}
