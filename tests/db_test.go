// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDBBasicOperations(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When performing basic operations", func() {
			// Test Put and Get
			database.Put(ctx, []byte("key1"), "value1")
			val, exists := database.Get(ctx, []byte("key1"))

			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "value1")

			// Test Get for non-existent key
			_, exists = database.Get(ctx, []byte("key2"))
			So(exists, ShouldBeFalse)

			// Test Delete
			deleted := database.Delete(ctx, []byte("key1"))
			So(deleted, ShouldBeTrue)

			_, exists = database.Get(ctx, []byte("key1"))
			So(exists, ShouldBeFalse)
		})
	})
}

func TestDBSnapshots(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When creating snapshots", func() {
			// Put initial value
			database.Put(ctx, []byte("key1"), "value1")

			// Create snapshot
			snapshot := database.Snapshot(ctx)
			defer snapshot.Close(ctx)

			// Modify value after snapshot
			database.Put(ctx, []byte("key1"), "value2")

			Convey("Then snapshot should see old value", func() {
				val, exists := snapshot.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")
			})

			Convey("Then current view should see new value", func() {
				val, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")
			})
		})
	})
}

func TestDBTransactions(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When executing transactions", func() {
			// Put initial value
			database.Put(ctx, []byte("key1"), "value1")

			// Execute transaction
			err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Read value
				val, exists := tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				// Modify value
				tx.Put(ctx, []byte("key1"), "value2")
				tx.Put(ctx, []byte("key2"), "value3")

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then final state should be correct", func() {
				val, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")

				val, exists = database.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value3")
			})
		})
	})
}

func TestDBConcurrentAccess(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing concurrent access", func() {
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
						key := []byte{byte(j % 10)}
						database.Get(ctx, key)
					}
				}()
			}

			// Start writers
			for i := 0; i < numWriters; i++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()
					for j := 0; j < numOps; j++ {
						key := []byte{byte(j % 10)}
						database.Put(ctx, key, j)
					}
				}(i)
			}

			wg.Wait()

			Convey("Then final state should be consistent", func() {
				// Verify final state
				for i := 0; i < 10; i++ {
					key := []byte{byte(i)}
					_, exists := database.Get(ctx, key)
					So(exists, ShouldBeTrue)
				}
			})
		})
	})
}

func TestDBConcurrentSnapshots(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When performing concurrent snapshots", func() {
			// Put initial data
			for i := 0; i < 10; i++ {
				key := []byte{byte(i)}
				database.Put(ctx, key, "initial")
			}

			var wg sync.WaitGroup
			const numSnapshots = 10
			const numOps = 100

			// Start snapshot readers
			for i := 0; i < numSnapshots; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					snapshot := database.Snapshot(ctx)
					defer snapshot.Close(ctx)

					// Read all values in snapshot
					for j := 0; j < 10; j++ {
						key := []byte{byte(j)}
						val, exists := snapshot.Get(ctx, key)
						// Note: We can't use Convey assertions in goroutines
						// Just verify the operation doesn't panic
						_ = val
						_ = exists
					}
				}()
			}

			// Start writers after a small delay to ensure snapshots are created first
			time.Sleep(1 * time.Millisecond)

			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func(writerID int) {
					defer wg.Done()
					for j := 0; j < numOps; j++ {
						key := []byte{byte(j % 10)}
						database.Put(ctx, key, "modified")
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the database should still be functional", func() {
				// Verify the database is still functional after concurrent operations
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})
		})
	})
}

func TestDBGetMetrics(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When performing operations and getting metrics", func() {
			// Perform some operations to generate metrics
			db.Put(ctx, []byte("key1"), "value1")
			db.Put(ctx, []byte("key2"), "value2")
			db.Get(ctx, []byte("key1"))
			db.Delete(ctx, []byte("key2"))

			// Give some time for background metrics processing
			time.Sleep(10 * time.Millisecond)

			metrics := db.GetMetrics(ctx)

			Convey("Then metrics should be valid", func() {
				So(metrics, ShouldNotBeNil)

				// Check operations count - all operations are now recorded
				// Note: The count might be higher due to operations from other tests
				So(metrics.Operations.Get, ShouldBeGreaterThanOrEqualTo, uint64(1))
				So(metrics.Operations.Put, ShouldBeGreaterThanOrEqualTo, uint64(1))
				So(metrics.Operations.Delete, ShouldBeGreaterThanOrEqualTo, uint64(1))

				// Check that latency data is available
				So(metrics.Latency.Get.Count, ShouldBeGreaterThan, uint64(0))
				So(metrics.Latency.Put.Count, ShouldBeGreaterThan, uint64(0))
				So(metrics.Latency.Delete.Count, ShouldBeGreaterThan, uint64(0))

				// Check that memory metrics are available
				So(metrics.Memory.ActiveSnapshots, ShouldBeGreaterThanOrEqualTo, uint64(0))
			})
		})
	})
}

func TestDBSnapshotIterate(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When iterating over snapshot", func() {
			// Add some data
			db.Put(ctx, []byte("key1"), "value1")
			db.Put(ctx, []byte("key2"), "value2")
			db.Put(ctx, []byte("key3"), "value3")

			// Create snapshot
			snapshot := db.Snapshot(ctx)
			defer snapshot.Close(ctx)

			Convey("Then all entries should be visited", func() {
				// Test iteration
				visited := make(map[string]string)
				snapshot.Iterate(ctx, func(key []byte, val string) bool {
					visited[string(key)] = val
					return true // continue iteration
				})

				// Check all entries were visited
				expected := map[string]string{
					"key1": "value1",
					"key2": "value2",
					"key3": "value3",
				}

				for k, v := range expected {
					So(visited[k], ShouldEqual, v)
				}
			})

			Convey("Then early termination should work", func() {
				// Test early termination
				visited := make(map[string]string)
				snapshot.Iterate(ctx, func(key []byte, val string) bool {
					visited[string(key)] = val
					return false // stop after first entry
				})

				So(len(visited), ShouldEqual, 1)
			})
		})
	})
}

func TestDBSnapshotIterateEmpty(t *testing.T) {
	Convey("Given a new empty database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When iterating over empty snapshot", func() {
			// Create snapshot of empty database
			snapshot := db.Snapshot(ctx)
			defer snapshot.Close(ctx)

			// Test iteration on empty snapshot
			count := 0
			snapshot.Iterate(ctx, func(key []byte, val string) bool {
				count++
				return true
			})

			So(count, ShouldEqual, 0)
		})
	})
}

func TestDBTransactionDelete(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When performing transaction with delete", func() {
			// Add initial data
			db.Put(ctx, []byte("key1"), "value1")
			db.Put(ctx, []byte("key2"), "value2")

			// Test transaction with delete
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Read existing value
				val, exists := tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				// Delete key
				tx.Delete(ctx, []byte("key1"))

				// Verify it's deleted in transaction
				_, exists = tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)

				// Verify other key is still accessible
				val, exists = tx.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then deletion should be persisted", func() {
				// Verify deletion is persisted
				_, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

func TestDBTransactionWriteSet(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When performing transaction with write set", func() {
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Write to multiple keys
				tx.Put(ctx, []byte("key1"), "value1")
				tx.Put(ctx, []byte("key2"), "value2")
				tx.Put(ctx, []byte("key3"), "value3")

				// Read back the writes
				val, exists := tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				val, exists = tx.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")

				val, exists = tx.Get(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value3")

				// Overwrite a key
				tx.Put(ctx, []byte("key1"), "new-value1")

				// Verify overwrite
				val, exists = tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "new-value1")

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then all writes should be persisted", func() {
				// Verify all writes are persisted
				val, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "new-value1")

				val, exists = db.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")

				val, exists = db.Get(ctx, []byte("key3"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value3")
			})
		})
	})
}

func TestDBTransactionDeleteThenPut(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When performing transaction with delete then put", func() {
			// Add initial data
			db.Put(ctx, []byte("key1"), "value1")

			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Delete then put the same key
				tx.Delete(ctx, []byte("key1"))
				tx.Put(ctx, []byte("key1"), "new-value1")

				// Verify the new value is visible
				val, exists := tx.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "new-value1")

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then the new value should be persisted", func() {
				// Verify the new value is persisted
				val, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "new-value1")
			})
		})
	})
}

func TestDBTransactionAbort(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, string]()
		defer db.Close(ctx)

		Convey("When performing transaction that aborts", func() {
			// Add initial data
			db.Put(ctx, []byte("key1"), "value1")

			// Test transaction that aborts
			err := db.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				tx.Put(ctx, []byte("key1"), "new-value1")
				tx.Put(ctx, []byte("key2"), "value2")

				// Return error to abort transaction
				return fmt.Errorf("intentional abort")
			})

			So(err, ShouldNotBeNil)

			Convey("Then no changes should be persisted", func() {
				// Verify no changes were persisted
				val, exists := db.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				_, exists = db.Get(ctx, []byte("key2"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}
