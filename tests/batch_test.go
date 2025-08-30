// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBatchOperations(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, []byte]()
		defer db.Close(ctx)

		Convey("When performing batch put operations", func() {
			keys := [][]byte{
				[]byte("key1"),
				[]byte("key2"),
				[]byte("key3"),
			}
			values := [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			}

			err := db.BatchPut(ctx, keys, values)

			So(err, ShouldBeNil)

			Convey("Then all values should be stored", func() {
				// Verify all values were stored
				for i, key := range keys {
					value, found := db.Get(ctx, key)
					So(found, ShouldBeTrue)
					So(string(value), ShouldEqual, string(values[i]))
				}
			})
		})

		Convey("When performing batch get operations", func() {
			// First put some values
			keys := [][]byte{
				[]byte("batchkey1"),
				[]byte("batchkey2"),
				[]byte("batchkey3"),
			}
			values := [][]byte{
				[]byte("batchvalue1"),
				[]byte("batchvalue2"),
				[]byte("batchvalue3"),
			}

			for i, key := range keys {
				db.Put(ctx, key, values[i])
			}

			Convey("Then batch get should return all values", func() {
				// Test batch get
				results := db.BatchGet(ctx, keys)

				So(len(results), ShouldEqual, len(keys))

				for i, result := range results {
					So(result.Found, ShouldBeTrue)
					So(string(result.Value), ShouldEqual, string(values[i]))
				}
			})
		})

		Convey("When performing batch delete operations", func() {
			keys := [][]byte{
				[]byte("deletekey1"),
				[]byte("deletekey2"),
				[]byte("deletekey3"),
			}

			// First put some values
			for _, key := range keys {
				db.Put(ctx, key, []byte("value"))
			}

			Convey("Then batch delete should remove all keys", func() {
				// Verify they exist initially
				for _, key := range keys {
					_, found := db.Get(ctx, key)
					So(found, ShouldBeTrue)
				}

				// Delete them
				err := db.BatchDelete(ctx, keys)
				So(err, ShouldBeNil)

				// Verify they're gone
				for _, key := range keys {
					_, found := db.Get(ctx, key)
					So(found, ShouldBeFalse)
				}
			})
		})

		Convey("When performing batch put with TTL", func() {
			keys := [][]byte{
				[]byte("ttlkey1"),
				[]byte("ttlkey2"),
			}
			values := [][]byte{
				[]byte("ttlvalue1"),
				[]byte("ttlvalue2"),
			}
			ttls := []time.Duration{
				100 * time.Millisecond,
				200 * time.Millisecond,
			}

			err := db.BatchPutWithTTL(ctx, keys, values, ttls)
			So(err, ShouldBeNil)

			Convey("Then values should exist initially", func() {
				// Verify values exist initially
				for i, key := range keys {
					value, found := db.Get(ctx, key)
					So(found, ShouldBeTrue)
					So(string(value), ShouldEqual, string(values[i]))
				}
			})

			Convey("Then values should expire according to TTL", func() {
				// Wait for first TTL to expire
				time.Sleep(150 * time.Millisecond)

				// Check first key should be expired
				_, found := db.Get(ctx, keys[0])
				So(found, ShouldBeFalse)

				// Second key should still exist
				_, found = db.Get(ctx, keys[1])
				So(found, ShouldBeTrue)

				// Wait for second TTL to expire
				time.Sleep(100 * time.Millisecond)

				// Check second key should be expired
				_, found = db.Get(ctx, keys[1])
				So(found, ShouldBeFalse)
			})
		})
	})
}

func TestBatchObject(t *testing.T) {
	Convey("Given a new batch", t, func() {
		Convey("When creating a new batch", func() {
			batch := core.NewBatch[[]byte, []byte]()

			So(batch, ShouldNotBeNil)
			So(batch.Size(), ShouldEqual, 0)
			So(batch.IsCommitted(), ShouldBeFalse)
		})

		Convey("When performing batch operations", func() {
			batch := core.NewBatch[[]byte, []byte]()

			// Add operations
			batch.Put([]byte("key1"), []byte("value1"))
			batch.PutWithTTL([]byte("key2"), []byte("value2"), 1*time.Second)
			batch.Delete([]byte("key3"))

			So(batch.Size(), ShouldEqual, 3)

			Convey("Then clear should reset the batch", func() {
				// Test clear
				batch.Clear()
				So(batch.Size(), ShouldEqual, 0)
				So(batch.IsCommitted(), ShouldBeFalse)
			})
		})
	})
}

func TestExecuteBatch(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, []byte]()
		defer db.Close(ctx)

		Convey("When executing a batch", func() {
			batch := core.NewBatch[[]byte, []byte]()

			// Add operations
			batch.Put([]byte("batchkey1"), []byte("batchvalue1"))
			batch.Put([]byte("batchkey2"), []byte("batchvalue2"))
			batch.Delete([]byte("batchkey3"))

			// Execute batch
			err := db.ExecuteBatch(ctx, batch)
			So(err, ShouldBeNil)

			So(batch.IsCommitted(), ShouldBeTrue)

			Convey("Then batch results should be correct", func() {
				// Verify results
				results := batch.GetResults()
				So(len(results), ShouldEqual, 3)

				// Check that puts succeeded
				So(results[0].Success, ShouldBeTrue)
				So(results[1].Success, ShouldBeTrue)
			})

			Convey("Then values should be in database", func() {
				// Verify values in database
				value, found := db.Get(ctx, []byte("batchkey1"))
				So(found, ShouldBeTrue)
				So(string(value), ShouldEqual, "batchvalue1")

				value, found = db.Get(ctx, []byte("batchkey2"))
				So(found, ShouldBeTrue)
				So(string(value), ShouldEqual, "batchvalue2")
			})
		})

		Convey("When executing a batch twice", func() {
			batch := core.NewBatch[[]byte, []byte]()
			batch.Put([]byte("key"), []byte("value"))

			// Execute first time
			err := db.ExecuteBatch(ctx, batch)
			So(err, ShouldBeNil)

			// Try to execute again
			err = db.ExecuteBatch(ctx, batch)
			So(err, ShouldNotBeNil)
		})
	})
}

func TestBatchGetWithSnapshot(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		db := core.New[[]byte, []byte]()
		defer db.Close(ctx)

		Convey("When performing batch get with snapshot", func() {
			// Put some initial values
			keys := [][]byte{
				[]byte("snapshotkey1"),
				[]byte("snapshotkey2"),
				[]byte("snapshotkey3"),
			}
			values := [][]byte{
				[]byte("snapshotvalue1"),
				[]byte("snapshotvalue2"),
				[]byte("snapshotvalue3"),
			}

			for i, key := range keys {
				db.Put(ctx, key, values[i])
			}

			// Create snapshot
			snapshot := db.Snapshot(ctx)
			defer snapshot.Close(ctx)

			// Modify values after snapshot
			db.Put(ctx, keys[0], []byte("modifiedvalue1"))
			db.Delete(ctx, keys[1])

			Convey("Then batch get should return snapshot values", func() {
				// Test batch get with snapshot
				results := db.BatchGetWithSnapshot(ctx, keys, snapshot)

				So(len(results), ShouldEqual, len(keys))

				// Should see original values from snapshot
				So(results[0].Found, ShouldBeTrue)
				So(string(results[0].Value), ShouldEqual, "snapshotvalue1")

				So(results[1].Found, ShouldBeTrue)
				So(string(results[1].Value), ShouldEqual, "snapshotvalue2")

				So(results[2].Found, ShouldBeTrue)
				So(string(results[2].Value), ShouldEqual, "snapshotvalue3")
			})
		})
	})
}
