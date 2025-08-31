// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"

	. "github.com/smartystreets/goconvey/convey"
)

// TestFlushDatabase tests the database Flush functionality
func TestFlushDatabase(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When adding data and flushing", func() {
			// Add some data
			database.Put(ctx, []byte("key1"), "value1")
			database.Put(ctx, []byte("key2"), "value2")

			// Flush the database
			err := database.Flush(ctx)

			So(err, ShouldBeNil)

			Convey("Then data should still be accessible after flush", func() {
				// Verify data is still accessible after flush
				val, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				val, exists = database.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")
			})
		})
	})
}

// TestFlushTransaction tests the transaction Flush functionality
func TestFlushTransaction(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When flushing within a transaction", func() {
			// Add initial data
			database.Put(ctx, []byte("key1"), "initial")

			// Start a transaction
			err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				// Modify data within transaction
				tx.Put(ctx, []byte("key1"), "modified")
				tx.Put(ctx, []byte("key2"), "new")

				// Flush the transaction
				if err := tx.Flush(ctx); err != nil {
					return err
				}

				Convey("Then data should be visible within transaction after flush", func() {
					// Verify data is visible within transaction after flush
					val, exists := tx.Get(ctx, []byte("key1"))
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, "modified")

					val, exists = tx.Get(ctx, []byte("key2"))
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, "new")
				})

				return nil
			})

			So(err, ShouldBeNil)

			Convey("Then data should be committed after transaction", func() {
				// Verify data is committed after transaction
				val, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "modified")

				val, exists = database.Get(ctx, []byte("key2"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "new")
			})
		})
	})
}

// TestFlushWithTTL tests Flush with TTL operations
func TestFlushWithTTL(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When flushing with TTL operations", func() {
			// Add data with TTL
			database.PutWithTTL(ctx, []byte("key1"), "value1", 1*time.Second)

			// Flush the database
			err := database.Flush(ctx)
			So(err, ShouldBeNil)

			Convey("Then data should be accessible before TTL expiration", func() {
				// Verify data is still accessible
				val, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")
			})

			Convey("Then data should be expired after TTL expiration", func() {
				// Wait for TTL to expire
				time.Sleep(1100 * time.Millisecond)

				// Flush again after TTL expiration
				err = database.Flush(ctx)
				So(err, ShouldBeNil)

				// Verify data is no longer accessible
				_, exists := database.Get(ctx, []byte("key1"))
				So(exists, ShouldBeFalse)
			})
		})
	})
}

// TestFlushConcurrent tests Flush with concurrent operations
func TestFlushConcurrent(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When performing concurrent operations with flush", func() {
			// Start concurrent operations
			done := make(chan bool, 10)
			for i := 0; i < 10; i++ {
				go func(id int) {
					defer func() { done <- true }()

					key := []byte{byte(id)}
					value := string(key)

					// Perform operations
					database.Put(ctx, key, value)
					database.Get(ctx, key)

					// Flush periodically
					if id%3 == 0 {
						database.Flush(ctx)
					}
				}(i)
			}

			// Wait for all goroutines to complete
			for i := 0; i < 10; i++ {
				<-done
			}

			Convey("Then final flush should succeed", func() {
				// Final flush
				err := database.Flush(ctx)
				So(err, ShouldBeNil)
			})

			Convey("Then all data should be accessible", func() {
				// Verify all data is accessible
				for i := 0; i < 10; i++ {
					key := []byte{byte(i)}
					expectedValue := string(key)

					val, exists := database.Get(ctx, key)
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, expectedValue)
				}
			})
		})
	})
}

// BenchmarkFlush benchmarks the Flush operation
func BenchmarkFlush(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i % 256)}
		value := string(key)
		database.Put(ctx, key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		database.Flush(ctx)
	}
}
