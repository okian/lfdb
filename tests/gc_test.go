// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"runtime"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"

	. "github.com/smartystreets/goconvey/convey"
)

// TestEpochReclamation tests epoch reclamation with long-running writers and periodic readers
func TestEpochReclamation(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When running long-running writers and periodic readers", func() {
			// Start long-running writers
			writerDone := make(chan bool)
			go func() {
				defer close(writerDone)
				for i := 0; i < 10000; i++ {
					key := []byte{byte(i % 100)}
					database.Put(ctx, key, "value")
					time.Sleep(time.Microsecond)
				}
			}()

			// Start periodic readers (snapshots)
			readerDone := make(chan bool)
			go func() {
				defer close(readerDone)
				for i := 0; i < 100; i++ {
					snapshot := database.Snapshot(ctx)

					// Read some values
					for j := 0; j < 10; j++ {
						key := []byte{byte(j)}
						snapshot.Get(ctx, key)
					}

					snapshot.Close(ctx)
					time.Sleep(time.Millisecond)
				}
			}()

			// Wait for operations to complete
			<-writerDone
			<-readerDone

			Convey("Then the database should still be functional", func() {
				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})
		})
	})
}

// TestAdversarialSnapshots tests that holding snapshots "too long" doesn't cause issues
func TestAdversarialSnapshots(t *testing.T) {
	Convey("Given a new database with pre-populated data", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Pre-populate with data
		for i := 0; i < 100; i++ {
			key := []byte{byte(i)}
			database.Put(ctx, key, "initial")
		}

		Convey("When holding snapshots for a long time", func() {
			// Create snapshots and hold them for a while
			snapshots := make([]core.Snapshot[[]byte, string], 10)
			for i := 0; i < 10; i++ {
				snapshots[i] = database.Snapshot(ctx)
			}

			// Perform many writes while snapshots are held
			for i := 0; i < 1000; i++ {
				key := []byte{byte(i % 100)}
				database.Put(ctx, key, "modified")
			}

			Convey("Then snapshots should still work correctly", func() {
				// Verify snapshots still work
				for i, snapshot := range snapshots {
					key := []byte{byte(i)}
					val, exists := snapshot.Get(ctx, key)
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, "initial")
				}
			})

			Convey("Then database should still be functional after closing snapshots", func() {
				// Close snapshots
				for _, snapshot := range snapshots {
					snapshot.Close(ctx)
				}

				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})
		})
	})
}

// TestMemoryPressure tests garbage collection under memory pressure
func TestMemoryPressure(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When under memory pressure", func() {
			// Create many snapshots to increase memory pressure
			snapshots := make([]core.Snapshot[[]byte, string], 50)
			for i := 0; i < 50; i++ {
				// Add some data
				key := []byte{byte(i)}
				database.Put(ctx, key, "value")

				// Create snapshot
				snapshots[i] = database.Snapshot(ctx)
			}

			// Force garbage collection
			runtime.GC()

			Convey("Then the database should still be functional", func() {
				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})

			// Clean up snapshots
			for _, snapshot := range snapshots {
				snapshot.Close(ctx)
			}
		})
	})
}

// TestConcurrentSnapshots tests garbage collection with many concurrent snapshots
func TestConcurrentSnapshots(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When creating many concurrent snapshots", func() {
			// Pre-populate with data
			for i := 0; i < 100; i++ {
				key := []byte{byte(i)}
				database.Put(ctx, key, i)
			}

			// Create many snapshots concurrently
			snapshots := make([]core.Snapshot[[]byte, int], 20)
			for i := 0; i < 20; i++ {
				snapshots[i] = database.Snapshot(ctx)
			}

			// Perform operations while snapshots are active
			for i := 0; i < 1000; i++ {
				key := []byte{byte(i % 100)}
				database.Put(ctx, key, i)
			}

			Convey("Then all snapshots should work correctly", func() {
				// Verify all snapshots work
				for i, snapshot := range snapshots {
					key := []byte{byte(i)}
					val, exists := snapshot.Get(ctx, key)
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, i)
				}
			})

			Convey("Then database should still be functional after closing snapshots", func() {
				// Close snapshots
				for _, snapshot := range snapshots {
					snapshot.Close(ctx)
				}

				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, 42)
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 42)
			})
		})
	})
}

// TestSnapshotChurn tests garbage collection with rapid snapshot creation and destruction
func TestSnapshotChurn(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When creating and destroying snapshots rapidly", func() {
			// Pre-populate with data
			for i := 0; i < 100; i++ {
				key := []byte{byte(i)}
				database.Put(ctx, key, "value")
			}

			// Create and destroy snapshots rapidly
			for i := 0; i < 1000; i++ {
				snapshot := database.Snapshot(ctx)

				// Read some values
				for j := 0; j < 10; j++ {
					key := []byte{byte(j)}
					snapshot.Get(ctx, key)
				}

				// Close snapshot immediately
				snapshot.Close(ctx)
			}

			Convey("Then the database should still be functional", func() {
				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})
		})
	})
}

// TestLongRunningSnapshots tests garbage collection with very long-running snapshots
func TestLongRunningSnapshots(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When holding snapshots for a very long time", func() {
			// Pre-populate with data
			for i := 0; i < 100; i++ {
				key := []byte{byte(i)}
				database.Put(ctx, key, "initial")
			}

			// Create snapshots and hold them for a long time
			snapshots := make([]core.Snapshot[[]byte, string], 5)
			for i := 0; i < 5; i++ {
				snapshots[i] = database.Snapshot(ctx)
			}

			// Perform many writes over a long period
			for i := 0; i < 10000; i++ {
				key := []byte{byte(i % 100)}
				database.Put(ctx, key, "modified")
				if i%1000 == 0 {
					time.Sleep(time.Millisecond)
				}
			}

			Convey("Then snapshots should still work correctly", func() {
				// Verify snapshots still work
				for i, snapshot := range snapshots {
					key := []byte{byte(i)}
					val, exists := snapshot.Get(ctx, key)
					So(exists, ShouldBeTrue)
					So(val, ShouldEqual, "initial")
				}
			})

			// Clean up snapshots
			for _, snapshot := range snapshots {
				snapshot.Close(ctx)
			}
		})
	})
}

// TestMixedWorkload tests garbage collection with mixed read/write workload
func TestMixedWorkload(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When running mixed read/write workload", func() {
			// Pre-populate with data
			for i := 0; i < 100; i++ {
				key := []byte{byte(i)}
				database.Put(ctx, key, i)
			}

			// Run mixed workload
			for i := 0; i < 10000; i++ {
				key := []byte{byte(i % 100)}

				switch i % 4 {
				case 0:
					// Write
					database.Put(ctx, key, i)
				case 1:
					// Read
					database.Get(ctx, key)
				case 2:
					// Snapshot read
					snapshot := database.Snapshot(ctx)
					snapshot.Get(ctx, key)
					snapshot.Close(ctx)
				case 3:
					// Delete and rewrite
					database.Delete(ctx, key)
					database.Put(ctx, key, i)
				}
			}

			Convey("Then the database should still be functional", func() {
				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, 42)
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 42)
			})
		})
	})
}

// TestStressGC tests garbage collection under stress conditions
func TestStressGC(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When under stress conditions", func() {
			// Create many snapshots
			snapshots := make([]core.Snapshot[[]byte, string], 100)
			for i := 0; i < 100; i++ {
				// Add data
				key := []byte{byte(i)}
				database.Put(ctx, key, "value")

				// Create snapshot
				snapshots[i] = database.Snapshot(ctx)
			}

			// Perform many operations
			for i := 0; i < 10000; i++ {
				key := []byte{byte(i % 100)}
				database.Put(ctx, key, "modified")
			}

			// Force garbage collection multiple times
			for i := 0; i < 10; i++ {
				runtime.GC()
				time.Sleep(time.Millisecond)
			}

			Convey("Then the database should still be functional", func() {
				// Verify database is still functional
				testKey := []byte("test")
				database.Put(ctx, testKey, "test-value")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "test-value")
			})

			// Clean up snapshots
			for _, snapshot := range snapshots {
				snapshot.Close(ctx)
			}
		})
	})
}
