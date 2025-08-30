// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

// TestRaceDetection tests for race conditions using Go's race detector
func TestRaceDetection(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing concurrent reads and writes", func() {
			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOps = 1000

			// Test concurrent reads and writes
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					for j := 0; j < numOps; j++ {
						key := []byte{byte(j % 10)}

						// Mix of operations
						switch j % 3 {
						case 0:
							database.Put(ctx, key, goroutineID*numOps+j)
						case 1:
							database.Get(ctx, key)
						case 2:
							database.Delete(ctx, key)
						}
					}
				}(i)
			}

			wg.Wait()

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

// TestRaceDetectionSnapshots tests for race conditions with snapshots
func TestRaceDetectionSnapshots(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database with pre-populated data", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Pre-populate with data
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			database.Put(ctx, key, "initial")
		}

		Convey("When running concurrent snapshots and writers", func() {
			var wg sync.WaitGroup
			const numSnapshots = 10
			const numWriters = 5
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
						snapshot.Get(ctx, key)
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
						database.Put(ctx, key, "modified")
					}
				}(i)
			}

			wg.Wait()

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

// TestRaceDetectionTransactions tests for race conditions with transactions
func TestRaceDetectionTransactions(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When running concurrent transactions", func() {
			var wg sync.WaitGroup
			const numTransactions = 10
			const numOps = 100

			// Start concurrent transactions
			for i := 0; i < numTransactions; i++ {
				wg.Add(1)
				go func(txnID int) {
					defer wg.Done()
					for j := 0; j < numOps; j++ {
						err := database.Txn(ctx, func(tx core.Txn[[]byte, int]) error {
							key := []byte{byte(j % 10)}

							// Read current value
							current, exists := tx.Get(ctx, key)
							if !exists {
								current = 0
							}

							// Write incremented value
							tx.Put(ctx, key, current+1)
							return nil
						})

						// Don't use Convey assertions inside goroutines
						if err != nil {
							panic(err)
						}
					}
				}(i)
			}

			wg.Wait()

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

// TestGoroutineLeaks tests for goroutine leaks
func TestGoroutineLeaks(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When creating and closing many snapshots and transactions", func() {
			// Create and close many snapshots
			for i := 0; i < 100; i++ {
				snapshot := database.Snapshot(ctx)
				snapshot.Close(ctx)
			}

			// Create and close many transactions
			for i := 0; i < 100; i++ {
				err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
					tx.Put(ctx, []byte("key"), "value")
					return nil
				})
				So(err, ShouldBeNil)
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

// TestStressGoroutineLeaks performs stress testing for goroutine leaks
func TestStressGoroutineLeaks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	defer goleak.VerifyNone(t)

	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When running stress test for a short duration", func() {
			// Run stress test for a short duration
			duration := 2 * time.Second
			deadline := time.Now().Add(duration)

			var wg sync.WaitGroup
			const numGoroutines = 20

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(goroutineID int) {
					defer wg.Done()
					counter := 0
					for time.Now().Before(deadline) {
						key := []byte{byte(goroutineID), byte(counter % 256)}

						// Mix of operations
						switch counter % 4 {
						case 0:
							database.Put(ctx, key, counter)
						case 1:
							database.Get(ctx, key)
						case 2:
							database.Delete(ctx, key)
						case 3:
							// Create and use snapshot
							snapshot := database.Snapshot(ctx)
							snapshot.Get(ctx, key)
							snapshot.Close(ctx)
						}
						counter++
					}
				}(i)
			}

			wg.Wait()

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

// TestConcurrentClose tests for race conditions during database close
func TestConcurrentClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()

		Convey("When closing database while operations are running", func() {
			// Start operations that will be interrupted by close
			var wg sync.WaitGroup
			const numGoroutines = 10

			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 100; j++ {
						key := []byte{byte(j)}
						database.Put(ctx, key, "value")
						database.Get(ctx, key)
					}
				}()
			}

			// Close database while operations are running
			time.Sleep(10 * time.Millisecond)
			database.Close(ctx)

			wg.Wait()
		})
	})
}

// TestSnapshotConcurrentClose tests for race conditions when closing snapshots
func TestSnapshotConcurrentClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given a new database with pre-populated data", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		// Pre-populate with data
		for i := 0; i < 10; i++ {
			key := []byte{byte(i)}
			database.Put(ctx, key, "value")
		}

		Convey("When creating snapshots and closing them concurrently", func() {
			var wg sync.WaitGroup
			const numSnapshots = 20

			// Create snapshots and close them concurrently
			for i := 0; i < numSnapshots; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					snapshot := database.Snapshot(ctx)

					// Use snapshot briefly
					for j := 0; j < 10; j++ {
						key := []byte{byte(j)}
						snapshot.Get(ctx, key)
					}

					snapshot.Close(ctx)
				}()
			}

			wg.Wait()

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
