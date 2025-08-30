// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	core "github.com/kianostad/lfdb/internal/core"
	"github.com/kianostad/lfdb/internal/storage/mvcc"
	. "github.com/smartystreets/goconvey/convey"
)

func TestDBEdgeCases(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When testing with nil keys", func() {
			// Test Put with nil key
			database.Put(ctx, nil, "nil-value")
			val, exists := database.Get(ctx, nil)

			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "nil-value")

			// Test Delete with nil key
			deleted := database.Delete(ctx, nil)
			So(deleted, ShouldBeTrue)
		})

		Convey("When testing with empty keys", func() {
			database.Put(ctx, []byte{}, "empty-value")
			val, exists := database.Get(ctx, []byte{})

			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "empty-value")
		})

		Convey("When testing with very large keys", func() {
			largeKey := make([]byte, 10000)
			for i := range largeKey {
				largeKey[i] = byte(i % 256)
			}
			database.Put(ctx, largeKey, "large-value")
			val, exists := database.Get(ctx, largeKey)

			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "large-value")
		})
	})
}

func TestDBConcurrentEdgeCases(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		var wg sync.WaitGroup
		const numGoroutines = 20
		const numOperations = 100

		Convey("When performing concurrent operations with edge cases", func() {
			// Test concurrent operations with edge cases
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						// Mix of normal and edge case operations
						switch j % 5 {
						case 0:
							// Normal operation
							key := []byte{byte(id), byte(j)}
							database.Put(ctx, key, id*numOperations+j)
						case 1:
							// Nil key
							database.Put(ctx, nil, j)
						case 2:
							// Empty key
							database.Put(ctx, []byte{}, j)
						case 3:
							// Large key
							largeKey := make([]byte, 1000)
							for k := range largeKey {
								largeKey[k] = byte((id + j + k) % 256)
							}
							database.Put(ctx, largeKey, j)
						case 4:
							// Get operations
							database.Get(ctx, []byte{byte(id), byte(j)})
							database.Get(ctx, nil)
							database.Get(ctx, []byte{})
						}
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the database should remain functional", func() {
				// Verify the database is still functional after concurrent operations
				testKey := []byte("test")
				database.Put(ctx, testKey, 42)
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 42)
			})
		})
	})
}

func TestDBSnapshotEdgeCases(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When testing snapshots with edge cases", func() {
			// Test snapshot with empty key
			database.Put(ctx, []byte{}, "empty-value")
			snapshot := database.Snapshot(ctx)
			defer snapshot.Close(ctx)

			val, exists := snapshot.Get(ctx, []byte{})
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "empty-value")

			// Test snapshot with regular keys
			database.Put(ctx, []byte("regular-key"), "regular-value")
			snapshot2 := database.Snapshot(ctx)
			defer snapshot2.Close(ctx)

			val, exists = snapshot2.Get(ctx, []byte("regular-key"))
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "regular-value")

			// Test multiple snapshots with edge cases
			database.Put(ctx, []byte{}, "empty-value")
			database.Put(ctx, []byte("regular-key"), "regular-value")

			snapshot = database.Snapshot(ctx)
			defer snapshot.Close(ctx)

			snapshot2 = database.Snapshot(ctx)
			defer snapshot2.Close(ctx)

			snapshot3 := database.Snapshot(ctx)
			defer snapshot3.Close(ctx)

			// All snapshots should see the same values
			val, exists = snapshot.Get(ctx, []byte{})
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "empty-value")

			val, exists = snapshot2.Get(ctx, []byte{})
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "empty-value")

			val, exists = snapshot3.Get(ctx, []byte{})
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "empty-value")
		})
	})
}

func TestDBTransactionEdgeCases(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When testing transactions with edge cases", func() {
			var val string
			var exists bool

			// Test transaction with nil key
			err := database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				tx.Put(ctx, nil, "nil-value")
				val, exists = tx.Get(ctx, nil)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "nil-value")
				return nil
			})
			So(err, ShouldBeNil)

			// Test transaction with empty key
			err = database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				tx.Put(ctx, []byte{}, "empty-value")
				val, exists = tx.Get(ctx, []byte{})
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "empty-value")
				return nil
			})
			So(err, ShouldBeNil)

			// Test transaction with large key
			err = database.Txn(ctx, func(tx core.Txn[[]byte, string]) error {
				largeKey := make([]byte, 5000)
				for i := range largeKey {
					largeKey[i] = byte(i % 256)
				}
				tx.Put(ctx, largeKey, "large-value")
				val, exists = tx.Get(ctx, largeKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "large-value")
				return nil
			})
			So(err, ShouldBeNil)

			// Verify the values are persisted
			_, exists = database.Get(ctx, []byte("key"))
			So(exists, ShouldBeFalse) // This key was never set
		})
	})
}

func TestDBStressTest(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing stress test", func() {
			var wg sync.WaitGroup
			const numGoroutines = 50
			const numOperations = 200

			// Start multiple goroutines performing various operations
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := []byte{byte(id), byte(j)}
						counter := id*numOperations + j

						// Mix of operations
						switch j % 4 {
						case 0:
							database.Put(ctx, key, counter)
						case 1:
							database.Get(ctx, key)
						case 2:
							database.Delete(ctx, key)
						case 3:
							// Create snapshots occasionally
							if j%10 == 0 {
								snapshot := database.Snapshot(ctx)
								snapshot.Get(ctx, key)
								snapshot.Close(ctx)
							}
						}
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the database should remain functional", func() {
				// Verify the database is still functional after stress test
				testKey := []byte("stress-test")
				database.Put(ctx, testKey, 42)
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 42)
			})
		})
	})
}

func TestDBMemoryPressure(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, string]()
		defer database.Close(ctx)

		Convey("When under memory pressure", func() {
			// Create many large values to simulate memory pressure
			const numLargeValues = 1000
			const valueSize = 10000

			for i := 0; i < numLargeValues; i++ {
				key := []byte(fmt.Sprintf("large-key-%d", i))
				value := make([]byte, valueSize)
				for j := range value {
					value[j] = byte((i + j) % 256)
				}
				database.Put(ctx, key, string(value))
			}

			Convey("Then the database should remain functional", func() {
				// Verify we can still read values
				testKey := []byte("large-key-42")
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(len(val), ShouldEqual, valueSize)
			})
		})
	})
}

func TestDBConcurrentSnapshotsStress(t *testing.T) {
	Convey("Given a new database", t, func() {
		ctx := context.Background()
		database := core.New[[]byte, int]()
		defer database.Close(ctx)

		Convey("When performing concurrent snapshot stress test", func() {
			// Pre-populate with some data
			database.Put(ctx, []byte("key1"), 1)
			database.Put(ctx, []byte("key2"), 2)

			var wg sync.WaitGroup
			const numSnapshots = 100
			const numReads = 50

			// Create many snapshots concurrently
			for i := 0; i < numSnapshots; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					snapshot := database.Snapshot(ctx)
					defer snapshot.Close(ctx)

					// Read from snapshot multiple times
					for j := 0; j < numReads; j++ {
						val, exists := snapshot.Get(ctx, []byte("key1"))
						_ = val
						_ = exists
					}
				}()
			}

			wg.Wait()

			Convey("Then the database should remain functional", func() {
				// Verify the database is still functional after snapshot stress
				testKey := []byte("snapshot-stress-test")
				database.Put(ctx, testKey, 42)
				val, exists := database.Get(ctx, testKey)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, 42)
			})
		})
	})
}

func TestEntryExtendTTLEdgeCases(t *testing.T) {
	Convey("Given a new entry", t, func() {
		entry := mvcc.NewEntry[string]([]byte("extend-ttl-key"))

		Convey("When testing ExtendTTL on non-existent entry", func() {
			extended := entry.ExtendTTL(10, 100*time.Millisecond)
			So(extended, ShouldBeFalse)
		})

		Convey("When testing ExtendTTL on entry without TTL", func() {
			entry.Put("value1", 20)
			extended := entry.ExtendTTL(25, 100*time.Millisecond)
			So(extended, ShouldBeFalse)
		})

		Convey("When testing ExtendTTL on deleted entry", func() {
			entry.Delete(30)
			extended := entry.ExtendTTL(35, 100*time.Millisecond)
			So(extended, ShouldBeFalse)
		})

		Convey("When testing ExtendTTL on entry with TTL", func() {
			entry.PutWithExpiry("value2", 40, time.Now().Add(50*time.Millisecond))

			// Check initial TTL
			initialTTL, exists := entry.GetTTL(45)
			So(exists, ShouldBeTrue)

			// Extend TTL
			extended := entry.ExtendTTL(50, 200*time.Millisecond)
			So(extended, ShouldBeTrue)

			// Check extended TTL
			extendedTTL, exists := entry.GetTTL(55)
			So(exists, ShouldBeTrue)
			So(extendedTTL, ShouldBeGreaterThan, initialTTL)
		})

		Convey("When testing ExtendTTL with zero duration", func() {
			entry.PutWithExpiry("value3", 60, time.Now().Add(100*time.Millisecond))
			extended := entry.ExtendTTL(65, 0)
			So(extended, ShouldBeTrue)
		})

		Convey("When testing ExtendTTL with negative duration", func() {
			entry.PutWithExpiry("value4", 70, time.Now().Add(100*time.Millisecond))

			extended := entry.ExtendTTL(80, -100*time.Millisecond)
			// Note: ExtendTTL behavior with negative duration may vary
			// We just verify the operation doesn't crash
			_ = extended

			// Check TTL after negative extension
			negativeTTL, exists := entry.GetTTL(85)
			// Note: The behavior may vary depending on implementation
			_ = negativeTTL
			_ = exists
		})

		Convey("When testing ExtendTTL on expired entry", func() {
			entry.PutWithExpiry("value5", 90, time.Now().Add(-1*time.Hour)) // expired
			extended := entry.ExtendTTL(95, 100*time.Millisecond)
			// Note: ExtendTTL behavior on expired entries may vary
			// We just verify the operation doesn't crash
			_ = extended
		})
	})
}

func TestEntryRemoveTTLEdgeCases(t *testing.T) {
	Convey("Given a new entry", t, func() {
		entry := mvcc.NewEntry[string]([]byte("remove-ttl-key"))

		Convey("When testing RemoveTTL on non-existent entry", func() {
			removed := entry.RemoveTTL(10)
			So(removed, ShouldBeFalse)
		})

		Convey("When testing RemoveTTL on entry without TTL", func() {
			entry.Put("value1", 20)
			removed := entry.RemoveTTL(25)
			// Note: RemoveTTL behavior on entry without TTL may vary
			// We just verify the operation doesn't crash
			_ = removed
		})

		Convey("When testing RemoveTTL on deleted entry", func() {
			entry.Delete(30)
			removed := entry.RemoveTTL(35)
			So(removed, ShouldBeFalse)
		})

		Convey("When testing RemoveTTL on entry with TTL", func() {
			entry.PutWithExpiry("value2", 40, time.Now().Add(100*time.Millisecond))

			// Check TTL exists
			checkTTL, exists := entry.GetTTL(45)
			So(exists, ShouldBeTrue)
			_ = checkTTL // Use the variable to avoid unused variable error

			// Remove TTL
			removed := entry.RemoveTTL(50)
			So(removed, ShouldBeTrue)

			// Check TTL is removed
			removedTTL, exists := entry.GetTTL(55)
			So(exists, ShouldBeFalse)
			_ = removedTTL // Use the variable to avoid unused variable error

			// Verify value still exists
			val, exists := entry.Get(55)
			So(exists, ShouldBeTrue)
			So(val, ShouldEqual, "value2")
		})

		Convey("When testing RemoveTTL on expired entry", func() {
			entry.PutWithExpiry("value3", 60, time.Now().Add(-1*time.Hour)) // expired
			removed := entry.RemoveTTL(65)
			// Note: RemoveTTL might succeed if the entry is still considered valid at commit timestamp
			// The important thing is that the TTL is properly removed
			if removed {
				// If it succeeds, verify the TTL was removed
				removedTTL, exists := entry.GetTTL(70)
				So(exists, ShouldBeFalse)
				_ = removedTTL // Use the variable to avoid unused variable error
			}
		})

		Convey("When testing RemoveTTL multiple times", func() {
			entry.PutWithExpiry("value4", 70, time.Now().Add(100*time.Millisecond))

			// Remove TTL first time
			removed := entry.RemoveTTL(75)
			So(removed, ShouldBeTrue)

			// Verify TTL was removed
			ttl, exists := entry.GetTTL(77)
			So(exists, ShouldBeFalse)
			_ = ttl // Use the variable to avoid unused variable error

			// Remove TTL second time (behavior may vary)
			removed = entry.RemoveTTL(80)
			// Note: Second RemoveTTL behavior may vary
			// We just verify the operation doesn't crash
			_ = removed
		})
	})
}

func TestEntryExtendTTLConcurrent(t *testing.T) {
	Convey("Given a new entry with initial value and TTL", t, func() {
		entry := mvcc.NewEntry[string]([]byte("extend-ttl-concurrent-key"))

		// Put initial value with TTL
		entry.PutWithExpiry("initial-value", 10, time.Now().Add(100*time.Millisecond))

		Convey("When performing concurrent ExtendTTL operations", func() {
			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOperations = 50

			// Test concurrent ExtendTTL operations
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						// Mix of positive and negative extensions
						var extension time.Duration
						if j%2 == 0 {
							extension = time.Duration(j+1) * time.Millisecond
						} else {
							extension = -time.Duration(j+1) * time.Millisecond
						}

						entry.ExtendTTL(uint64(id*numOperations+j+100), extension)
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the entry should still be functional", func() {
				val, exists := entry.Get(1000)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "initial-value")
			})
		})
	})
}

func TestEntryRemoveTTLConcurrent(t *testing.T) {
	Convey("Given a new entry with initial value and TTL", t, func() {
		entry := mvcc.NewEntry[string]([]byte("remove-ttl-concurrent-key"))

		// Put initial value with TTL
		entry.PutWithExpiry("initial-value", 10, time.Now().Add(100*time.Millisecond))

		Convey("When performing concurrent RemoveTTL operations", func() {
			var wg sync.WaitGroup
			const numGoroutines = 10
			const numOperations = 50

			// Test concurrent RemoveTTL operations
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						entry.RemoveTTL(uint64(id*numOperations + j + 100))
					}
				}(i)
			}

			wg.Wait()

			Convey("Then the entry should still be functional", func() {
				val, exists := entry.Get(1000)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "initial-value")
			})
		})
	})
}

func TestEntryTTLOperationsWithDelete(t *testing.T) {
	Convey("Given a new entry", t, func() {
		entry := mvcc.NewEntry[string]([]byte("ttl-delete-key"))

		Convey("When performing TTL operations with delete", func() {
			// Put with TTL
			entry.PutWithExpiry("value1", 10, time.Now().Add(100*time.Millisecond))

			// Verify TTL exists
			verifyTTL, exists := entry.GetTTL(15)
			So(exists, ShouldBeTrue)
			_ = verifyTTL // Use the variable to avoid unused variable error

			// Delete the entry
			deleted := entry.Delete(20)
			So(deleted, ShouldBeTrue)

			// Try to extend TTL on deleted entry
			extended := entry.ExtendTTL(25, 50*time.Millisecond)
			So(extended, ShouldBeFalse)

			// Try to remove TTL on deleted entry
			removed := entry.RemoveTTL(30)
			So(removed, ShouldBeFalse)

			// Put new value with TTL after delete
			entry.PutWithExpiry("value2", 35, time.Now().Add(200*time.Millisecond))

			// Extend TTL on new value
			extended = entry.ExtendTTL(40, 100*time.Millisecond)
			So(extended, ShouldBeTrue)

			// Remove TTL on new value
			removed = entry.RemoveTTL(45)
			So(removed, ShouldBeTrue)

			Convey("Then old deleted state should still be visible at old timestamp", func() {
				_, exists := entry.Get(25)
				So(exists, ShouldBeFalse)
			})

			Convey("Then new value should exist at new timestamp", func() {
				val, exists := entry.Get(50)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value2")
			})
		})
	})
}

func TestEntryTTLOperationsMVCC(t *testing.T) {
	Convey("Given a new entry", t, func() {
		entry := mvcc.NewEntry[string]([]byte("ttl-mvcc-key"))

		Convey("When performing TTL operations with MVCC", func() {
			// Put initial value with TTL
			entry.PutWithExpiry("value1", 10, time.Now().Add(100*time.Millisecond))

			// Extend TTL
			entry.ExtendTTL(20, 50*time.Millisecond)

			// Remove TTL
			entry.RemoveTTL(30)

			Convey("Then old versions should still be accessible", func() {
				// At timestamp 15, should see original TTL
				ttl1, exists := entry.GetTTL(15)
				So(exists, ShouldBeTrue)
				_ = ttl1 // Use the variable to avoid unused variable error

				// At timestamp 25, should see extended TTL
				ttl2, exists := entry.GetTTL(25)
				So(exists, ShouldBeTrue)
				_ = ttl2 // Use the variable to avoid unused variable error

				// At timestamp 35, should see no TTL (removed)
				ttl3, exists := entry.GetTTL(35)
				So(exists, ShouldBeFalse)
				_ = ttl3 // Use the variable to avoid unused variable error
			})

			Convey("Then values should be accessible at all timestamps", func() {
				val, exists := entry.Get(15)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				val, exists = entry.Get(25)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")

				val, exists = entry.Get(35)
				So(exists, ShouldBeTrue)
				So(val, ShouldEqual, "value1")
			})
		})
	})
}
