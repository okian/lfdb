// Licensed under the MIT License. See LICENSE file in the project root for details.

package tests

import (
	"context"
	core "github.com/kianostad/lfdb/internal/core"
	"testing"
)

// BenchmarkGet benchmarks Get operations
func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	key := []byte("benchmark-key")
	database.Put(ctx, key, "benchmark-value")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			database.Get(ctx, key)
		}
	})
}

// BenchmarkPut benchmarks Put operations
func BenchmarkPut(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	value := "benchmark-value"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Use different keys to avoid conflicts
			testKey := []byte{byte(counter % 256)}
			database.Put(ctx, testKey, value)
			counter++
		}
	})
}

// BenchmarkDelete benchmarks Delete operations
func BenchmarkDelete(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Put a value first, then delete it
			testKey := []byte{byte(counter % 256)}
			database.Put(ctx, testKey, "value")
			database.Delete(ctx, testKey)
			counter++
		}
	})
}

// BenchmarkMixedWorkload benchmarks mixed Get/Put/Delete operations
func BenchmarkMixedWorkload(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, "initial-value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter % 100)}

			switch counter % 3 {
			case 0:
				database.Get(ctx, key)
			case 1:
				database.Put(ctx, key, "new-value")
			case 2:
				database.Delete(ctx, key)
			}
			counter++
		}
	})
}

// BenchmarkSnapshot benchmarks snapshot creation and usage
func BenchmarkSnapshot(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			snapshot := database.Snapshot(ctx)

			// Read some values from snapshot
			for i := 0; i < 10; i++ {
				key := []byte{byte(i)}
				snapshot.Get(ctx, key)
			}

			snapshot.Close(ctx)
		}
	})
}

// BenchmarkTransaction benchmarks transaction operations
func BenchmarkTransaction(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, int]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			err := database.Txn(ctx, func(tx core.Txn[[]byte, int]) error {
				// Read and write some values
				for i := 0; i < 5; i++ {
					key := []byte{byte(i)}
					val, exists := tx.Get(ctx, key)
					if exists {
						tx.Put(ctx, key, val+1)
					}
				}
				return nil
			})

			if err != nil {
				b.Fatalf("Transaction failed: %v", err)
			}
			counter++
		}
	})
}

// BenchmarkConcurrentWriters benchmarks concurrent writers
func BenchmarkConcurrentWriters(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter % 1000)}
			database.Put(ctx, key, "value")
			counter++
		}
	})
}

// BenchmarkConcurrentReaders benchmarks concurrent readers
func BenchmarkConcurrentReaders(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 1000; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, "value")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter % 1000)}
			database.Get(ctx, key)
			counter++
		}
	})
}

// BenchmarkHotKey benchmarks operations on a single hot key
func BenchmarkHotKey(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	hotKey := []byte("hot-key")
	database.Put(ctx, hotKey, "initial")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			switch counter % 3 {
			case 0:
				database.Get(ctx, hotKey)
			case 1:
				database.Put(ctx, hotKey, "value")
			case 2:
				database.Delete(ctx, hotKey)
			}
			counter++
		}
	})
}

// BenchmarkUniformKeys benchmarks operations on uniformly distributed keys
func BenchmarkUniformKeys(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			key := []byte{byte(counter % 256)}

			switch counter % 3 {
			case 0:
				database.Get(ctx, key)
			case 1:
				database.Put(ctx, key, "value")
			case 2:
				database.Delete(ctx, key)
			}
			counter++
		}
	})
}

// BenchmarkMemoryUsage benchmarks memory usage under load
func BenchmarkMemoryUsage(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Create unique keys to test memory growth
			key := make([]byte, 8)
			key[0] = byte(counter >> 56)
			key[1] = byte(counter >> 48)
			key[2] = byte(counter >> 40)
			key[3] = byte(counter >> 32)
			key[4] = byte(counter >> 24)
			key[5] = byte(counter >> 16)
			key[6] = byte(counter >> 8)
			key[7] = byte(counter)

			database.Put(ctx, key, "value")
			counter++
		}
	})
}

// BenchmarkSnapshotIsolation benchmarks snapshot isolation performance
func BenchmarkSnapshotIsolation(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, string]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, "initial")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			// Create snapshot
			snapshot := database.Snapshot(ctx)

			// Modify data outside snapshot
			key := []byte{byte(counter % 100)}
			database.Put(ctx, key, "modified")

			// Read from snapshot (should see old value)
			snapshot.Get(ctx, key)

			snapshot.Close(ctx)
			counter++
		}
	})
}

// BenchmarkTransactionIsolation benchmarks transaction isolation performance
func BenchmarkTransactionIsolation(b *testing.B) {
	ctx := context.Background()
	database := core.New[[]byte, int]()
	defer database.Close(ctx)

	// Pre-populate with data
	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		database.Put(ctx, key, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			err := database.Txn(ctx, func(tx core.Txn[[]byte, int]) error {
				// Read multiple values
				for i := 0; i < 10; i++ {
					key := []byte{byte(i)}
					tx.Get(ctx, key)
				}

				// Write some values
				for i := 0; i < 5; i++ {
					key := []byte{byte(i)}
					tx.Put(ctx, key, counter)
				}

				return nil
			})

			if err != nil {
				b.Fatalf("Transaction failed: %v", err)
			}
			counter++
		}
	})
}
