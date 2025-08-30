// Licensed under the MIT License. See LICENSE file in the project root for details.

package index

import (
	"testing"
)

// Benchmark comparison between original and optimized implementations
func BenchmarkHashIndexComparison(b *testing.B) {
	// Test data
	keys := make([][]byte, 1000)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 8)
		for j := range key {
			key[j] = byte((i + j) % 256)
		}
		keys[i] = key
	}

	b.Run("OriginalHashIndex", func(b *testing.B) {
		index := NewHashIndex[string](1024)

		// Pre-populate
		for i := 0; i < 100; i++ {
			index.GetOrCreate(keys[i])
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := keys[counter%len(keys)]
				index.GetOrCreate(key)
				counter++
			}
		})
	})

	b.Run("OptimizedHashIndex", func(b *testing.B) {
		index := NewOptimizedHashIndex[string](1024)

		// Pre-populate
		for i := 0; i < 100; i++ {
			index.GetOrCreate(keys[i])
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := keys[counter%len(keys)]
				index.GetOrCreate(key)
				counter++
			}
		})
	})
}

func BenchmarkByteComparisonComparison(b *testing.B) {
	// Test data of different sizes
	testCases := []struct {
		name string
		size int
	}{
		{"Small", 8},
		{"Medium", 64},
		{"Large", 1024},
		{"VeryLarge", 8192},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create test data
			data1 := make([]byte, tc.size)
			data2 := make([]byte, tc.size)
			for i := range data1 {
				data1[i] = byte(i % 256)
				data2[i] = byte(i % 256)
			}

			b.Run("Original", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bytesEqual(data1, data2)
				}
			})

			b.Run("Optimized", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bytesEqualOptimized(data1, data2)
				}
			})
		})
	}
}

func BenchmarkHashFunctionComparison(b *testing.B) {
	// Test data of different sizes
	testCases := []struct {
		name string
		size int
	}{
		{"Short", 4},
		{"Medium", 32},
		{"Long", 256},
		{"VeryLong", 2048},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create test data
			data := make([]byte, tc.size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			originalIndex := NewHashIndex[string](1024)
			optimizedIndex := NewOptimizedHashIndex[string](1024)

			b.Run("Original", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					originalIndex.hash(data)
				}
			})

			b.Run("Optimized", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					optimizedIndex.hashOptimized(data)
				}
			})
		})
	}
}

func BenchmarkConcurrentAccessComparison(b *testing.B) {
	const numKeys = 1000
	// const numGoroutines = 8 // Currently unused

	// Test data
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 16)
		for j := range key {
			key[j] = byte((i + j) % 256)
		}
		keys[i] = key
	}

	b.Run("OriginalHashIndex", func(b *testing.B) {
		index := NewHashIndex[string](1024)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := keys[counter%len(keys)]
				index.GetOrCreate(key)
				counter++
			}
		})
	})

	b.Run("OptimizedHashIndex", func(b *testing.B) {
		index := NewOptimizedHashIndex[string](1024)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			counter := 0
			for pb.Next() {
				key := keys[counter%len(keys)]
				index.GetOrCreate(key)
				counter++
			}
		})
	})
}

func BenchmarkMemoryUsageComparison(b *testing.B) {
	const numKeys = 10000

	// Test data
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 32)
		for j := range key {
			key[j] = byte((i + j) % 256)
		}
		keys[i] = key
	}

	b.Run("OriginalHashIndex", func(b *testing.B) {
		index := NewHashIndex[string](1024)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			index.GetOrCreate(key)
		}
	})

	b.Run("OptimizedHashIndex", func(b *testing.B) {
		index := NewOptimizedHashIndex[string](1024)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%len(keys)]
			index.GetOrCreate(key)
		}
	})
}
