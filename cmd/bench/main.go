// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package main provides comprehensive benchmarking tools for the lock-free database.
//
// This command-line tool performs various performance benchmarks to evaluate
// database performance under different workloads and conditions. It's useful
// for performance testing, capacity planning, and comparing different configurations.
//
// # Benchmark Categories
//
// The benchmark suite includes:
//   - Single-threaded operations (baseline performance)
//   - Concurrent reads (scalability testing)
//   - Concurrent writes (write contention testing)
//   - Mixed workloads (real-world simulation)
//   - Snapshot performance (consistency overhead)
//   - Hot key performance (cache efficiency)
//   - Iterator performance (bulk operations)
//   - Memory usage analysis (resource consumption)
//
// # Usage
//
// Run all benchmarks:
//
//	go run cmd/bench/main.go
//
// Build and run:
//
//	go build -o bench cmd/bench/main.go
//	./bench
//
// # Benchmark Details
//
// ## Single-threaded Operations
// Measures baseline performance for individual Get and Put operations.
// Provides a reference point for other benchmarks.
//
// ## Concurrent Reads
// Tests read scalability by varying the number of concurrent readers.
// Helps identify read amplification and contention issues.
//
// ## Concurrent Writes
// Tests write performance under contention by varying the number of writers.
// Measures CAS retry rates and write throughput.
//
// ## Mixed Workload
// Simulates realistic workloads with varying read/write ratios.
// Tests the database under conditions similar to production use.
//
// ## Snapshot Performance
// Measures the overhead of creating and using snapshots.
// Important for applications requiring consistent reads.
//
// ## Hot Key Performance
// Tests performance when multiple operations target the same key.
// Identifies contention issues in high-traffic scenarios.
//
// ## Iterator Performance
// Measures bulk iteration performance for data export and analysis.
// Important for batch processing applications.
//
// ## Memory Usage
// Analyzes memory consumption patterns and garbage collection efficiency.
// Helps with capacity planning and resource allocation.
//
// # Dangers and Warnings
//
//   - **Resource Consumption**: Benchmarks can consume significant CPU and memory resources.
//   - **System Impact**: High-intensity benchmarks may impact other system processes.
//   - **Data Loss**: Benchmarks use in-memory databases - all data is lost after completion.
//   - **Long Running**: Some benchmarks may take several minutes to complete.
//   - **System Requirements**: Requires sufficient RAM for large dataset benchmarks.
//   - **CPU Affinity**: Results may vary based on CPU architecture and core count.
//   - **Garbage Collection**: Go's GC may impact benchmark results unpredictably.
//
// # Best Practices
//
//   - Run benchmarks on dedicated systems to avoid interference
//   - Use consistent hardware and software configurations for comparisons
//   - Run multiple iterations to account for variance
//   - Monitor system resources during benchmark execution
//   - Use appropriate dataset sizes for your use case
//   - Consider the impact of garbage collection on results
//   - Document benchmark conditions for reproducibility
//
// # Performance Considerations
//
//   - Benchmark results are system-dependent and may vary significantly
//   - CPU architecture, memory speed, and core count affect performance
//   - Go runtime version and GC settings impact results
//   - Operating system scheduling can affect concurrent benchmark results
//   - Memory pressure from other processes can skew results
//
// # Interpreting Results
//
// Key metrics to consider:
//   - **Throughput**: Operations per second (higher is better)
//   - **Latency**: Time per operation (lower is better)
//   - **Scalability**: Performance improvement with more cores
//   - **Contention**: Performance degradation under high concurrency
//   - **Memory Usage**: Peak and steady-state memory consumption
//
// # Thread Safety
//
// Benchmarks are designed to test thread safety and concurrent access patterns.
// Results help identify potential contention and scalability issues.
//
// # Customization
//
// The benchmark parameters can be modified to test different scenarios:
//   - Dataset sizes (numKeys, opsPerGoroutine)
//   - Concurrency levels (numGoroutines)
//   - Key/value sizes and patterns
//   - Workload distributions
//
// # Production Considerations
//
// Benchmark results provide guidance for:
//   - Hardware sizing and capacity planning
//   - Performance optimization opportunities
//   - Concurrency limit recommendations
//   - Memory allocation requirements
//   - Expected performance characteristics
//
// # See Also
//
// For interactive testing, see the REPL tool.
// For detailed API documentation, see the core package.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	core "github.com/kianostad/lfdb/internal/core"
)

func main() {
	fmt.Println("Lock-Free Database Benchmarks")
	fmt.Println("=============================")

	// Benchmark 1: Single-threaded operations
	benchmarkSingleThreaded()

	// Benchmark 2: Concurrent reads
	benchmarkConcurrentReads()

	// Benchmark 3: Concurrent writes
	benchmarkConcurrentWrites()

	// Benchmark 4: Mixed workload
	benchmarkMixedWorkload()

	// Benchmark 5: Snapshot performance
	benchmarkSnapshots()

	// Benchmark 6: Hot key performance
	benchmarkHotKeys()

	// Benchmark 7: Iterator performance
	benchmarkIterators()

	// Benchmark 8: Memory usage
	benchmarkMemoryUsage()
}

func benchmarkSingleThreaded() {
	fmt.Println("\n1. Single-threaded operations")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 100000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Benchmark gets
	start := time.Now()
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		database.Get(ctx, key)
	}
	duration := time.Since(start)
	fmt.Printf("   Get: %d ops in %v (%.0f ops/sec)\n", numKeys, duration, float64(numKeys)/duration.Seconds())

	// Benchmark puts
	start = time.Now()
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("putkey%d", i))
		value := []byte(fmt.Sprintf("putvalue%d", i))
		database.Put(ctx, key, value)
	}
	duration = time.Since(start)
	fmt.Printf("   Put: %d ops in %v (%.0f ops/sec)\n", numKeys, duration, float64(numKeys)/duration.Seconds())
}

func benchmarkConcurrentReads() {
	fmt.Println("\n2. Concurrent reads")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Test different numbers of goroutines
	for _, numGoroutines := range []int{1, 2, 4, 8, 16, 32} {
		var wg sync.WaitGroup
		const opsPerGoroutine = 10000
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("key%d", j%numKeys))
					database.Get(ctx, key)
				}
			}()
		}

		wg.Wait()
		duration := time.Since(start)
		totalOps := numGoroutines * opsPerGoroutine
		fmt.Printf("   %d goroutines: %d ops in %v (%.0f ops/sec)\n",
			numGoroutines, totalOps, duration, float64(totalOps)/duration.Seconds())
	}
}

func benchmarkConcurrentWrites() {
	fmt.Println("\n3. Concurrent writes")

	// Test different numbers of goroutines
	for _, numGoroutines := range []int{1, 2, 4, 8, 16, 32} {
		ctx := context.Background()
		database := core.New[[]byte, []byte]()

		var wg sync.WaitGroup
		const opsPerGoroutine = 1000
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := []byte(fmt.Sprintf("key%d_%d", goroutineID, j))
					value := []byte(fmt.Sprintf("value%d_%d", goroutineID, j))
					database.Put(ctx, key, value)
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		totalOps := numGoroutines * opsPerGoroutine
		fmt.Printf("   %d goroutines: %d ops in %v (%.0f ops/sec)\n",
			numGoroutines, totalOps, duration, float64(totalOps)/duration.Seconds())

		database.Close(ctx)
	}
}

func benchmarkMixedWorkload() {
	fmt.Println("\n4. Mixed workload (80% reads, 20% writes)")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Test different numbers of goroutines
	for _, numGoroutines := range []int{1, 2, 4, 8, 16, 32} {
		var wg sync.WaitGroup
		const opsPerGoroutine = 10000
		start := time.Now()

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					if j%5 < 4 { // 80% reads
						key := []byte(fmt.Sprintf("key%d", j%numKeys))
						database.Get(ctx, key)
					} else { // 20% writes
						key := []byte(fmt.Sprintf("writekey%d_%d", goroutineID, j))
						value := []byte(fmt.Sprintf("writevalue%d_%d", goroutineID, j))
						database.Put(ctx, key, value)
					}
				}
			}(i)
		}

		wg.Wait()
		duration := time.Since(start)
		totalOps := numGoroutines * opsPerGoroutine
		fmt.Printf("   %d goroutines: %d ops in %v (%.0f ops/sec)\n",
			numGoroutines, totalOps, duration, float64(totalOps)/duration.Seconds())
	}
}

func benchmarkSnapshots() {
	fmt.Println("\n5. Snapshot performance")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Benchmark snapshot creation and reads
	start := time.Now()
	const numSnapshots = 1000
	for i := 0; i < numSnapshots; i++ {
		snapshot := database.Snapshot(ctx)

		// Read all keys in snapshot
		for j := 0; j < numKeys; j++ {
			key := []byte(fmt.Sprintf("key%d", j))
			snapshot.Get(ctx, key)
		}

		snapshot.Close(ctx)
	}
	duration := time.Since(start)
	totalOps := numSnapshots * numKeys
	fmt.Printf("   %d snapshots with %d reads each: %d ops in %v (%.0f ops/sec)\n",
		numSnapshots, numKeys, totalOps, duration, float64(totalOps)/duration.Seconds())
}

func benchmarkHotKeys() {
	fmt.Println("\n6. Hot key performance")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 1000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Test hot key access (same key repeatedly)
	hotKey := []byte("key1")
	start := time.Now()
	const hotKeyOps = 100000
	for i := 0; i < hotKeyOps; i++ {
		database.Get(ctx, hotKey)
	}
	duration := time.Since(start)
	fmt.Printf("   Hot key access: %d ops in %v (%.0f ops/sec)\n",
		hotKeyOps, duration, float64(hotKeyOps)/duration.Seconds())
}

func benchmarkIterators() {
	fmt.Println("\n7. Iterator performance")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Benchmark iterator
	start := time.Now()
	snapshot := database.Snapshot(ctx)
	count := 0
	snapshot.Iterate(ctx, func(key, val []byte) bool {
		count++
		return true
	})
	snapshot.Close(ctx)
	duration := time.Since(start)
	fmt.Printf("   Iterator over %d keys: %v (%.0f keys/sec)\n",
		count, duration, float64(count)/duration.Seconds())
}

func benchmarkMemoryUsage() {
	fmt.Println("\n8. Memory usage")
	ctx := context.Background()
	database := core.New[[]byte, []byte]()
	defer database.Close(ctx)

	// Prefill with data
	const numKeys = 100000
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		database.Put(ctx, key, value)
	}

	// Get metrics
	metrics := database.GetMetrics(ctx)
	fmt.Printf("   Active snapshots: %v\n", metrics.Memory.ActiveSnapshots)
	fmt.Printf("   Total operations: %v\n", metrics.Operations.Put)
}
