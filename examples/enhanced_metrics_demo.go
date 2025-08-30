// Licensed under the MIT License. See LICENSE file in the project root for details.

package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/kianostad/lfdb/internal/monitoring/metrics"
)

func enhancedMetricsDemo() {
	fmt.Println("Enhanced Metrics API Demo")
	fmt.Println("=========================")

	// Create metrics with custom configuration
	config := metrics.DefaultMetricsConfig()
	config.BufferSize = 5000
	config.LatencyBuffers["get"] = 500
	config.LatencyBuffers["put"] = 500
	config.EnablePrometheus = true

	metrics := metrics.NewMetricsWithConfig(config)
	defer metrics.Close()

	// Simulate various operations with different latencies
	fmt.Println("\n1. Recording Operations with Variable Latencies")
	fmt.Println("------------------------------------------------")

	// Record operations with variable latencies
	for i := 0; i < 100; i++ {
		// Simulate variable latency (50-500μs for gets, 100-1000μs for puts)
		getLatency := time.Duration(50+rand.Intn(450)) * time.Microsecond
		putLatency := time.Duration(100+rand.Intn(900)) * time.Microsecond

		metrics.RecordGet(getLatency)
		metrics.RecordPut(putLatency)

		// Record some errors
		if i%10 == 0 {
			metrics.RecordError("get")
		}
		if i%15 == 0 {
			metrics.RecordError("put")
		}
	}

	// Record batch operations
	fmt.Println("\n2. Recording Batch Operations")
	fmt.Println("-----------------------------")

	metrics.RecordBatchPut(5*time.Millisecond, 1000)
	metrics.RecordBatchGet(2*time.Millisecond, 500)
	metrics.RecordBatchPut(3*time.Millisecond, 750)

	// Record transactions with operation counts
	fmt.Println("\n3. Recording Transactions")
	fmt.Println("-------------------------")

	metrics.RecordTxnWithOps(10*time.Millisecond, 5)
	metrics.RecordTxnWithOps(15*time.Millisecond, 10)
	metrics.RecordTxnWithOps(8*time.Millisecond, 3)

	// Record errors with context
	fmt.Println("\n4. Recording Errors with Context")
	fmt.Println("---------------------------------")

	// Use simple error recording for now
	metrics.RecordError("get")
	metrics.RecordError("put")

	// Set memory metrics
	fmt.Println("\n5. Setting Memory Metrics")
	fmt.Println("-------------------------")

	metrics.SetActiveSnapshots(25)
	metrics.SetChainLength(8)
	metrics.SetHeapUsage(1024 * 1024 * 50) // 50MB
	metrics.RecordAllocation(1024)
	metrics.RecordAllocation(2048)
	metrics.RecordGCTrim()

	// Give some time for background processing
	time.Sleep(100 * time.Millisecond)

	// Get comprehensive statistics
	fmt.Println("\n6. Comprehensive Statistics")
	fmt.Println("---------------------------")

	stats := metrics.GetStats()

	// Display operation counts
	fmt.Printf("Operation Counts:\n")
	fmt.Printf("  get: %d\n", stats.Operations.Get)
	fmt.Printf("  put: %d\n", stats.Operations.Put)
	fmt.Printf("  delete: %d\n", stats.Operations.Delete)
	fmt.Printf("  snapshot: %d\n", stats.Operations.Snapshot)
	fmt.Printf("  txn: %d\n", stats.Operations.Txn)
	fmt.Printf("  batch_put: %d\n", stats.Operations.BatchPut)
	fmt.Printf("  batch_get: %d\n", stats.Operations.BatchGet)

	// Display latency statistics
	fmt.Printf("\nLatency Statistics:\n")
	fmt.Printf("  Get: Count=%d, Mean=%v, P95=%v, P99=%v\n",
		stats.Latency.Get.Count, stats.Latency.Get.Mean, stats.Latency.Get.P95, stats.Latency.Get.P99)
	fmt.Printf("  Put: Count=%d, Mean=%v, P95=%v, P99=%v\n",
		stats.Latency.Put.Count, stats.Latency.Put.Mean, stats.Latency.Put.P95, stats.Latency.Put.P99)
	fmt.Printf("  Delete: Count=%d, Mean=%v, P95=%v, P99=%v\n",
		stats.Latency.Delete.Count, stats.Latency.Delete.Mean, stats.Latency.Delete.P95, stats.Latency.Delete.P99)

	// Display error counts
	fmt.Printf("\nError Counts:\n")
	fmt.Printf("  get: %d\n", stats.Errors.Get)
	fmt.Printf("  put: %d\n", stats.Errors.Put)
	fmt.Printf("  delete: %d\n", stats.Errors.Delete)
	fmt.Printf("  txn: %d\n", stats.Errors.Txn)

	// Display memory metrics
	fmt.Printf("\nMemory Metrics:\n")
	fmt.Printf("  active_snapshots: %d\n", stats.Memory.ActiveSnapshots)
	fmt.Printf("  chain_length: %d\n", stats.Memory.ChainLength)
	fmt.Printf("  gc_trims: %d\n", stats.Memory.GCTrims)
	fmt.Printf("  heap_usage: %d bytes\n", stats.Memory.HeapUsage)
	fmt.Printf("  allocation_count: %d\n", stats.Memory.AllocationCount)

	// Display configuration
	fmt.Printf("\nConfiguration:\n")
	fmt.Printf("  buffer_size: %d\n", stats.Configuration.BufferSize)
	fmt.Printf("  sampling_rate: %f\n", stats.Configuration.SamplingRate)
	fmt.Printf("  latency_buffers: %v\n", stats.Configuration.LatencyBuffers)

	// Export to different formats
	fmt.Println("\n7. Export Capabilities")
	fmt.Println("---------------------")

	// Export to JSON
	jsonData := metrics.ExportJSON()
	fmt.Printf("JSON Export (%d bytes):\n", len(jsonData))
	fmt.Printf("  %s...\n", string(jsonData[:100]))

	// Export to Prometheus
	prometheusData := metrics.ExportPrometheus()
	fmt.Printf("\nPrometheus Export (%d bytes):\n", len(prometheusData))
	fmt.Printf("  %s...\n", prometheusData[:200])

	fmt.Println("\nDemo completed successfully!")
}

// EnhancedMetricsDemo demonstrates the enhanced metrics API
func EnhancedMetricsDemo() {
	enhancedMetricsDemo()
}
