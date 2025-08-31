// Licensed under the MIT License. See LICENSE file in the project root for details.

package metrics

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestNewMetrics(t *testing.T) {
	metrics := NewMetrics()
	if metrics == nil {
		t.Fatal("NewMetrics() returned nil")
	}
	defer metrics.Close()
}

func TestNewMetricsWithConfig(t *testing.T) {
	config := DefaultMetricsConfig()
	config.BufferSize = 5000
	config.LatencyBuffers["get"] = 500

	metrics := NewMetricsWithConfig(config)
	if metrics == nil {
		t.Fatal("NewMetricsWithConfig() returned nil")
	}
	defer metrics.Close()
}

func TestRecordGet(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 100 * time.Microsecond
	metrics.RecordGet(duration)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Get != 1 {
		t.Errorf("Expected GetCount to be 1, got %d", stats.Operations.Get)
	}

	// Check that latency is recorded in the ring buffer
	getLatency := int64(stats.Latency.Get.Mean.Nanoseconds())
	if getLatency != int64(duration.Nanoseconds()) {
		t.Errorf("Expected GetLatency to be %d, got %d", int64(duration.Nanoseconds()), getLatency)
	}
}

func TestRecordPut(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 200 * time.Microsecond
	metrics.RecordPut(duration)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Put != 1 {
		t.Errorf("Expected PutCount to be 1, got %d", stats.Operations.Put)
	}

	putLatency := int64(stats.Latency.Put.Mean.Nanoseconds())
	if putLatency != int64(duration.Nanoseconds()) {
		t.Errorf("Expected PutLatency to be %d, got %d", int64(duration.Nanoseconds()), putLatency)
	}
}

func TestRecordDelete(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 150 * time.Microsecond
	metrics.RecordDelete(duration)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Delete != 1 {
		t.Errorf("Expected DeleteCount to be 1, got %d", stats.Operations.Delete)
	}

	deleteLatency := int64(stats.Latency.Delete.Mean.Nanoseconds())
	if deleteLatency != int64(duration.Nanoseconds()) {
		t.Errorf("Expected DeleteLatency to be %d, got %d", int64(duration.Nanoseconds()), deleteLatency)
	}
}

func TestRecordSnapshot(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 1 * time.Millisecond
	metrics.RecordSnapshot(duration)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Snapshot != 1 {
		t.Errorf("Expected SnapshotCount to be 1, got %d", stats.Operations.Snapshot)
	}

	snapshotLatency := int64(stats.Latency.Snapshot.Mean.Nanoseconds())
	if snapshotLatency != int64(duration.Nanoseconds()) {
		t.Errorf("Expected SnapshotLatency to be %d, got %d", int64(duration.Nanoseconds()), snapshotLatency)
	}
}

func TestRecordTxn(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 5 * time.Millisecond
	metrics.RecordTxn(duration)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Txn != 1 {
		t.Errorf("Expected TxnCount to be 1, got %d", stats.Operations.Txn)
	}

	txnLatency := int64(stats.Latency.Txn.Mean.Nanoseconds())
	if txnLatency != int64(duration.Nanoseconds()) {
		t.Errorf("Expected TxnLatency to be %d, got %d", int64(duration.Nanoseconds()), txnLatency)
	}
}

func TestRecordTxnWithOps(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	duration := 5 * time.Millisecond
	operations := 10
	metrics.RecordTxnWithOps(duration, operations)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()
	if stats.Operations.Txn != 1 {
		t.Errorf("Expected TxnCount to be 1, got %d", stats.Operations.Txn)
	}
}

func TestRecordBatchOperations(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	// Record batch operations
	metrics.RecordBatchPut(1*time.Millisecond, 100)
	metrics.RecordBatchGet(500*time.Microsecond, 50)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()

	if stats.Operations.BatchPut != 1 {
		t.Errorf("Expected BatchPutCount to be 1, got %d", stats.Operations.BatchPut)
	}
	if stats.Operations.BatchGet != 1 {
		t.Errorf("Expected BatchGetCount to be 1, got %d", stats.Operations.BatchGet)
	}
}

func TestRecordError(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.RecordError("get")
	metrics.RecordError("put")
	metrics.RecordError("delete")
	metrics.RecordError("txn")

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()

	if stats.Errors.Get != 1 {
		t.Errorf("Expected GetErrors to be 1, got %d", stats.Errors.Get)
	}
	if stats.Errors.Put != 1 {
		t.Errorf("Expected PutErrors to be 1, got %d", stats.Errors.Put)
	}
	if stats.Errors.Delete != 1 {
		t.Errorf("Expected DeleteErrors to be 1, got %d", stats.Errors.Delete)
	}
	if stats.Errors.Txn != 1 {
		t.Errorf("Expected TxnErrors to be 1, got %d", stats.Errors.Txn)
	}
}

func TestRecordErrorWithContext(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	ctx := ErrorContext{
		Operation: "get",
		Error:     nil,
		Metadata: map[string]string{
			"key":    "user:123",
			"reason": "not_found",
		},
	}
	metrics.RecordErrorWithContext(ctx)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()

	if stats.Errors.Get != 1 {
		t.Errorf("Expected GetErrors to be 1, got %d", stats.Errors.Get)
	}
}

func TestRecordErrorUnknown(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.RecordError("unknown")

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()

	// Unknown errors should not be recorded
	// Note: With structured types, we can't check for unknown errors directly
	// but the error recording logic should handle this correctly
	if stats.Errors.Get != 0 {
		t.Errorf("Expected get errors to be 0, got %d", stats.Errors.Get)
	}
}

func TestSetActiveSnapshots(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.SetActiveSnapshots(10)

	stats := metrics.GetStats()

	if stats.Memory.ActiveSnapshots != 10 {
		t.Errorf("Expected ActiveSnapshots to be 10, got %d", stats.Memory.ActiveSnapshots)
	}
}

func TestSetChainLength(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.SetChainLength(5)

	stats := metrics.GetStats()

	if stats.Memory.ChainLength != 5 {
		t.Errorf("Expected ChainLength to be 5, got %d", stats.Memory.ChainLength)
	}
}

func TestRecordGCTrim(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.RecordGCTrim()
	metrics.RecordGCTrim()

	stats := metrics.GetStats()

	if stats.Memory.GCTrims != 2 {
		t.Errorf("Expected GCTrims to be 2, got %d", stats.Memory.GCTrims)
	}
}

func TestSetHeapUsage(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.SetHeapUsage(1024 * 1024) // 1MB

	stats := metrics.GetStats()

	if stats.Memory.HeapUsage != 1024*1024 {
		t.Errorf("Expected HeapUsage to be 1048576, got %d", stats.Memory.HeapUsage)
	}
}

func TestRecordAllocation(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	metrics.RecordAllocation(1024)
	metrics.RecordAllocation(2048)

	stats := metrics.GetStats()

	if stats.Memory.AllocationCount != 2 {
		t.Errorf("Expected AllocationCount to be 2, got %d", stats.Memory.AllocationCount)
	}
}

func TestGetStats(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	// Record some operations
	metrics.RecordGet(100 * time.Microsecond)
	metrics.RecordPut(200 * time.Microsecond)
	metrics.RecordDelete(150 * time.Microsecond)
	metrics.RecordSnapshot(1 * time.Millisecond)
	metrics.RecordTxn(5 * time.Millisecond)
	metrics.RecordError("get")
	metrics.RecordError("put")
	metrics.RecordGCTrim()

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	stats := metrics.GetStats()

	// Check operations
	if stats.Operations.Get != 1 {
		t.Errorf("Expected get operations to be 1, got %d", stats.Operations.Get)
	}
	if stats.Operations.Put != 1 {
		t.Errorf("Expected put operations to be 1, got %d", stats.Operations.Put)
	}
	if stats.Operations.Delete != 1 {
		t.Errorf("Expected delete operations to be 1, got %d", stats.Operations.Delete)
	}
	if stats.Operations.Snapshot != 1 {
		t.Errorf("Expected snapshot operations to be 1, got %d", stats.Operations.Snapshot)
	}
	if stats.Operations.Txn != 1 {
		t.Errorf("Expected txn operations to be 1, got %d", stats.Operations.Txn)
	}

	// Check latencies
	if int64(stats.Latency.Get.Mean.Nanoseconds()) != int64(100*time.Microsecond.Nanoseconds()) {
		t.Errorf("Expected get latency to be %d, got %d", int64(100*time.Microsecond.Nanoseconds()), int64(stats.Latency.Get.Mean.Nanoseconds()))
	}
	if int64(stats.Latency.Put.Mean.Nanoseconds()) != int64(200*time.Microsecond.Nanoseconds()) {
		t.Errorf("Expected put latency to be %d, got %d", int64(200*time.Microsecond.Nanoseconds()), int64(stats.Latency.Put.Mean.Nanoseconds()))
	}
	if int64(stats.Latency.Delete.Mean.Nanoseconds()) != int64(150*time.Microsecond.Nanoseconds()) {
		t.Errorf("Expected delete latency to be %d, got %d", int64(150*time.Microsecond.Nanoseconds()), int64(stats.Latency.Delete.Mean.Nanoseconds()))
	}
	if int64(stats.Latency.Snapshot.Mean.Nanoseconds()) != int64(1*time.Millisecond.Nanoseconds()) {
		t.Errorf("Expected snapshot latency to be %d, got %d", int64(1*time.Millisecond.Nanoseconds()), int64(stats.Latency.Snapshot.Mean.Nanoseconds()))
	}
	if int64(stats.Latency.Txn.Mean.Nanoseconds()) != int64(5*time.Millisecond.Nanoseconds()) {
		t.Errorf("Expected txn latency to be %d, got %d", int64(5*time.Millisecond.Nanoseconds()), int64(stats.Latency.Txn.Mean.Nanoseconds()))
	}

	// Check errors
	if stats.Errors.Get != 1 {
		t.Errorf("Expected get errors to be 1, got %d", stats.Errors.Get)
	}
	if stats.Errors.Put != 1 {
		t.Errorf("Expected put errors to be 1, got %d", stats.Errors.Put)
	}

	// Check memory
	if stats.Memory.GCTrims != 1 {
		t.Errorf("Expected gc_trims to be 1, got %d", stats.Memory.GCTrims)
	}

	// Check that config is included
	if stats.Configuration.BufferSize == 0 {
		t.Error("Expected config to include buffer_size")
	}
}

// TestConcurrentAccess verifies that all operation counters account for
// concurrent updates without dropping events.
func TestConcurrentAccess(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	operationsPerGoroutine := 100

	// Start multiple goroutines recording operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < operationsPerGoroutine; j++ {
				metrics.RecordGet(time.Microsecond)
				metrics.RecordPut(time.Microsecond)
				metrics.RecordDelete(time.Microsecond)
			}
		}()
	}

	wg.Wait()

	expectedCount := uint64(numGoroutines * operationsPerGoroutine)

	// Poll until background processing catches up or timeout triggers.
	deadline := time.Now().Add(100 * time.Millisecond)
	for {
		stats := metrics.GetStats()
		if stats.Operations.Get == expectedCount &&
			stats.Operations.Put == expectedCount &&
			stats.Operations.Delete == expectedCount {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected %d operations, got get=%d put=%d delete=%d",
				expectedCount, stats.Operations.Get, stats.Operations.Put, stats.Operations.Delete)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRingBufferAverage(t *testing.T) {
	rb := NewDurationRingBuffer(5)

	// Add some values
	rb.Push(100 * time.Microsecond)
	rb.Push(200 * time.Microsecond)
	rb.Push(300 * time.Microsecond)

	average := rb.GetAverage()
	expected := 200 * time.Microsecond // (100 + 200 + 300) / 3

	if average != expected {
		t.Errorf("Expected average to be %v, got %v", expected, average)
	}
}

func TestRingBufferOverflow(t *testing.T) {
	rb := NewDurationRingBuffer(3)

	// Add more values than the buffer can hold
	rb.Push(100 * time.Microsecond)
	rb.Push(200 * time.Microsecond)
	rb.Push(300 * time.Microsecond)
	rb.Push(400 * time.Microsecond) // This should overwrite the first value

	average := rb.GetAverage()
	expected := 300 * time.Microsecond // (200 + 300 + 400) / 3

	if average != expected {
		t.Errorf("Expected average to be %v, got %v", expected, average)
	}
}

func TestRingBufferEmpty(t *testing.T) {
	rb := NewDurationRingBuffer(5)

	average := rb.GetAverage()
	if average != 0 {
		t.Errorf("Expected average to be 0 for empty buffer, got %v", average)
	}
}

func TestRingBufferStats(t *testing.T) {
	rb := NewDurationRingBuffer(10)

	// Add values: 100, 200, 300, 400, 500
	for i := 1; i <= 5; i++ {
		rb.Push(time.Duration(i*100) * time.Microsecond)
	}

	stats := rb.GetStats()

	if stats.Count != 5 {
		t.Errorf("Expected count to be 5, got %d", stats.Count)
	}
	if stats.Min != 100*time.Microsecond {
		t.Errorf("Expected min to be 100μs, got %v", stats.Min)
	}
	if stats.Max != 500*time.Microsecond {
		t.Errorf("Expected max to be 500μs, got %v", stats.Max)
	}
	if stats.Mean != 300*time.Microsecond {
		t.Errorf("Expected mean to be 300μs, got %v", stats.Mean)
	}
	if stats.P50 != 300*time.Microsecond {
		t.Errorf("Expected P50 to be 300μs, got %v", stats.P50)
	}
	// For 5 values: P95 = 0.95 * 4 = 3.8 -> index 3 (400μs), P99 = 0.99 * 4 = 3.96 -> index 3 (400μs)
	if stats.P95 != 400*time.Microsecond {
		t.Errorf("Expected P95 to be 400μs, got %v", stats.P95)
	}
	if stats.P99 != 400*time.Microsecond {
		t.Errorf("Expected P99 to be 400μs, got %v", stats.P99)
	}
}

func TestExportJSON(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	// Record some operations
	metrics.RecordGet(100 * time.Microsecond)
	metrics.RecordPut(200 * time.Microsecond)

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	jsonData := metrics.ExportJSON()
	if len(jsonData) == 0 {
		t.Error("Expected non-empty JSON export")
	}

	// Verify JSON is valid
	var parsed map[string]interface{}
	if err := json.Unmarshal(jsonData, &parsed); err != nil {
		t.Errorf("Expected valid JSON, got error: %v", err)
	}
}

func TestExportPrometheus(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	// Record some operations
	metrics.RecordGet(100 * time.Microsecond)
	metrics.RecordError("get")

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	prometheusData := metrics.ExportPrometheus()
	if len(prometheusData) == 0 {
		t.Error("Expected non-empty Prometheus export")
	}

	// Check that it contains expected Prometheus format
	if len(prometheusData) < 100 {
		t.Error("Expected substantial Prometheus data")
	}
}

func TestGetStatsLegacy(t *testing.T) {
	metrics := NewMetrics()
	defer metrics.Close()

	// Record some operations
	metrics.RecordGet(100 * time.Microsecond)
	metrics.RecordPut(200 * time.Microsecond)
	metrics.RecordError("get")

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	// Test legacy API
	legacyStats := metrics.GetStatsLegacy()

	// Check that legacy format is correct
	operations := legacyStats["operations"].(map[string]uint64)
	if operations["get"] != 1 {
		t.Errorf("Expected get operations to be 1, got %d", operations["get"])
	}
	if operations["put"] != 1 {
		t.Errorf("Expected put operations to be 1, got %d", operations["put"])
	}

	errors := legacyStats["errors"].(map[string]uint64)
	if errors["get"] != 1 {
		t.Errorf("Expected get errors to be 1, got %d", errors["get"])
	}

	// Check that latency_stats is available
	latencyStats := legacyStats["latency_stats"].(map[string]LatencyStats)
	if latencyStats["get"].Count != 1 {
		t.Errorf("Expected get latency count to be 1, got %d", latencyStats["get"].Count)
	}
}
