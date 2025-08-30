// Licensed under the MIT License. See LICENSE file in the project root for details.

package metrics

import (
	"testing"
	"time"
)

// BenchmarkAtomicMetrics benchmarks the current atomic-based metrics
func BenchmarkAtomicMetrics(b *testing.B) {
	metrics := NewMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.RecordGet(100 * time.Microsecond)
			metrics.RecordPut(200 * time.Microsecond)
			metrics.RecordDelete(150 * time.Microsecond)
		}
	})
}

// BenchmarkBufferedMetrics benchmarks the buffered channel-based metrics
func BenchmarkBufferedMetrics(b *testing.B) {
	metrics := NewBufferedMetrics(10000)
	defer metrics.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			metrics.RecordGet(100 * time.Microsecond)
			metrics.RecordPut(200 * time.Microsecond)
			metrics.RecordDelete(150 * time.Microsecond)
		}
	})
}

// BenchmarkAtomicMetricsHighContention benchmarks atomic metrics under high contention
func BenchmarkAtomicMetricsHighContention(b *testing.B) {
	metrics := NewMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Simulate high contention by recording many operations
			for i := 0; i < 10; i++ {
				metrics.RecordGet(100 * time.Microsecond)
				metrics.RecordPut(200 * time.Microsecond)
				metrics.RecordDelete(150 * time.Microsecond)
				metrics.RecordError("get")
				metrics.RecordError("put")
			}
		}
	})
}

// BenchmarkBufferedMetricsHighContention benchmarks buffered metrics under high contention
func BenchmarkBufferedMetricsHighContention(b *testing.B) {
	metrics := NewBufferedMetrics(10000)
	defer metrics.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Simulate high contention by recording many operations
			for i := 0; i < 10; i++ {
				metrics.RecordGet(100 * time.Microsecond)
				metrics.RecordPut(200 * time.Microsecond)
				metrics.RecordDelete(150 * time.Microsecond)
				metrics.RecordError("get")
				metrics.RecordError("put")
			}
		}
	})
}

// BenchmarkAtomicMetricsGetStats benchmarks getting stats from atomic metrics
func BenchmarkAtomicMetricsGetStats(b *testing.B) {
	metrics := NewMetrics()

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		metrics.RecordGet(100 * time.Microsecond)
		metrics.RecordPut(200 * time.Microsecond)
		metrics.RecordDelete(150 * time.Microsecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.GetStats()
	}
}

// BenchmarkBufferedMetricsGetStats benchmarks getting stats from buffered metrics
func BenchmarkBufferedMetricsGetStats(b *testing.B) {
	metrics := NewBufferedMetrics(10000)
	defer metrics.Close()

	// Pre-populate with some data
	for i := 0; i < 1000; i++ {
		metrics.RecordGet(100 * time.Microsecond)
		metrics.RecordPut(200 * time.Microsecond)
		metrics.RecordDelete(150 * time.Microsecond)
	}

	// Give some time for background processing
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		metrics.GetStats()
	}
}

// BenchmarkAtomicMetricsMixedWorkload benchmarks atomic metrics with mixed workload
func BenchmarkAtomicMetricsMixedWorkload(b *testing.B) {
	metrics := NewMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mixed workload: mostly gets, some puts, few deletes
			metrics.RecordGet(100 * time.Microsecond)
			if pb.Next() {
				metrics.RecordPut(200 * time.Microsecond)
			}
			if pb.Next() {
				metrics.RecordDelete(150 * time.Microsecond)
			}
			if pb.Next() {
				metrics.RecordError("get")
			}
		}
	})
}

// BenchmarkBufferedMetricsMixedWorkload benchmarks buffered metrics with mixed workload
func BenchmarkBufferedMetricsMixedWorkload(b *testing.B) {
	metrics := NewBufferedMetrics(10000)
	defer metrics.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Mixed workload: mostly gets, some puts, few deletes
			metrics.RecordGet(100 * time.Microsecond)
			if pb.Next() {
				metrics.RecordPut(200 * time.Microsecond)
			}
			if pb.Next() {
				metrics.RecordDelete(150 * time.Microsecond)
			}
			if pb.Next() {
				metrics.RecordError("get")
			}
		}
	})
}

// BenchmarkRingBufferPush benchmarks ring buffer push operations
func BenchmarkRingBufferPush(b *testing.B) {
	rb := NewDurationRingBuffer(1000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Push(100 * time.Microsecond)
		}
	})
}

// BenchmarkRingBufferGetAverage benchmarks ring buffer average calculation
func BenchmarkRingBufferGetAverage(b *testing.B) {
	rb := NewDurationRingBuffer(1000)

	// Pre-populate the buffer
	for i := 0; i < 1000; i++ {
		rb.Push(time.Duration(i) * time.Microsecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rb.GetAverage()
	}
}
