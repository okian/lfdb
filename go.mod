// Licensed under the MIT License. See LICENSE file in the project root for details.

// LFDB (Lock-Free Database) is a high-performance, lock-free in-memory database
// written in Go with MVCC (Multi-Version Concurrency Control) support.
//
// This module provides a complete database solution with the following packages:
//
//   - github.com/kianostad/lfdb/internal/core: Main database interface and implementation
//   - github.com/kianostad/lfdb/internal/storage: Storage layer with indexes and MVCC
//   - github.com/kianostad/lfdb/internal/concurrency: Concurrency primitives and epoch management
//   - github.com/kianostad/lfdb/internal/monitoring: Metrics and observability
//
// # Key Features
//
//   - Lock-free algorithms for maximum concurrency
//   - MVCC for consistent reads without blocking writes
//   - Epoch-based garbage collection for safe memory reclamation
//   - SIMD optimizations for bulk operations
//   - Comprehensive metrics and monitoring
//   - TTL support for automatic expiration
//   - Atomic operations for numeric types
//   - Batch operations for improved performance
//   - Snapshot isolation for consistent reads
//
// # Quick Start
//
//	go get github.com/kianostad/lfdb
//
//	import "github.com/kianostad/lfdb/internal/core"
//
//	db := core.New[[]byte, string]()
//	defer db.Close(ctx)
//
//	db.Put(ctx, []byte("key"), "value")
//	value, exists := db.Get(ctx, []byte("key"))
//
// # Performance Characteristics
//
//   - High-throughput read/write operations
//   - Linear scalability with CPU cores
//   - Minimal memory overhead per entry
//   - Efficient garbage collection
//   - Lock-free concurrent access
//
// # Use Cases
//
//   - High-performance caching systems
//   - Real-time data processing
//   - In-memory analytics
//   - Session storage
//   - Configuration management
//   - Temporary data storage with TTL
//
// # Dependencies
//
// This module requires Go 1.24.6 or later and includes the following dependencies:
//   - github.com/smartystreets/goconvey: Testing framework
//   - go.uber.org/goleak: Memory leak detection
//   - golang.org/x/sys: System calls
//   - pgregory.net/rapid: Property-based testing
//
// # Documentation
//
// For detailed documentation, run:
//   go doc github.com/kianostad/lfdb/internal/core
//   go doc github.com/kianostad/lfdb/internal/storage
//   go doc github.com/kianostad/lfdb/internal/concurrency
//   go doc github.com/kianostad/lfdb/internal/monitoring
//
// # Examples
//
// See the examples/ directory for usage examples and the cmd/ directory for
// command-line tools including a REPL and benchmarking suite.
//
// # License
//
// This project is licensed under the MIT License.
// See the LICENSE file for details.
module github.com/kianostad/lfdb

go 1.24.6

require (
	github.com/smartystreets/goconvey v1.8.1
	go.uber.org/goleak v1.3.0
	golang.org/x/sys v0.35.0
	pgregory.net/rapid v1.2.0
)

require (
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/smarty/assertions v1.15.0 // indirect
)
