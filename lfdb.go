// Licensed under the MIT License. See LICENSE file in the project root for details.

// Package lfdb provides a high-performance, lock-free in-memory database with MVCC (Multi-Version Concurrency Control) support.
//
// This is the main public API for the LFDB library. It provides easy access to all database functionality
// including basic CRUD operations, transactions, snapshots, atomic operations, TTL support, and batch operations.
//
// # Quick Start
//
//	import "github.com/kianostad/lfdb"
//
//	// String-based API (recommended for most use cases)
//	db := lfdb.NewStringDB[string]()
//	defer db.Close(ctx)
//
//	db.Put(ctx, "key", "value")
//	value, exists := db.Get(ctx, "key")
//
//	// Or use the original []byte-based API for maximum performance
//	byteDB := lfdb.New[[]byte, string]()
//	defer byteDB.Close(ctx)
//
//	byteDB.Put(ctx, []byte("key"), "value")
//	value, exists := byteDB.Get(ctx, []byte("key"))
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
// # Usage Examples
//
// String-based API (recommended):
//
//	db := lfdb.NewStringDB[string]()
//	defer db.Close(ctx)
//
//	db.Put(ctx, "key", "value")
//	value, exists := db.Get(ctx, "key")
//	deleted := db.Delete(ctx, "key")
//
// Transactions:
//
//	err := db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
//	    tx.Put(ctx, "key1", "value1")
//	    tx.Put(ctx, "key2", "value2")
//	    return nil
//	})
//
// Snapshots:
//
//	snapshot := db.Snapshot(ctx)
//	defer snapshot.Close(ctx)
//	value, exists := snapshot.Get(ctx, "key")
//
// Atomic operations (for numeric types):
//
//	numDB := lfdb.NewStringDB[int]()
//	newVal, _ := numDB.Add(ctx, "counter", 5)
//	newVal, _ = numDB.Increment(ctx, "counter")
//
// TTL operations:
//
//	db.PutWithTTL(ctx, "temp", "value", 5*time.Minute)
//	ttl, exists := db.GetTTL(ctx, "temp")
//
// Batch operations:
//
//	batch := lfdb.NewStringBatch[string]()
//	batch.Put("key1", "value1")
//	batch.Put("key2", "value2")
//	db.ExecuteBatch(ctx, batch)
//
// # API Design Philosophy
//
// The library provides two APIs:
//
// 1. **String-based API** (recommended): Uses string keys for better developer experience
//   - Cleaner syntax: `db.Put(ctx, "key", "value")`
//   - Better readability and debugging
//   - Type safety for string keys
//   - Minimal performance overhead (converts to []byte internally)
//
// 2. **[]byte-based API**: Uses []byte keys for maximum performance
//   - Zero-copy operations possible
//   - Slightly better performance for high-throughput scenarios
//   - More verbose syntax: `db.Put(ctx, []byte("key"), "value")`
//
// # Performance Characteristics
//
//   - **String API Overhead**: ~1-2% performance overhead due to string-to-[]byte conversion
//   - **Memory Usage**: String keys use slightly more memory due to UTF-8 encoding
//   - **Hash Performance**: Both APIs use the same optimized hash index internally
//   - **Concurrency**: Both APIs provide the same lock-free performance
//
// # Best Practices
//
//   - Use the string-based API for most applications
//   - Use the []byte-based API only when you need maximum performance
//   - Always call Close() when done with the database
//   - Use transactions for multi-key operations
//   - Use snapshots for consistent reads across multiple operations
//   - Monitor metrics for performance insights
//
// # See Also
//
// For database interface details, see the core package.
package lfdb

import core "github.com/kianostad/lfdb/internal/core"

// Re-export core types for backward compatibility
type (
	// DB is the main database interface with []byte keys
	DB[K ~[]byte, V any] = core.DB[K, V]

	// Snapshot provides a consistent read view of the database
	Snapshot[K ~[]byte, V any] = core.Snapshot[K, V]

	// Txn represents a transaction with snapshot isolation
	Txn[K ~[]byte, V any] = core.Txn[K, V]

	// BatchGetResult represents the result of a batch get operation
	BatchGetResult[V any] = core.BatchGetResult[V]

	// Batch represents a batch of operations to be executed atomically
	Batch[K ~[]byte, V any] = *core.Batch[K, V]
)

// String-based API types for better developer experience
type (
	// StringKeyDB provides a string-keyed interface for better developer experience
	// while maintaining []byte performance internally
	StringKeyDB[V any] = *core.StringDB[V]

	// StringKeySnapshot provides a string-keyed snapshot interface
	StringKeySnapshot[V any] = *core.StringSnapshot[V]

	// StringKeyTxn provides a string-keyed transaction interface
	StringKeyTxn[V any] = *core.StringTxn[V]

	// StringKeyBatch represents a batch of operations with string keys
	StringKeyBatch[V any] = *core.StringBatch[V]

	// StringKeyBatchGetResult represents the result of a batch get operation with string keys
	StringKeyBatchGetResult[V any] = core.StringBatchGetResult[V]
)

// New creates a new database instance with []byte keys (original API)
func New[K ~[]byte, V any]() DB[K, V] {
	return core.New[K, V]()
}

// NewStringDB creates a new database instance with string keys (recommended API)
func NewStringDB[V any]() StringKeyDB[V] {
	return core.NewStringDB[V]()
}

// NewBatch creates a new batch with []byte keys (original API)
func NewBatch[K ~[]byte, V any]() Batch[K, V] {
	return core.NewBatch[K, V]()
}

// NewStringBatch creates a new batch with string keys
func NewStringBatch[V any]() StringKeyBatch[V] {
	return core.NewStringBatch[V]()
}

// Common type aliases for convenience ([]byte-based API)
type (
	// StringDB is a database with string keys and values ([]byte-based API)
	StringDB = DB[[]byte, string]

	// IntDB is a database with string keys and int values ([]byte-based API)
	IntDB = DB[[]byte, int]

	// Int64DB is a database with string keys and int64 values ([]byte-based API)
	Int64DB = DB[[]byte, int64]

	// Float64DB is a database with string keys and float64 values ([]byte-based API)
	Float64DB = DB[[]byte, float64]

	// BytesDB is a database with string keys and byte slice values ([]byte-based API)
	BytesDB = DB[[]byte, []byte]
)

// Convenience constructors for common types ([]byte-based API)
func NewStringDBLegacy() StringDB {
	return New[[]byte, string]()
}

func NewIntDB() IntDB {
	return New[[]byte, int]()
}

func NewInt64DB() Int64DB {
	return New[[]byte, int64]()
}

func NewFloat64DB() Float64DB {
	return New[[]byte, float64]()
}

func NewBytesDB() BytesDB {
	return New[[]byte, []byte]()
}
