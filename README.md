# LFDB - Lock-Free Database

A high-performance, lock-free in-memory database written in Go with MVCC (Multi-Version Concurrency Control) support.

## Features

- **Lock-free algorithms** for maximum concurrency
- **MVCC** for consistent reads without blocking writes
- **Epoch-based garbage collection** for safe memory reclamation
- **SIMD optimizations** for bulk operations
- **TTL support** for automatic expiration
- **Atomic operations** for numeric types
- **Batch operations** for improved performance
- **Snapshot isolation** for consistent reads
- **Comprehensive metrics** and monitoring

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/kianostad/lfdb"
)

func main() {
    ctx := context.Background()
    
    // Create a new database with string keys (recommended)
    db := lfdb.NewStringDB[string]()
    defer db.Close(ctx)
    
    // Basic operations
    db.Put(ctx, "hello", "world")
    value, exists := db.Get(ctx, "hello")
    fmt.Printf("Value: %s, exists: %t\n", value, exists)
    
    // Transactions
    err := db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
        tx.Put(ctx, "key1", "value1")
        tx.Put(ctx, "key2", "value2")
        return nil
    })
    
    // Snapshots
    snapshot := db.Snapshot(ctx)
    defer snapshot.Close(ctx)
    value, exists = snapshot.Get(ctx, "key1")
    
    // TTL operations
    db.PutWithTTL(ctx, "temp", "value", 5*time.Minute)
    ttl, exists := db.GetTTL(ctx, "temp")
    
    // Atomic operations (for numeric types)
    numDB := lfdb.NewStringDB[int]()
    newVal, _ := numDB.Add(ctx, "counter", 5)
    newVal, _ = numDB.Increment(ctx, "counter")
}

// Alternative: Use []byte keys for maximum performance
func performanceExample() {
    ctx := context.Background()
    
    // Create database with []byte keys (maximum performance)
    db := lfdb.New[[]byte, string]()
    defer db.Close(ctx)
    
    db.Put(ctx, []byte("hello"), "world")
    value, exists := db.Get(ctx, []byte("hello"))
}
```

## Installation

```bash
go get github.com/kianostad/lfdb
```

## API Overview

### Basic Operations

```go
// Create database with string keys (recommended for most use cases)
db := lfdb.NewStringDB[string]()

// CRUD operations
db.Put(ctx, "key", "value")
value, exists := db.Get(ctx, "key")
deleted := db.Delete(ctx, "key")

// Cleanup
db.Close(ctx)

// Alternative: Use []byte keys for maximum performance
byteDB := lfdb.New[[]byte, string]()
byteDB.Put(ctx, []byte("key"), "value")
value, exists = byteDB.Get(ctx, []byte("key"))
```

### Transactions

```go
// String-based transactions (recommended)
err := db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
    tx.Put(ctx, "key1", "value1")
    tx.Put(ctx, "key2", "value2")
    return nil
})

// Alternative: []byte-based transactions
err = byteDB.Txn(ctx, func(tx lfdb.Txn[[]byte, string]) error {
    tx.Put(ctx, []byte("key1"), "value1")
    tx.Put(ctx, []byte("key2"), "value2")
    return nil
})
```

### Snapshots

```go
// String-based snapshots (recommended)
snapshot := db.Snapshot(ctx)
defer snapshot.Close(ctx)

value, exists := snapshot.Get(ctx, "key")
snapshot.Iterate(ctx, func(key string, val string) bool {
    // Process each key-value pair
    return true // Return false to stop iteration
})

// Alternative: []byte-based snapshots
byteSnapshot := byteDB.Snapshot(ctx)
defer byteSnapshot.Close(ctx)

value, exists = byteSnapshot.Get(ctx, []byte("key"))
byteSnapshot.Iterate(ctx, func(key []byte, val string) bool {
    return true
})
```

### TTL Operations

```go
// String-based TTL operations (recommended)
db.PutWithTTL(ctx, "temp", "value", 5*time.Minute)
ttl, exists := db.GetTTL(ctx, "temp")
db.ExtendTTL(ctx, "temp", 1*time.Minute)
db.RemoveTTL(ctx, "temp")

// Alternative: []byte-based TTL operations
byteDB.PutWithTTL(ctx, []byte("temp"), "value", 5*time.Minute)
ttl, exists = byteDB.GetTTL(ctx, []byte("temp"))
```

### Atomic Operations

```go
// String-based atomic operations (recommended)
numDB := lfdb.NewStringDB[int]()

newVal, _ := numDB.Add(ctx, "counter", 5)
newVal, _ = numDB.Increment(ctx, "counter")
newVal, _ = numDB.Decrement(ctx, "counter")
newVal, _ = numDB.Multiply(ctx, "counter", 2)
newVal, _ = numDB.Divide(ctx, "counter", 2)

// Alternative: []byte-based atomic operations
byteNumDB := lfdb.New[[]byte, int]()
newVal, _ = byteNumDB.Add(ctx, []byte("counter"), 5)
```

### Batch Operations

```go
// String-based batch operations (recommended)
keys := []string{"key1", "key2"}
values := []string{"value1", "value2"}
err := db.BatchPut(ctx, keys, values)

results := db.BatchGet(ctx, keys)
for i, result := range results {
    fmt.Printf("Key: %s, Value: %s, Exists: %t\n", 
        keys[i], result.Value, result.Found)
}

err = db.BatchDelete(ctx, keys)

// Alternative: []byte-based batch operations
byteKeys := [][]byte{[]byte("key1"), []byte("key2")}
err = byteDB.BatchPut(ctx, byteKeys, values)
```

### Convenience Types

```go
// String-based convenience types (recommended)
stringDB := lfdb.NewStringDB[string]()
intDB := lfdb.NewStringDB[int]()
int64DB := lfdb.NewStringDB[int64]()
float64DB := lfdb.NewStringDB[float64]()
bytesDB := lfdb.NewStringDB[[]byte]()

// Alternative: Legacy []byte-based convenience types
legacyStringDB := lfdb.NewStringDBLegacy()
legacyIntDB := lfdb.NewIntDB()
legacyInt64DB := lfdb.NewInt64DB()
legacyFloat64DB := lfdb.NewFloat64DB()
legacyBytesDB := lfdb.NewBytesDB()
```

## Performance Characteristics

- **High-throughput**: Optimized for concurrent read/write operations
- **Linear scalability**: Performance scales with CPU cores
- **Low latency**: Lock-free algorithms minimize contention
- **Memory efficient**: Epoch-based garbage collection with minimal overhead
- **SIMD optimized**: Bulk operations use SIMD instructions where available

## Thread Safety

All database operations are thread-safe and can be called concurrently from multiple goroutines. The database uses lock-free algorithms to ensure maximum concurrency without blocking.

## Memory Management

The database uses epoch-based garbage collection to safely reclaim memory from deleted entries and expired TTL entries. Manual garbage collection can be triggered if needed:

```go
err := db.ManualGC(ctx)
```

## Best Practices

1. **Always close databases**: Use `defer db.Close(ctx)` to ensure proper cleanup
2. **Use transactions**: Group related operations in transactions for consistency
3. **Prefer batch operations**: Use batch operations for bulk data handling
4. **Monitor memory**: Call `ManualGC()` periodically in long-running applications
5. **Use snapshots**: Use snapshots for read-heavy workloads that need consistency
6. **Keep keys small**: Prefer keys under 1KB for optimal performance
7. **Use TTL**: Use TTL for temporary data to prevent memory leaks
8. **Handle context**: Use appropriate context timeouts and cancellation

## Examples

See the `examples/` directory for comprehensive usage examples:

```bash
go run examples/basic_usage.go
```

## Command Line Tools

The project includes command-line tools for testing and benchmarking:

```bash
# REPL for interactive database usage
make repl

# Run benchmarks
make bench

# Run throughput tests
./scripts/run_throughput_tests.sh
```

## Testing

```bash
# Run all tests
make test

# Run benchmarks
make bench

# Run specific test categories
go test ./tests/...
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
