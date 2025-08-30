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
    "github.com/kianostad/lfdb"
)

func main() {
    ctx := context.Background()
    
    // Create a new database
    db := lfdb.New[[]byte, string]()
    defer db.Close(ctx)
    
    // Basic operations
    db.Put(ctx, []byte("hello"), "world")
    value, exists := db.Get(ctx, []byte("hello"))
    fmt.Printf("Value: %s, exists: %t\n", value, exists)
    
    // Transactions
    err := db.Txn(ctx, func(tx lfdb.Txn[[]byte, string]) error {
        tx.Put(ctx, []byte("key1"), "value1")
        tx.Put(ctx, []byte("key2"), "value2")
        return nil
    })
    
    // Snapshots
    snapshot := db.Snapshot(ctx)
    defer snapshot.Close(ctx)
    value, exists = snapshot.Get(ctx, []byte("key1"))
    
    // TTL operations
    db.PutWithTTL(ctx, []byte("temp"), "value", 5*time.Minute)
    ttl, exists := db.GetTTL(ctx, []byte("temp"))
    
    // Atomic operations (for numeric types)
    numDB := lfdb.New[[]byte, int]()
    newVal, _ := numDB.Add(ctx, []byte("counter"), 5)
    newVal, _ = numDB.Increment(ctx, []byte("counter"))
}
```

## Installation

```bash
go get github.com/kianostad/lfdb
```

## API Overview

### Basic Operations

```go
// Create database
db := lfdb.New[[]byte, string]()

// CRUD operations
db.Put(ctx, []byte("key"), "value")
value, exists := db.Get(ctx, []byte("key"))
deleted := db.Delete(ctx, []byte("key"))

// Cleanup
db.Close(ctx)
```

### Transactions

```go
err := db.Txn(ctx, func(tx lfdb.Txn[[]byte, string]) error {
    tx.Put(ctx, []byte("key1"), "value1")
    tx.Put(ctx, []byte("key2"), "value2")
    return nil
})
```

### Snapshots

```go
snapshot := db.Snapshot(ctx)
defer snapshot.Close(ctx)

value, exists := snapshot.Get(ctx, []byte("key"))
snapshot.Iterate(ctx, func(key []byte, val string) bool {
    // Process each key-value pair
    return true // Return false to stop iteration
})
```

### TTL Operations

```go
// Put with TTL
db.PutWithTTL(ctx, []byte("temp"), "value", 5*time.Minute)

// Get remaining TTL
ttl, exists := db.GetTTL(ctx, []byte("temp"))

// Extend TTL
db.ExtendTTL(ctx, []byte("temp"), 1*time.Minute)

// Remove TTL
db.RemoveTTL(ctx, []byte("temp"))
```

### Atomic Operations

```go
numDB := lfdb.New[[]byte, int]()

// Add to existing value
newVal, _ := numDB.Add(ctx, []byte("counter"), 5)

// Increment/decrement
newVal, _ = numDB.Increment(ctx, []byte("counter"))
newVal, _ = numDB.Decrement(ctx, []byte("counter"))

// Multiply/divide
newVal, _ = numDB.Multiply(ctx, []byte("counter"), 2)
newVal, _ = numDB.Divide(ctx, []byte("counter"), 2)
```

### Batch Operations

```go
// Batch put
keys := [][]byte{[]byte("key1"), []byte("key2")}
values := []string{"value1", "value2"}
err := db.BatchPut(ctx, keys, values)

// Batch get
results := db.BatchGet(ctx, keys)
for i, result := range results {
    fmt.Printf("Key: %s, Value: %s, Exists: %t\n", 
        string(keys[i]), result.Value, result.Exists)
}

// Batch delete
err = db.BatchDelete(ctx, keys)
```

### Convenience Types

```go
// Pre-defined types for common use cases
stringDB := lfdb.NewStringDB()
intDB := lfdb.NewIntDB()
int64DB := lfdb.NewInt64DB()
float64DB := lfdb.NewFloat64DB()
bytesDB := lfdb.NewBytesDB()
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
