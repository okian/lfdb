# String-based API Example

This example demonstrates the new string-based API for the LFDB library, which provides a much better developer experience while maintaining the performance benefits of the underlying `[]byte` implementation.

## Why String Keys?

The original LFDB API uses `[]byte` for keys, which provides maximum performance but requires verbose syntax:

```go
// Original []byte API
db.Put(ctx, []byte("key"), "value")
val, exists := db.Get(ctx, []byte("key"))
```

The new string-based API provides a much cleaner interface:

```go
// New string API (recommended)
db.Put(ctx, "key", "value")
val, exists := db.Get(ctx, "key")
```

## Benefits

### 1. **Better Developer Experience**
- **Cleaner syntax**: No need for `[]byte()` conversions
- **More readable**: String literals are self-documenting
- **Type safety**: Prevents accidental passing of non-string data
- **Easier debugging**: String keys are easier to inspect and log

### 2. **Minimal Performance Overhead**
- **~1-2% overhead**: Due to string-to-[]byte conversion
- **Same internal performance**: Uses the same optimized hash index
- **Lock-free operations**: Maintains all concurrency benefits

### 3. **Backward Compatibility**
- **Original API preserved**: `[]byte` API still available for maximum performance
- **Gradual migration**: Can adopt string API incrementally
- **Same functionality**: All features available in both APIs

## Usage Examples

### Basic Operations
```go
db := lfdb.NewStringDB[string]()
defer db.Close(ctx)

// Clean, readable syntax
db.Put(ctx, "user:123", "John Doe")
db.Put(ctx, "config:timeout", "30s")

val, exists := db.Get(ctx, "user:123")
deleted := db.Delete(ctx, "config:timeout")
```

### Transactions
```go
err := db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
    tx.Put(ctx, "session:abc123", "active")
    tx.Put(ctx, "session:def456", "pending")
    return nil
})
```

### Snapshots
```go
snapshot := db.Snapshot(ctx)
defer snapshot.Close(ctx)

val, exists := snapshot.Get(ctx, "session:abc123")
snapshot.Iterate(ctx, func(key string, val string) bool {
    fmt.Printf("%s: %s\n", key, val)
    return true
})
```

### Atomic Operations
```go
numDB := lfdb.NewStringDB[int]()
newVal, _ := numDB.Add(ctx, "counter", 5)
newVal, _ = numDB.Increment(ctx, "counter")
```

### TTL Operations
```go
db.PutWithTTL(ctx, "temp:session", "expires-soon", 5*time.Minute)
ttl, exists := db.GetTTL(ctx, "temp:session")
```

### Batch Operations
```go
batch := lfdb.NewStringBatch[string]()
batch.Put("key1", "value1")
batch.Put("key2", "value2")
db.ExecuteBatch(ctx, batch)
```

## Performance Comparison

The example includes a performance comparison that shows:
- String API overhead is typically 1-2%
- Both APIs use the same optimized internal implementation
- Lock-free performance is maintained

## When to Use Each API

### Use String API When:
- Building new applications
- Developer experience is a priority
- Performance overhead is acceptable
- Working with human-readable keys

### Use []byte API When:
- Maximum performance is critical
- Working with binary keys
- Zero-copy operations are needed
- Migrating existing code

## Running the Example

```bash
cd examples/string_api
go run main.go
```

This will demonstrate all the features of the string-based API and show a performance comparison with the original []byte API.
