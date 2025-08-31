# LFDB — Lock-Free In-Memory Database for Go

High-performance, lock-free in-memory database with MVCC (multi-version concurrency control), snapshots, TTL, batches, atomic ops, and metrics — written in Go.

## Highlights

- Lock-free algorithms with snapshot isolation (MVCC)
- Epoch-based garbage collection and safe memory reclamation
- TTL and absolute expiry support
- Atomic ops for numeric types (add/inc/dec/mul/div)
- Batch puts/gets and snapshot iterators
- Metrics and observability hooks

## Install

```bash
go get github.com/kianostad/lfdb
```

- Requires Go 1.24+ (matches `go.mod` and CI matrix)

## Quick Start (String API, recommended)

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

    db := lfdb.NewStringDB[string]()
    defer db.Close(ctx)

    // CRUD
    db.Put(ctx, "hello", "world")
    if v, ok := db.Get(ctx, "hello"); ok { fmt.Println(v) }

    // Txn
    _ = db.Txn(ctx, func(tx lfdb.StringKeyTxn[string]) error {
        tx.Put(ctx, "k1", "v1")
        tx.Put(ctx, "k2", "v2")
        return nil
    })

    // Snapshot
    snap := db.Snapshot(ctx)
    defer snap.Close(ctx)
    snap.Iterate(ctx, func(k, v string) bool { fmt.Println(k, v); return true })

    // TTL
    db.PutWithTTL(ctx, "temp", "value", 5*time.Second)
    if ttl, ok := db.GetTTL(ctx, "temp"); ok { fmt.Println("TTL:", ttl) }

    // Atomic
    cdb := lfdb.NewStringDB[int]()
    cdb.Add(ctx, "counter", 5)
    cdb.Increment(ctx, "counter")
}
```

## Alternative API (raw []byte keys)

For maximum performance and zero-copy key handling:

```go
db := lfdb.New[[]byte, string]()
defer db.Close(context.Background())
db.Put(ctx, []byte("hello"), "world")
```

## Core Operations

- CRUD: `Put`, `Get`, `Delete`
- Transactions: `Txn(func(tx ...){ ... })` with `tx.Put`, `tx.Get`, `tx.Delete` and atomic/TTL helpers
- Snapshots: `Snapshot(ctx)` with `Get` and `Iterate`
- TTL: `PutWithTTL`, `PutWithExpiry`, `GetTTL`, `ExtendTTL`, `RemoveTTL`
- Atomic (numeric): `Add`, `Increment`, `Decrement`, `Multiply`, `Divide`
- Batch: `NewStringBatch` / `NewBatch`, `ExecuteBatch`, `BatchPut`, `BatchGet`, `BatchPutWithTTL`

See runnable examples in `examples/string_api/main.go` and `examples/basic_usage.go`.

## Metrics

- Access a snapshot of counters and gauges via `db.GetMetrics(ctx)`.
- A demo showing how to record and inspect metrics lives in `examples/enhanced_metrics_demo.go`.

## CLI Tools

- REPL: `go run cmd/repl/main.go`
- Benchmarks: `go run cmd/bench/main.go` or `make bench`

## Makefile Commands

Use `make help` for a full list. Common tasks:

- `make test`: Run tests with race detector
- `make bench`: Run benchmarks
- `make format`: Format with `goimports`
- `make lint`: Run `golangci-lint`
- `make lint-all`: Lint + security (`gosec`) + vuln check
- `make build`: Build all binaries in `cmd/`

Advanced test suites:

- `make fuzz` | `make property` | `make linearizability` | `make race-detection` | `make gc`
- `make test-all` to run everything; `make all-tests` to include benches

## Documentation

- Architecture: `docs/ARCHITECTURE.md`
- Contributing: `docs/CONTRIBUTING.md`
- Makefile overview: `docs/MAKEFILE.md`

## Best Practices

- Always `defer db.Close(ctx)` to release resources
- Use transactions for related writes; snapshots for consistent reads
- Prefer batch ops for large workloads
- Call `ManualGC(ctx)` periodically in long-running processes
- Keep keys reasonably small; use TTL for temporary data

## Testing and CI

- Local: `make test` or `go test ./...`
- CI: GitHub Actions with multi-arch, race, coverage, fuzz, property, and security scanning

## License

MIT — see `LICENSE`.
