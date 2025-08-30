# LFDB Architecture

## Overview

LFDB (Lock-Free Database) is a high-performance, in-memory database built with Go that provides lock-free operations, MVCC (Multi-Version Concurrency Control), and snapshot isolation.

## Architecture Principles

### 1. Lock-Free Design
- All operations are lock-free using atomic operations and compare-and-swap (CAS) primitives
- No blocking or waiting for locks, ensuring maximum concurrency
- Uses epoch-based reclamation for safe memory management

### 2. MVCC (Multi-Version Concurrency Control)
- Each write creates a new version of the data
- Readers can access consistent snapshots without blocking writers
- Automatic garbage collection of obsolete versions

### 3. Snapshot Isolation
- Consistent read views across concurrent operations
- Transactions see a consistent state of the database
- No dirty reads or non-repeatable reads

## Component Architecture

### Core Layer (`internal/core/`)
The core layer provides the main database interface and implementation:

- **Database Interface**: Defines the public API for database operations
- **Snapshot Management**: Handles consistent read views
- **Transaction Processing**: Manages ACID transactions
- **Batch Operations**: Efficient bulk operations

### Storage Layer (`internal/storage/`)
The storage layer handles data persistence and indexing:

#### Index (`internal/storage/index/`)
- **Hash Index**: Fast key-value lookups using hash tables
- **Optimized Iterators**: Efficient iteration over data
- **SIMD Optimizations**: Vectorized operations for bulk processing

#### MVCC (`internal/storage/mvcc/`)
- **Entry Management**: Handles versioned data entries
- **Garbage Collection**: Cleans up obsolete versions
- **Object Pooling**: Reduces memory allocations

### Concurrency Layer (`internal/concurrency/`)
The concurrency layer provides thread-safe primitives:

#### Epoch Management (`internal/concurrency/epoch/`)
- **Epoch-Based Reclamation**: Safe memory management without locks
- **Thread Registration**: Tracks active threads for cleanup

### Monitoring Layer (`internal/monitoring/`)
The monitoring layer provides observability:

#### Metrics (`internal/monitoring/metrics/`)
- **Performance Metrics**: Tracks operation latencies and throughput
- **Memory Usage**: Monitors memory consumption
- **Active Snapshots**: Tracks concurrent read operations

## Data Flow

### Write Operations
1. **Validation**: Check transaction constraints
2. **Version Creation**: Create new MVCC entry with timestamp
3. **Index Update**: Update hash index with new entry
4. **Commit**: Atomically update global timestamp

### Read Operations
1. **Snapshot Creation**: Capture current timestamp
2. **Index Lookup**: Find entry in hash index
3. **Version Resolution**: Get appropriate version for timestamp
4. **Return**: Return value and existence flag

### Garbage Collection
1. **Epoch Tracking**: Monitor active epochs
2. **Version Scanning**: Identify obsolete versions
3. **Safe Deletion**: Remove versions when no active readers
4. **Memory Reclamation**: Return memory to pool

## Performance Characteristics

### Scalability
- **Horizontal**: No shared state between instances
- **Vertical**: Scales with CPU cores due to lock-free design
- **Memory**: Efficient memory usage with object pooling

### Latency
- **Reads**: O(1) average case with hash index
- **Writes**: O(1) average case with atomic operations
- **Snapshots**: O(1) creation, O(n) iteration

### Throughput
- **Concurrent Reads**: Unlimited due to MVCC
- **Concurrent Writes**: Limited only by memory bandwidth
- **Mixed Workloads**: Optimized for read-heavy scenarios

## Design Decisions

### Why Lock-Free?
- **Maximum Concurrency**: No blocking or waiting
- **Predictable Performance**: No lock contention
- **Scalability**: Scales with hardware threads

### Why MVCC?
- **Consistency**: Readers never see inconsistent state
- **Performance**: No read locks required
- **Isolation**: Snapshot isolation for transactions

### Why Hash Index?
- **Speed**: O(1) average case lookups
- **Simplicity**: Easy to implement and debug
- **Memory Efficiency**: Compact representation

### Why Epoch-Based Reclamation?
- **Safety**: No use-after-free bugs
- **Performance**: Minimal overhead
- **Simplicity**: Easier than reference counting

## Future Enhancements

### Planned Features
- **Persistence**: Disk-based storage with WAL
- **Compression**: Data compression for memory efficiency
- **Clustering**: Distributed database support
- **Advanced Indexes**: B-tree, LSM-tree support

### Performance Optimizations
- **Memory Mapping**: Zero-copy data access
- **SIMD Operations**: Vectorized bulk operations
- **Cache Optimization**: CPU cache-friendly data layout
- **NUMA Awareness**: Multi-socket optimization
