# Contributing to LFDB

Thank you for your interest in contributing to LFDB! This document provides guidelines for contributing to the project.

## Project Structure

LFDB follows Go best practices with a clear separation of concerns:

```
lfdb/
├── cmd/                    # Application entry points
│   ├── bench/             # Benchmarking tool
│   ├── repl/              # Interactive REPL
│   ├── throughput/        # Throughput testing tool
│   └── throughput_optimized/ # Optimized throughput testing
├── internal/              # Private application code
│   ├── core/              # Core database functionality
│   ├── storage/           # Storage layer
│   │   ├── index/         # Index implementations
│   │   └── mvcc/          # MVCC implementation
│   ├── concurrency/       # Concurrency primitives
│   └── monitoring/        # Observability
├── tests/                 # Integration and property tests
├── docs/                  # Documentation
├── examples/              # Usage examples
└── scripts/               # Build and deployment scripts
```

## Development Setup

### Prerequisites
- Go 1.24 or later
- Git

### Getting Started
```bash
# Clone the repository
git clone https://github.com/kianostad/lfdb.git
cd lfdb

# Install dependencies
go mod download

# Run tests
make test

# Build all binaries
make build
```

## Development Workflow

### 1. Create a Feature Branch
```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes
Follow these guidelines:
- Write clear, readable code
- Add tests for new functionality
- Update documentation as needed
- Follow Go coding conventions

### 3. Run Tests
```bash
# Run all tests
make test

# Run tests with race detection
make race

# Run benchmarks
make bench

# Run property tests
make property
```

### 4. Code Quality Checks
```bash
# Run linter
make lint

# Run all checks (lint + test + race)
make check
```

### 5. Commit Your Changes
```bash
git add .
git commit -m "feat: add your feature description"
```

### 6. Push and Create Pull Request
```bash
git push origin feature/your-feature-name
```

## Coding Standards

### Go Conventions
- Follow [Effective Go](https://golang.org/doc/effective_go.html)
- Use `gofmt` for code formatting
- Follow [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)

### Package Organization
- **`internal/core/`**: Main database interface and implementation
- **`internal/storage/`**: Data storage and indexing
- **`internal/concurrency/`**: Thread-safe primitives
- **`internal/monitoring/`**: Observability and metrics

### Testing Guidelines
- Write unit tests for all public APIs
- Include property-based tests for complex logic
- Test concurrent scenarios with race detection
- Aim for >95% test coverage

### Documentation
- Document all public APIs
- Include usage examples
- Update README.md for user-facing changes
- Update ARCHITECTURE.md for architectural changes

## Testing Strategy

### Unit Tests
```bash
# Run unit tests for a specific package
go test ./internal/core/

# Run tests with coverage
go test -cover ./internal/core/
```

### Integration Tests
```bash
# Run all integration tests
go test ./tests/
```

### Property Tests
```bash
# Run property-based tests
go test -tags=property ./tests/
```

### Performance Tests
```bash
# Run benchmarks
go test -bench=. ./internal/core/

# Run throughput tests
make throughput
```

## Performance Considerations

### Lock-Free Design
- All operations must be lock-free
- Use atomic operations for shared state
- Implement epoch-based reclamation for memory safety

### Memory Management
- Use object pooling to reduce allocations
- Implement efficient garbage collection
- Monitor memory usage in benchmarks

### Concurrency
- Test with multiple goroutines
- Verify linearizability under concurrent access
- Measure performance under contention

## Common Issues and Solutions

### Import Path Issues
If you encounter import path errors after restructuring:
```bash
# Update all import paths
find . -name "*.go" -exec sed -i 's|github.com/kianostad/lfdb/pkg/db|github.com/kianostad/lfdb/internal/core|g' {} \;
```

### Test Failures
- Ensure all imports are updated to new structure
- Check that test files are in the correct location
- Verify that package names match directory structure

### Build Issues
- Run `go mod tidy` to clean up dependencies
- Check that all required packages are imported
- Verify Go version compatibility

## Release Process

### Versioning
- Follow [Semantic Versioning](https://semver.org/)
- Update version in `go.mod` and documentation
- Create release notes for significant changes

### Release Checklist
- [ ] All tests pass
- [ ] Benchmarks show no regressions
- [ ] Documentation is updated
- [ ] Release notes are prepared
- [ ] Version is tagged

## Getting Help

- **Issues**: Create an issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions
- **Code Review**: Request review for pull requests

## License

By contributing to LFDB, you agree that your contributions will be licensed under the same license as the project.
