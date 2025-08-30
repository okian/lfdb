# Licensed under the MIT License. See LICENSE file in the project root for details.

.PHONY: test bench race lint clean

# Default target
all: test

# Run tests
test:
	go test -race -v -p=1 ./...

# Run benchmarks
bench:
	go test -race -bench=. -benchmem -p=1 ./...

# Run tests with race detector
race:
	go test -race -v -p=1 ./...

# Run tests with coverage
test-coverage:
	go test -race -v -coverprofile=coverage.out -covermode=atomic -p=1 ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

# Run fuzz tests
fuzz:
	go test -fuzz=FuzzBasicOperations -fuzztime=20s ./tests/...
	go test -fuzz=FuzzConcurrentOperations -fuzztime=20s ./tests/...
	go test -fuzz=FuzzSnapshotOperations -fuzztime=20s ./tests/...
	go test -fuzz=FuzzTransactionOperations -fuzztime=20s ./tests/...
	go test -fuzz=FuzzRandomSequence -fuzztime=20s ./tests/...
	go test -fuzz=FuzzEpochReclamation -fuzztime=20s ./tests/...

# Run property-based tests
property:
	go test -v -run TestPropertyBased ./...

# Run linearizability tests
linearizability:
	go test -v -run TestLinearizability ./...

# Run race detection tests
race-detection:
	go test -race -v -run TestRaceDetection ./...

# Run GC tests
gc:
	go test -v -run TestGC ./...

# Run all comprehensive tests
test-all: test race fuzz property linearizability race-detection gc

# Run all tests (comprehensive)
all-tests: test race fuzz property linearizability race-detection gc bench

# Run throughput tests
throughput:
	./scripts/run_throughput_tests.sh basic

throughput-optimized:
	go run ./cmd/throughput_optimized -duration=30s -goroutines=8 -batch-size=100

throughput-all:
	./scripts/run_throughput_tests.sh all

throughput-realtime:
	./scripts/run_throughput_tests.sh realtime

# Run all basic tests and benchmarks
run-all: test race bench

# Show help
help:
	@echo "Available commands:"
	@echo "  test          - Run all tests"
	@echo "  race          - Run tests with race detector"
	@echo "  fuzz          - Run fuzz tests"
	@echo "  property      - Run property-based tests"
	@echo "  linearizability - Run linearizability tests"
	@echo "  race-detection - Run race detection tests"
	@echo "  gc            - Run garbage collection tests"
	@echo "  bench         - Run benchmarks"
	@echo "  test-all      - Run all comprehensive tests"
	@echo "  all-tests     - Run all tests including benchmarks"
	@echo "  run-all       - Run basic tests and benchmarks"
	@echo "  throughput    - Run basic throughput test"
	@echo "  throughput-optimized - Run optimized throughput test"
	@echo "  throughput-all - Run all throughput tests"
	@echo "  throughput-realtime - Run real-time throughput test"
	@echo "  lint          - Run linter"
	@echo "  check         - Run lint and tests"
	@echo "  build         - Build all binaries"
	@echo "  clean         - Clean build artifacts"

# Run linter
lint:
	golangci-lint run

# Clean build artifacts
clean:
	go clean -cache -testcache
	rm -rf dist/

# Install dependencies
deps:
	go mod download
	go mod tidy

# Run all checks
check: lint test race

# Build all binaries
build:
	go build ./cmd/...
	go build -o bin/throughput ./cmd/throughput
	go build -o bin/throughput-optimized ./cmd/throughput_optimized

# Install tools
tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
