# Makefile Structure

This project uses a modular Makefile structure with automatic tool installation.

## File Structure

- `Makefile` - Main Makefile that includes project-specific commands
- `Makefile.generic` - Generic Go project commands (reusable across projects)
- `Makefile.project` - Project-specific commands for LFDB

## Generic Commands (from Makefile.generic)

These commands work for any Go project:

### Development Commands
- `make format` - Format code with goimports
- `make lint` - Run golangci-lint (auto-installs if missing)
- `make security` - Run gosec security scanner (auto-installs if missing)
- `make vulncheck` - Run vulnerability check (auto-installs if missing)
- `make lint-all` - Run all linting and security checks

### Testing Commands
- `make test` - Run all tests with race detection
- `make race` - Run tests with race detector
- `make bench` - Run benchmarks
- `make test-coverage` - Run tests with coverage report

### Build Commands
- `make build` - Build all binaries
- `make deps` - Install Go dependencies

### Utility Commands
- `make clean` - Clean build artifacts
- `make help` - Show available commands

## Project-Specific Commands (from Makefile.project)

These commands are specific to the LFDB database project:

### Advanced Testing
- `make fuzz` - Run fuzz tests
- `make property` - Run property-based tests
- `make linearizability` - Run linearizability tests
- `make race-detection` - Run race detection tests
- `make gc` - Run garbage collection tests

### Comprehensive Test Suites
- `make test-all` - Run all comprehensive tests
- `make all-tests` - Run all tests including benchmarks
- `make run-all` - Run basic tests and benchmarks

## Automatic Tool Installation

Each command automatically installs its required tools if they're missing:

- `make lint` → Installs `golangci-lint` if missing
- `make security` → Installs `gosec` if missing
- `make vulncheck` → Installs `govulncheck` if missing
- `make format` → Installs `goimports` if missing

## Usage Examples

```bash
# Format code
make format

# Run all tests
make test

# Run comprehensive test suite
make all-tests

# Run linting and security checks
make lint-all



# Show all available commands
make help
```

## Creating Your Own Project

To use this structure in another Go project:

1. Copy `Makefile.generic` to your project
2. Create your own `Makefile.project` with project-specific commands
3. Create a main `Makefile` that includes your project file:

```makefile
# Licensed under the MIT License. See LICENSE file in the project root for details.

# Include the project-specific Makefile
include Makefile.project
```

## Benefits

- **Modular**: Generic commands can be reused across projects
- **Automatic**: Each command installs its own tools when needed
- **Go-native**: No Python dependencies required
- **Comprehensive**: Covers testing, linting, security, and formatting
- **Fast**: Only installs missing tools
