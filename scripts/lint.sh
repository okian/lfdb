#!/bin/bash
# Licensed under the MIT License. See LICENSE file in the project root for details.

set -euo pipefail

echo "🔍 Running comprehensive linting and security checks..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Check if tools are installed
check_tool() {
    if ! command -v "$1" &> /dev/null; then
        print_error "$1 is not installed. Run 'make tools' to install all required tools."
        exit 1
    fi
}

# Check required tools
echo "Checking required tools..."
check_tool "golangci-lint"
check_tool "gosec"
check_tool "govulncheck"

# Run golangci-lint
echo "Running golangci-lint..."
if golangci-lint run --timeout=5m; then
    print_status "golangci-lint passed"
else
    print_error "golangci-lint failed"
    exit 1
fi

# Run gosec security scanner
echo "Running gosec security scanner..."
if gosec -fmt sarif -out results.sarif ./...; then
    print_status "gosec security scan completed"
    if [ -f results.sarif ]; then
        print_warning "Security scan results saved to results.sarif"
    fi
else
    print_error "gosec security scan failed"
    exit 1
fi

# Run vulnerability check
echo "Running vulnerability check..."
if govulncheck ./...; then
    print_status "vulnerability check passed"
else
    print_warning "vulnerability check found issues"
fi

# Run go vet
echo "Running go vet..."
if go vet ./...; then
    print_status "go vet passed"
else
    print_error "go vet failed"
    exit 1
fi

# Run go fmt check
echo "Checking code formatting..."
if [ -z "$(gofmt -l .)" ]; then
    print_status "code formatting is correct"
else
    print_error "code formatting issues found. Run 'go fmt ./...' to fix"
    exit 1
fi

# Run go mod tidy check
echo "Checking go.mod and go.sum..."
if go mod tidy -check; then
    print_status "go.mod and go.sum are up to date"
else
    print_warning "go.mod and go.sum need updating. Run 'go mod tidy' to fix"
fi

print_status "All linting and security checks completed successfully!"
