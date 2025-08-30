#!/bin/bash
# Licensed under the MIT License. See LICENSE file in the project root for details.
# This script runs both basic and real-time throughput measurements

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Database Throughput Testing Suite${NC}"
echo "=================================="

# Create output directory
OUTPUT_DIR="throughput-reports"
mkdir -p "$OUTPUT_DIR"

# Function to run basic throughput test
run_basic_test() {
    echo -e "\n${YELLOW}Running Basic Throughput Test...${NC}"
    echo "Duration: 30 seconds"
    echo "Read ratio: 80%"
    echo "Goroutines: 8"
    
    go run ./cmd/throughput/main.go \
        -duration=30s \
        -interval=1s \
        -read-ratio=0.8 \
        -goroutines=8 \
        -db-size=100000 \
        -output="$OUTPUT_DIR/basic-test"
    
    echo -e "${GREEN}Basic test completed!${NC}"
}

# Function to run real-time throughput test
run_realtime_test() {
    echo -e "\n${YELLOW}Running Real-Time Throughput Test...${NC}"
    echo "Duration: 60 seconds"
    echo "Read ratio: 80%"
    echo "Goroutines: 16"
    echo "Warmup: 5 seconds"
    
    go run ./cmd/throughput/real_time.go \
        -duration=60s \
        -interval=1s \
        -read-ratio=0.8 \
        -goroutines=16 \
        -db-size=100000 \
        -warmup=5s \
        -cooldown=2s \
        -output="$OUTPUT_DIR/realtime-test"
    
    echo -e "${GREEN}Real-time test completed!${NC}"
}

# Function to run read-heavy workload test
run_read_heavy_test() {
    echo -e "\n${YELLOW}Running Read-Heavy Workload Test...${NC}"
    echo "Duration: 45 seconds"
    echo "Read ratio: 95%"
    echo "Goroutines: 32"
    
    go run ./cmd/throughput/real_time.go \
        -duration=45s \
        -interval=1s \
        -read-ratio=0.95 \
        -goroutines=32 \
        -db-size=500000 \
        -warmup=3s \
        -cooldown=1s \
        -output="$OUTPUT_DIR/read-heavy-test"
    
    echo -e "${GREEN}Read-heavy test completed!${NC}"
}

# Function to run write-heavy workload test
run_write_heavy_test() {
    echo -e "\n${YELLOW}Running Write-Heavy Workload Test...${NC}"
    echo "Duration: 45 seconds"
    echo "Read ratio: 20%"
    echo "Goroutines: 16"
    
    go run ./cmd/throughput/real_time.go \
        -duration=45s \
        -interval=1s \
        -read-ratio=0.2 \
        -goroutines=16 \
        -db-size=100000 \
        -warmup=3s \
        -cooldown=1s \
        -output="$OUTPUT_DIR/write-heavy-test"
    
    echo -e "${GREEN}Write-heavy test completed!${NC}"
}

# Function to run high-concurrency test
run_high_concurrency_test() {
    echo -e "\n${YELLOW}Running High-Concurrency Test...${NC}"
    echo "Duration: 60 seconds"
    echo "Read ratio: 70%"
    echo "Goroutines: 64"
    
    go run ./cmd/throughput/real_time.go \
        -duration=60s \
        -interval=1s \
        -read-ratio=0.7 \
        -goroutines=64 \
        -db-size=200000 \
        -warmup=5s \
        -cooldown=2s \
        -output="$OUTPUT_DIR/high-concurrency-test"
    
    echo -e "${GREEN}High-concurrency test completed!${NC}"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [OPTION]"
    echo ""
    echo "Options:"
    echo "  basic           Run basic throughput test (30s, 8 goroutines, 80% reads)"
    echo "  realtime        Run real-time throughput test (60s, 16 goroutines, 80% reads)"
    echo "  read-heavy      Run read-heavy workload test (95% reads, 32 goroutines)"
    echo "  write-heavy     Run write-heavy workload test (20% reads, 16 goroutines)"
    echo "  high-conc       Run high-concurrency test (64 goroutines)"
    echo "  all             Run all tests"
    echo "  help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 basic"
    echo "  $0 realtime"
    echo "  $0 all"
}

# Main script logic
case "${1:-help}" in
    "basic")
        run_basic_test
        ;;
    "realtime")
        run_realtime_test
        ;;
    "read-heavy")
        run_read_heavy_test
        ;;
    "write-heavy")
        run_write_heavy_test
        ;;
    "high-conc")
        run_high_concurrency_test
        ;;
    "all")
        echo -e "${BLUE}Running all throughput tests...${NC}"
        run_basic_test
        run_realtime_test
        run_read_heavy_test
        run_write_heavy_test
        run_high_concurrency_test
        echo -e "\n${GREEN}All tests completed!${NC}"
        ;;
    "help"|*)
        show_usage
        exit 1
        ;;
esac

echo -e "\n${BLUE}Test Results:${NC}"
echo "Reports generated in: $OUTPUT_DIR"
echo ""
echo "Available reports:"
find "$OUTPUT_DIR" -name "*.html" -o -name "*.csv" | sort | while read file; do
    echo "  - $file"
done

echo -e "\n${GREEN}To view HTML reports, open them in your web browser:${NC}"
echo "  open $OUTPUT_DIR/*.html"
