#!/bin/bash

# Comprehensive KV Store Benchmark Suite
# Tests multiple protocols (raw, grpc, thrift) with different workload patterns
# Generates JSON results and provides aggregated analysis

set -e

# Configuration
BENCHMARK_REQUESTS=500000
BENCHMARK_ITERATIONS=3  # Number of times to run each configuration
# Thread configurations (will iterate over these)
RAW_THREAD_CONFIGS=(1 2 4 8)
GRPC_THREAD_CONFIGS=(32 64 128 256)
THRIFT_THREAD_CONFIGS=(32 64 128 256)

# Minimum benchmark mode configuration
MIN_BENCHMARK_REQUESTS=10000
MIN_BENCHMARK_THREADS=8
MIN_BENCHMARK_ITERATIONS=1

# Client type filtering (empty means all clients)
CLIENT_TYPES=""

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULTS_BASE_DIR="../benchmark_results"
GRPC_SERVER_PORT=50051
THRIFT_SERVER_PORT=9090
SERVER_STARTUP_TIMEOUT=10

# Ensure we're running from the scripts directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    if [ ! -z "$GRPC_SERVER_PID" ]; then
        log_info "Stopping gRPC server (PID: $GRPC_SERVER_PID)"
        kill $GRPC_SERVER_PID 2>/dev/null || true
        wait $GRPC_SERVER_PID 2>/dev/null || true
    fi
    if [ ! -z "$THRIFT_SERVER_PID" ]; then
        log_info "Stopping Thrift server (PID: $THRIFT_SERVER_PID)"
        kill $THRIFT_SERVER_PID 2>/dev/null || true
        wait $THRIFT_SERVER_PID 2>/dev/null || true
    fi
}

# Set up cleanup trap
trap cleanup EXIT

# Check if required binaries exist
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if [ ! -f "../bin/benchmark" ]; then
        log_error "Benchmark binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    if [ ! -f "../bin/rocksdbserver-rust" ]; then
        log_error "gRPC server binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    if [ ! -f "../bin/rocksdbserver-thrift" ]; then
        log_error "Thrift server binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Start Thrift server
start_thrift_server() {
    log_info "Starting Thrift server on port $THRIFT_SERVER_PORT..."
    
    # Clean up any existing server process
    pkill -f "rocksdbserver-thrift" 2>/dev/null || true
    sleep 2
    
    # Start the server in background
    ../bin/rocksdbserver-thrift > thrift_server.log 2>&1 &
    THRIFT_SERVER_PID=$!
    
    log_info "Thrift server started with PID: $THRIFT_SERVER_PID"
    
    # Wait for server to be ready
    log_info "Waiting for Thrift server to be ready..."
    for i in $(seq 1 $SERVER_STARTUP_TIMEOUT); do
        if nc -z localhost $THRIFT_SERVER_PORT 2>/dev/null; then
            log_success "Thrift server is ready"
            return 0
        fi
        sleep 1
    done
    
    log_error "Thrift server failed to start within $SERVER_STARTUP_TIMEOUT seconds"
    cat thrift_server.log
    exit 1
}

# Stop Thrift server
stop_thrift_server() {
    if [ ! -z "$THRIFT_SERVER_PID" ]; then
        log_info "Stopping Thrift server (PID: $THRIFT_SERVER_PID)"
        kill $THRIFT_SERVER_PID 2>/dev/null || true
        wait $THRIFT_SERVER_PID 2>/dev/null || true
        THRIFT_SERVER_PID=""
        sleep 2
    fi
}

# Start gRPC server
start_grpc_server() {
    log_info "Starting gRPC server on port $GRPC_SERVER_PORT..."
    
    # Clean up any existing server process
    pkill -f "rocksdbserver-rust" 2>/dev/null || true
    sleep 2
    
    # Start the server in background
    ../bin/rocksdbserver-rust > grpc_server.log 2>&1 &
    GRPC_SERVER_PID=$!
    
    log_info "gRPC server started with PID: $GRPC_SERVER_PID"
    
    # Wait for server to be ready
    log_info "Waiting for gRPC server to be ready..."
    for i in $(seq 1 $SERVER_STARTUP_TIMEOUT); do
        if nc -z localhost $GRPC_SERVER_PORT 2>/dev/null; then
            log_success "gRPC server is ready"
            return 0
        fi
        sleep 1
    done
    
    log_error "gRPC server failed to start within $SERVER_STARTUP_TIMEOUT seconds"
    cat grpc_server.log
    exit 1
}

# Stop gRPC server
stop_grpc_server() {
    if [ ! -z "$GRPC_SERVER_PID" ]; then
        log_info "Stopping gRPC server (PID: $GRPC_SERVER_PID)"
        kill $GRPC_SERVER_PID 2>/dev/null || true
        wait $GRPC_SERVER_PID 2>/dev/null || true
        GRPC_SERVER_PID=""
        sleep 2
    fi
}

# Run a single benchmark with multiple iterations and average the results
run_benchmark() {
    local protocol=$1
    local mode=$2
    local write_pct=$3
    local output_file=$4
    local threads=$5
    
    local cmd_args=""
    case $mode in
        "read")
            cmd_args="-mode=read -prepopulate=10000"
            ;;
        "write")
            cmd_args="-mode=write"
            ;;
        "mixed")
            cmd_args="-mode=mixed -write-pct=$write_pct"
            ;;
    esac
    
    log_info "Running benchmark: Protocol=$protocol, Mode=$mode, Write%=$write_pct, Threads=$threads (${BENCHMARK_ITERATIONS} iterations)"
    
    # Create temporary files for each iteration
    local temp_files=()
    for i in $(seq 1 $BENCHMARK_ITERATIONS); do
        local temp_file="${output_file%.json}_iter${i}.json"
        temp_files+=("$temp_file")
        
        log_info "Running iteration $i/$BENCHMARK_ITERATIONS..."
        ../bin/benchmark \
            -protocol=$protocol \
            $cmd_args \
            -requests=$BENCHMARK_REQUESTS \
            -threads=$threads \
            -json="$temp_file" \
            > /dev/null 2>&1
        
        if [ $? -ne 0 ]; then
            log_error "Benchmark iteration $i failed for $protocol $mode"
            # Clean up temp files
            for temp_file in "${temp_files[@]}"; do
                rm -f "$temp_file"
            done
            exit 1
        fi
    done
    
    # Keep iteration files for generate report script to average automatically
    # The main output file will be the first iteration (generate report will handle averaging)
    cp "${temp_files[0]}" "$output_file"
    
    # Keep temp files with iteration naming for generate report script
    # No cleanup - the generate report script will detect and average them
    
    log_success "Benchmark completed ($BENCHMARK_ITERATIONS iterations): $output_file"
}

# Create results directory
setup_results_dir() {
    log_info "Setting up results directory: $RESULTS_DIR"
    mkdir -p "$RESULTS_BASE_DIR"
    mkdir -p "$RESULTS_DIR"
    log_info "Results will be saved in: $RESULTS_DIR"
}

# Check if a client type should be run
should_run_client() {
    local client_type=$1
    if [ -z "$CLIENT_TYPES" ]; then
        return 0  # Run all if no filter specified
    fi
    
    echo "$CLIENT_TYPES" | grep -q "$client_type"
    return $?
}

# Run minimum benchmarks (fast mode)
run_min_benchmarks() {
    log_info "Starting minimum benchmark suite (fast mode)..."
    echo
    
    local requests=$MIN_BENCHMARK_REQUESTS
    local threads=$MIN_BENCHMARK_THREADS
    local iterations=$MIN_BENCHMARK_ITERATIONS
    
    # Override global settings temporarily
    local original_requests=$BENCHMARK_REQUESTS
    local original_iterations=$BENCHMARK_ITERATIONS
    BENCHMARK_REQUESTS=$requests
    BENCHMARK_ITERATIONS=$iterations
    
    log_info "Configuration: $requests requests, $threads threads, $iterations iteration(s)"
    if [ ! -z "$CLIENT_TYPES" ]; then
        log_info "Client types: $CLIENT_TYPES"
    fi
    echo
    
    # Run raw benchmarks (no server needed)
    if should_run_client "raw"; then
        log_info "=== Running RAW Protocol Benchmarks (Minimal) ==="
        log_info "Running RAW benchmark with $threads threads"
        run_benchmark "raw" "mixed" "10" "$RESULTS_DIR/raw_mixed_90_10_${threads}t.json" "$threads"
        echo
    fi
    
    # Run gRPC benchmarks (requires server)
    if should_run_client "grpc"; then
        log_info "=== Running gRPC Protocol Benchmarks (Minimal) ==="
        
        start_grpc_server
        log_info "Running gRPC benchmark with $threads threads"
        run_benchmark "grpc" "mixed" "10" "$RESULTS_DIR/grpc_mixed_90_10_${threads}t.json" "$threads"
        stop_grpc_server
        echo
    fi
    
    # Run Thrift benchmarks (requires server)
    if should_run_client "thrift"; then
        log_info "=== Running Thrift Protocol Benchmarks (Minimal) ==="
        
        start_thrift_server
        log_info "Running Thrift benchmark with $threads threads"
        run_benchmark "thrift" "mixed" "10" "$RESULTS_DIR/thrift_mixed_90_10_${threads}t.json" "$threads"
        stop_thrift_server
        echo
    fi
    
    # Restore global settings
    BENCHMARK_REQUESTS=$original_requests
    BENCHMARK_ITERATIONS=$original_iterations
    
    log_success "Minimum benchmarks completed successfully"
}

# Run all benchmarks
run_all_benchmarks() {
    log_info "Starting comprehensive benchmark suite..."
    echo
    
    if [ ! -z "$CLIENT_TYPES" ]; then
        log_info "Client types: $CLIENT_TYPES"
        echo
    fi
    
    # Run raw benchmarks (no server needed)
    if should_run_client "raw"; then
        log_info "=== Running RAW Protocol Benchmarks ==="
        
        for threads in "${RAW_THREAD_CONFIGS[@]}"; do
            log_info "Running RAW benchmarks with $threads threads"
            run_benchmark "raw" "read" "0" "$RESULTS_DIR/raw_read_100_${threads}t.json" "$threads"
            run_benchmark "raw" "write" "100" "$RESULTS_DIR/raw_write_100_${threads}t.json" "$threads"
            run_benchmark "raw" "mixed" "10" "$RESULTS_DIR/raw_mixed_90_10_${threads}t.json" "$threads"
        done
        
        echo
    fi
    
    # Run gRPC benchmarks (requires server)
    if should_run_client "grpc"; then
        log_info "=== Running gRPC Protocol Benchmarks ==="
        
        start_grpc_server
        
        for threads in "${GRPC_THREAD_CONFIGS[@]}"; do
            log_info "Running gRPC benchmarks with $threads threads"
            run_benchmark "grpc" "read" "0" "$RESULTS_DIR/grpc_read_100_${threads}t.json" "$threads"
            run_benchmark "grpc" "write" "100" "$RESULTS_DIR/grpc_write_100_${threads}t.json" "$threads"
            run_benchmark "grpc" "mixed" "10" "$RESULTS_DIR/grpc_mixed_90_10_${threads}t.json" "$threads"
        done
        
        stop_grpc_server
        
        echo
    fi
    
    # Run Thrift benchmarks (requires server)
    if should_run_client "thrift"; then
        log_info "=== Running Thrift Protocol Benchmarks ==="
        
        start_thrift_server
        
        for threads in "${THRIFT_THREAD_CONFIGS[@]}"; do
            log_info "Running Thrift benchmarks with $threads threads"
            run_benchmark "thrift" "read" "0" "$RESULTS_DIR/thrift_read_100_${threads}t.json" "$threads"
            run_benchmark "thrift" "write" "100" "$RESULTS_DIR/thrift_write_100_${threads}t.json" "$threads"
            run_benchmark "thrift" "mixed" "10" "$RESULTS_DIR/thrift_mixed_90_10_${threads}t.json" "$threads"
        done
        
        stop_thrift_server
        
        echo
    fi
    
    log_success "All benchmarks completed successfully"
}

# Generate aggregated analysis and HTML report
generate_analysis() {
    log_info "Generating aggregated analysis and HTML report..."
    
    # Use standalone Python script for both console analysis and HTML report
    python3 generate_benchmark_report.py "$RESULTS_DIR" --console
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --min|-m)
                MIN_MODE=true
                shift
                ;;
            --client|-c)
                if [[ $# -lt 2 ]]; then
                    log_error "Option --client requires a value"
                    exit 1
                fi
                CLIENT_TYPES="$2"
                shift 2
                ;;
            --help|-h)
                # Help is handled before main() is called
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

# Main execution
main() {
    echo
    
    # Parse arguments
    parse_args "$@"
    
    local suite_type="Comprehensive"
    local run_function="run_all_benchmarks"
    local results_suffix=""
    
    # Check for minimum benchmark mode
    if [[ "$MIN_MODE" == "true" ]]; then
        suite_type="Minimum (Fast)"
        run_function="run_min_benchmarks"
        results_suffix="_minimum"
    fi
    
    # Add client suffix if filtering
    if [ ! -z "$CLIENT_TYPES" ]; then
        local client_suffix=$(echo "$CLIENT_TYPES" | tr ',' '_')
        results_suffix="${results_suffix}_${client_suffix}"
    fi
    
    # Set results directory with appropriate suffix
    RESULTS_DIR="$RESULTS_BASE_DIR/run_$TIMESTAMP$results_suffix"
    
    log_info "KV Store $suite_type Benchmark Suite"
    log_info "========================================"
    log_info "Run ID: $TIMESTAMP"
    if [[ "$suite_type" == "Comprehensive" ]]; then
        log_info "Iterations per configuration: $BENCHMARK_ITERATIONS"
        log_info "Requests per test: $BENCHMARK_REQUESTS"
    else
        log_info "Requests per test: $MIN_BENCHMARK_REQUESTS"
        log_info "Threads: $MIN_BENCHMARK_THREADS"
        log_info "Iterations: $MIN_BENCHMARK_ITERATIONS"
    fi
    echo
    
    check_prerequisites
    setup_results_dir
    $run_function
    
    echo
    generate_analysis
    
    echo
    log_success "Benchmark suite completed successfully!"
    log_info "Results saved in: $RESULTS_DIR"
    log_info "HTML Report: $RESULTS_DIR/benchmark_report.html"
    log_info "Run ID: $TIMESTAMP"
}

# Show usage if help requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "KV Store Benchmark Suite"
    echo
    echo "This script runs benchmarks across protocols: Raw RocksDB, gRPC, Thrift"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --min, -m                Run minimum benchmark suite (fast mode)"
    echo "                           • 10K requests, 8 threads, 1 iteration"
    echo "                           • Only mixed workload (90% read, 10% write)"
    echo "                           • All three protocols (unless --client specified)"
    echo
    echo "  --client, -c TYPE[,TYPE] Run only specified client types"
    echo "                           • TYPE can be: raw, grpc, thrift"
    echo "                           • Examples: --client raw, --client grpc,thrift"
    echo
    echo "  -h, --help               Show this help message"
    echo
    echo "Default (comprehensive) mode:"
    echo "  Requests per test: $BENCHMARK_REQUESTS"
    echo "  Iterations per configuration: $BENCHMARK_ITERATIONS"
    echo "  Raw protocol threads: ${RAW_THREAD_CONFIGS[*]}"
    echo "  gRPC protocol threads: ${GRPC_THREAD_CONFIGS[*]}"
    echo "  Thrift protocol threads: ${THRIFT_THREAD_CONFIGS[*]}"
    echo "  Workloads: 100% Read, 100% Write, 90% Read + 10% Write"
    echo
    echo "Minimum mode configuration:"
    echo "  Requests per test: $MIN_BENCHMARK_REQUESTS"
    echo "  Threads: $MIN_BENCHMARK_THREADS"
    echo "  Iterations: $MIN_BENCHMARK_ITERATIONS"
    echo "  Workload: 90% Read + 10% Write only"
    echo
    echo "Results:"
    echo "  Base directory: $RESULTS_BASE_DIR"
    echo "  Each run creates timestamped subdirectory: run_YYYYMMDD_HHMMSS"
    echo "  HTML report generated automatically"
    echo
    echo "Examples:"
    echo "  $0                      # Run full comprehensive benchmark suite"
    echo "  $0 --min                # Run quick benchmark for testing (recommended for development)"
    echo "  $0 --client raw         # Run only raw protocol benchmarks"
    echo "  $0 --client grpc,thrift # Run only gRPC and Thrift benchmarks"
    echo "  $0 --min --client grpc  # Run minimum benchmark only for gRPC"
    exit 0
fi

# Execute main function
main "$@"