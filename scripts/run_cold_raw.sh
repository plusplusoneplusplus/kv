#!/bin/bash

# Cold Raw Benchmark Suite - Comprehensive cold page analysis
# Tests multiple thread counts, read/write ratios, and dataset sizes
# Generates JSON results and provides aggregated HTML report

set -euo pipefail

# Configuration
BENCHMARK_REQUESTS=50000
BENCHMARK_ITERATIONS=2  # Number of times to run each configuration
# Thread configurations
THREAD_CONFIGS=(1 2 4 8 16)
# Read/Write ratio configurations (write percentages)
WRITE_RATIOS=(0 10)  # 100% read, 90:10 mixed
# Dataset size configurations (in GB)
DATASET_SIZES=(2)

# Minimum benchmark mode configuration
MIN_BENCHMARK_REQUESTS=5000
MIN_BENCHMARK_THREADS=4
MIN_BENCHMARK_ITERATIONS=1
MIN_DATASET_SIZE=2

TIMESTAMP=$(date '+%Y%m%d_%H%M%S')
RESULTS_BASE_DIR="../benchmark_results"
VALUE_SIZE=8192  # 8KB values for large datasets

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

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

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if [ ! -f "$ROOT_DIR/bin/benchmark-rust" ]; then
        log_error "Benchmark binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    if [ ! -f "$ROOT_DIR/configs/db/cold_block_cache.toml" ]; then
        log_error "Cold block cache config not found: $ROOT_DIR/configs/db/cold_block_cache.toml"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Create results directory
setup_results_dir() {
    log_info "Setting up results directory: $RESULTS_DIR"
    mkdir -p "$RESULTS_BASE_DIR"
    mkdir -p "$RESULTS_DIR"
    log_info "Results will be saved in: $RESULTS_DIR"
}

# Create or reuse dataset
setup_dataset() {
    local size_gb=$1
    local dataset_dir="$ROOT_DIR/data/cold-dataset-${size_gb}gb"
    local keys_needed=$((size_gb * 1024 * 1024 / 8))  # GB -> KB / 8KB per key
    
    if [ -d "$dataset_dir" ]; then
        local actual_size=$(du -sh "$dataset_dir" | cut -f1)
        log_info "Reusing existing dataset: $dataset_dir ($actual_size)"
        echo "$dataset_dir"
        return
    fi
    
    log_info "Creating ${size_gb}GB dataset at $dataset_dir..."
    log_info "Keys needed: $keys_needed (this will take several minutes)"
    
    "$ROOT_DIR/bin/benchmark-rust" \
        --protocol=raw \
        --addr="$dataset_dir" \
        --mode=write \
        --threads=4 \
        --requests=$keys_needed \
        --key-size=32 \
        --value-size=$VALUE_SIZE \
        --prepopulate=0 \
        > /dev/null 2>&1
    
    local actual_size=$(du -sh "$dataset_dir" | cut -f1)
    log_success "Dataset created: $dataset_dir ($actual_size)"
    echo "$dataset_dir"
}

# Run a single cold benchmark with multiple iterations
run_cold_benchmark() {
    local dataset_dir=$1
    local threads=$2
    local write_pct=$3
    local size_gb=$4
    local output_file=$5
    
    local mode="read"
    local keys_in_dataset=$((size_gb * 1024 * 1024 / 8))
    
    if [ $write_pct -gt 0 ]; then
        mode="mixed"
    fi
    
    log_info "Running cold benchmark: Dataset=${size_gb}GB, Threads=$threads, Write%=$write_pct (${BENCHMARK_ITERATIONS} iterations)"
    
    # Create temporary files for each iteration
    local temp_files=()
    for i in $(seq 1 $BENCHMARK_ITERATIONS); do
        local temp_file="${output_file%.json}_iter${i}.json"
        temp_files+=("$temp_file")
        
        log_info "Running iteration $i/$BENCHMARK_ITERATIONS..."
        
        local cmd_args="--mode=$mode"
        if [ $write_pct -gt 0 ]; then
            cmd_args="$cmd_args --write-pct=$write_pct"
        fi
        
        "$ROOT_DIR/bin/benchmark-rust" \
            --protocol=raw \
            --addr="$dataset_dir" \
            $cmd_args \
            --threads=$threads \
            --requests=$BENCHMARK_REQUESTS \
            --key-size=32 \
            --value-size=$VALUE_SIZE \
            --prepopulate=$keys_in_dataset \
            --no-cache-read \
            --config="$ROOT_DIR/configs/db/cold_block_cache.toml" \
            --json="$temp_file" \
            > /dev/null 2>&1
        
        if [ $? -ne 0 ]; then
            log_error "Cold benchmark iteration $i failed"
            for temp_file in "${temp_files[@]}"; do
                rm -f "$temp_file"
            done
            exit 1
        fi
    done
    
    # Keep iteration files for report generation (averaging)
    cp "${temp_files[0]}" "$output_file"
    
    log_success "Cold benchmark completed ($BENCHMARK_ITERATIONS iterations): $output_file"
}

# Run minimum benchmarks (fast mode)
run_min_benchmarks() {
    log_info "Starting minimum cold benchmark suite (fast mode)..."
    echo
    
    local requests=$MIN_BENCHMARK_REQUESTS
    local threads=$MIN_BENCHMARK_THREADS
    local iterations=$MIN_BENCHMARK_ITERATIONS
    local dataset_size=$MIN_DATASET_SIZE
    
    # Override global settings temporarily
    local original_requests=$BENCHMARK_REQUESTS
    local original_iterations=$BENCHMARK_ITERATIONS
    BENCHMARK_REQUESTS=$requests
    BENCHMARK_ITERATIONS=$iterations
    
    log_info "Configuration: $requests requests, $threads threads, ${dataset_size}GB dataset, $iterations iteration(s)"
    echo
    
    # Create dataset
    local dataset_dir=$(setup_dataset $dataset_size)
    
    # Run single configuration: mixed 90:10 workload
    log_info "=== Running Minimum Cold Benchmark ==="
    run_cold_benchmark "$dataset_dir" "$threads" "10" "$dataset_size" "$RESULTS_DIR/cold_${dataset_size}gb_mixed_90_10_${threads}t.json"
    
    # Restore global settings
    BENCHMARK_REQUESTS=$original_requests
    BENCHMARK_ITERATIONS=$original_iterations
    
    log_success "Minimum cold benchmarks completed successfully"
}

# Run all benchmarks
run_all_benchmarks() {
    log_info "Starting comprehensive cold benchmark suite..."
    echo
    
    # Create all required datasets first
    local datasets=()
    for size_gb in "${DATASET_SIZES[@]}"; do
        local dataset_dir=$(setup_dataset $size_gb)
        datasets+=("$dataset_dir")
    done
    
    echo
    
    # Run benchmarks for each configuration
    local benchmark_count=0
    local total_benchmarks=$((${#DATASET_SIZES[@]} * ${#THREAD_CONFIGS[@]} * ${#WRITE_RATIOS[@]}))
    
    for size_idx in "${!DATASET_SIZES[@]}"; do
        local size_gb=${DATASET_SIZES[$size_idx]}
        local dataset_dir=${datasets[$size_idx]}
        
        log_info "=== Running Cold Benchmarks for ${size_gb}GB Dataset ==="
        
        for threads in "${THREAD_CONFIGS[@]}"; do
            for write_pct in "${WRITE_RATIOS[@]}"; do
                benchmark_count=$((benchmark_count + 1))
                
                local workload_name
                if [ $write_pct -eq 0 ]; then
                    workload_name="read_100"
                else
                    local read_pct=$((100 - write_pct))
                    workload_name="mixed_${read_pct}_${write_pct}"
                fi
                
                local output_file="$RESULTS_DIR/cold_${size_gb}gb_${workload_name}_${threads}t.json"
                
                log_info "Progress: $benchmark_count/$total_benchmarks"
                run_cold_benchmark "$dataset_dir" "$threads" "$write_pct" "$size_gb" "$output_file"
                echo
            done
        done
    done
    
    log_success "All cold benchmarks completed successfully"
}

# Generate aggregated analysis and HTML report
generate_analysis() {
    log_info "Generating aggregated analysis and HTML report..."
    
    # Create simple analysis script if generate_benchmark_report.py doesn't exist
    if [ ! -f "$SCRIPT_DIR/generate_benchmark_report.py" ]; then
        log_warn "generate_benchmark_report.py not found, creating basic HTML report..."
        create_basic_html_report
    else
        python3 "$SCRIPT_DIR/generate_benchmark_report.py" "$RESULTS_DIR" --console
    fi
}

# Create basic HTML report if main generator is not available
create_basic_html_report() {
    local report_file="$RESULTS_DIR/cold_benchmark_report.html"
    
    cat > "$report_file" << 'EOF'
<!DOCTYPE html>
<html>
<head>
    <title>Cold Page Benchmark Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1, h2 { color: #2c3e50; }
        .summary { background: #f8f9fa; padding: 20px; border-left: 4px solid #3498db; margin: 20px 0; }
        .config { background: #e8f4f8; padding: 15px; border-radius: 5px; margin: 10px 0; }
        .metrics { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin: 20px 0; }
        .metric { background: #fff; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }
        .metric h4 { margin: 0 0 10px 0; color: #2c3e50; }
        .metric .value { font-size: 24px; font-weight: bold; color: #3498db; }
        .metric .unit { font-size: 14px; color: #7f8c8d; }
        table { width: 100%; border-collapse: collapse; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background: #f2f2f2; font-weight: bold; }
        .cold { color: #e74c3c; font-weight: bold; }
        .warm { color: #f39c12; font-weight: bold; }
        .hot { color: #27ae60; font-weight: bold; }
    </style>
</head>
<body>
    <h1>Cold Page Benchmark Report</h1>
    
    <div class="summary">
        <h2>Summary</h2>
        <p>This report analyzes cold page read performance across different configurations.</p>
        <p>Cold reads bypass caches and require disk I/O, resulting in higher latencies (50μs - 1ms+)</p>
    </div>
    
    <div class="config">
        <h3>Test Configuration</h3>
        <ul>
            <li><strong>Dataset Size:</strong> 2GB</li>
            <li><strong>Thread Configs:</strong> 1, 2, 4, 8, 16</li>
            <li><strong>Workload Ratios:</strong> 100% Read, 90:10 Read/Write</li>
            <li><strong>Value Size:</strong> 8KB (creates large datasets quickly)</li>
            <li><strong>Cache Configuration:</strong> Tiny 1MB block cache with no-cache-read enabled</li>
        </ul>
    </div>
    
    <h2>Results Analysis</h2>
    <p>Individual test results are available in JSON format in the results directory.</p>
    <p>Look for these latency patterns:</p>
    <ul>
        <li><span class="hot">< 10μs:</span> Hot (data cached)</li>
        <li><span class="warm">10μs - 100μs:</span> Warm (partial cache misses)</li>
        <li><span class="cold">> 100μs:</span> Cold (true disk I/O)</li>
    </ul>
    
    <div class="summary">
        <h3>Key Insights</h3>
        <ul>
            <li>Larger datasets (10GB+) force more cache misses</li>
            <li>Higher thread counts can increase contention and latency</li>
            <li>Mixed workloads show different patterns than pure reads</li>
            <li>P99 latencies reveal true cold behavior better than averages</li>
        </ul>
    </div>
    
    <h2>Files Generated</h2>
    <p>Check the results directory for:</p>
    <ul>
        <li>JSON result files: <code>cold_[size]gb_[workload]_[threads]t.json</code></li>
        <li>Iteration files: <code>cold_[size]gb_[workload]_[threads]t_iter[N].json</code></li>
        <li>This HTML report: <code>cold_benchmark_report.html</code></li>
    </ul>
    
    <p><em>Generated on: $(date)</em></p>
</body>
</html>
EOF
    
    log_success "Basic HTML report created: $report_file"
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --min|-m)
                MIN_MODE=true
                shift
                ;;
            --help|-h)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
}

# Show help
show_help() {
    echo "Cold Page Benchmark Suite"
    echo
    echo "This script runs comprehensive cold page benchmarks with multiple configurations"
    echo "and generates detailed analysis reports."
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --min, -m                Run minimum benchmark suite (fast mode)"
    echo "                           • $MIN_BENCHMARK_REQUESTS requests, $MIN_BENCHMARK_THREADS threads, ${MIN_DATASET_SIZE}GB dataset"
    echo "                           • $MIN_BENCHMARK_ITERATIONS iteration, mixed 90:10 workload only"
    echo
    echo "  -h, --help               Show this help message"
    echo
    echo "Default (comprehensive) mode:"
    echo "  Requests per test: $BENCHMARK_REQUESTS"
    echo "  Iterations per configuration: $BENCHMARK_ITERATIONS"
    echo "  Thread configurations: ${THREAD_CONFIGS[*]}"
    echo "  Write ratios: ${WRITE_RATIOS[*]}% (0% = pure read)"
    echo "  Dataset size: ${DATASET_SIZES[*]}GB"
    echo "  Value size: ${VALUE_SIZE} bytes"
    echo
    echo "Features:"
    echo "  • Creates large datasets to force cache misses"
    echo "  • Uses tiny RocksDB block cache (1MB) + no-cache-read"
    echo "  • Multiple iterations with result averaging"
    echo "  • Comprehensive HTML report generation"
    echo
    echo "Expected latency ranges:"
    echo "  • Hot (cached): <10μs"
    echo "  • Warm (partial cache miss): 10-100μs"  
    echo "  • Cold (true disk I/O): >100μs"
    echo
    echo "Examples:"
    echo "  $0                      # Run full comprehensive cold benchmark suite"
    echo "  $0 --min                # Run quick test (recommended for development)"
}

# Main execution
main() {
    echo
    
    # Initialize variables
    MIN_MODE=false
    
    # Parse arguments
    parse_args "$@"
    
    local suite_type="Comprehensive Cold Page"
    local run_function="run_all_benchmarks"
    local results_suffix="_cold"
    
    # Check for minimum benchmark mode
    if [[ "$MIN_MODE" == "true" ]]; then
        suite_type="Minimum Cold Page (Fast)"
        run_function="run_min_benchmarks"
        results_suffix="_cold_minimum"
    fi
    
    # Set results directory
    RESULTS_DIR="$RESULTS_BASE_DIR/run_$TIMESTAMP$results_suffix"
    
    log_info "$suite_type Benchmark Suite"
    log_info "========================================"
    log_info "Run ID: $TIMESTAMP"
    if [[ "$suite_type" == "Comprehensive Cold Page" ]]; then
        log_info "Iterations per configuration: $BENCHMARK_ITERATIONS"
        log_info "Requests per test: $BENCHMARK_REQUESTS"
        log_info "Dataset sizes: ${DATASET_SIZES[*]}GB"
        log_info "Thread configs: ${THREAD_CONFIGS[*]}"
        log_info "Write ratios: ${WRITE_RATIOS[*]}%"
    else
        log_info "Requests per test: $MIN_BENCHMARK_REQUESTS"
        log_info "Dataset size: ${MIN_DATASET_SIZE}GB"
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
    log_success "Cold benchmark suite completed successfully!"
    log_info "Results saved in: $RESULTS_DIR"
    log_info "HTML Report: $RESULTS_DIR/cold_benchmark_report.html"
    log_info "Run ID: $TIMESTAMP"
    echo
    log_info "Cleanup datasets with: rm -rf $ROOT_DIR/data/cold-dataset-*"
}

# Execute main function
main "$@"