#!/bin/bash

# Raw RocksDB Benchmark Wrapper
# Convenience script that runs the main benchmark suite with raw protocol only
# This provides dedicated raw benchmarking without duplicating infrastructure

set -e

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Show usage if help requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo -e "${BLUE}Raw RocksDB Dedicated Benchmark Wrapper${NC}"
    echo
    echo "This script provides convenient access to raw RocksDB benchmarking by calling"
    echo "the main benchmark suite with '--client raw' and optimized settings."
    echo
    echo -e "${GREEN}Usage:${NC} $0 [OPTIONS]"
    echo
    echo -e "${GREEN}Options:${NC}"
    echo "  --min, -m                Run minimum benchmark suite (fast mode)"
    echo "  -h, --help               Show this help message"
    echo
    echo -e "${GREEN}Features:${NC}"
    echo "  • Tests direct RocksDB performance (no network overhead)"
    echo "  • Uses same infrastructure as main benchmark suite"
    echo "  • Applies same configuration from db_config.toml"
    echo "  • Generates JSON results and HTML reports"
    echo "  • Clean result organization with 'raw_' prefix"
    echo
    echo -e "${GREEN}Examples:${NC}"
    echo "  $0                       # Run comprehensive raw benchmarks"
    echo "  $0 --min                 # Run quick raw benchmark test"
    echo
    echo -e "${GREEN}Equivalent Commands:${NC}"
    echo "  $0           ≡  ./benchmark_suite.sh --client raw"
    echo "  $0 --min     ≡  ./benchmark_suite.sh --client raw --min"
    echo
    echo -e "${YELLOW}Note:${NC} This wrapper calls benchmark_suite.sh --client raw"
    echo "For advanced options, use benchmark_suite.sh directly with --client raw"
    exit 0
fi

# Build arguments to pass to benchmark_suite.sh
ARGS=("--client" "raw")

# Parse arguments and pass them through
while [[ $# -gt 0 ]]; do
    case $1 in
        --min|-m)
            ARGS+=("--min")
            shift
            ;;
        *)
            # Pass through any other arguments
            ARGS+=("$1")
            shift
            ;;
    esac
done

# Print header
echo
echo -e "${BLUE}Raw RocksDB Dedicated Benchmark Suite${NC}"
echo -e "${BLUE}====================================${NC}"
echo "Calling benchmark_suite.sh --client raw with your options..."
echo

# Change to script directory and call the main benchmark suite
cd "$SCRIPT_DIR"
exec ./benchmark_suite.sh "${ARGS[@]}"