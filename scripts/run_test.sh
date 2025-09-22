#!/bin/bash

# KV Store Comprehensive Test Suite
# Tests basic operations and consistency of the KV store server
# Verifies FFI interface and Rust workspace functionality
# Tests both Thrift server with C++ FFI bindings and Rust workspace tests

set -e
set -o pipefail

# Configuration
THRIFT_SERVER_PORT=${THRIFT_SERVER_PORT:-9097}  # Default to 9097, can be overridden with env var
SERVER_STARTUP_TIMEOUT=10
TEST_DATA_PREFIX="test_"
WITH_RSML=${WITH_RSML:-true}  # Flag for RSML feature testing - enabled by default for manual testing

# Ensure we're running from the scripts directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test tracking
TESTS_PASSED=0
TESTS_FAILED=0

# Test directories
TEST_WORK_DIR=""
TEST_SERVER_LOG=""
TEST_DB_DIR=""

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    ((++TESTS_PASSED))
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    ((++TESTS_FAILED))
}

log_test() {
    echo -e "${YELLOW}[TEST]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test environment..."

    if [ ! -z "$THRIFT_SERVER_PID" ]; then
        log_info "Stopping Thrift server (PID: $THRIFT_SERVER_PID)"
        kill $THRIFT_SERVER_PID 2>/dev/null || true
        wait $THRIFT_SERVER_PID 2>/dev/null || true
    fi

    # Note: FFI tests handle their own cleanup, no manual cleanup needed

    if [ -n "$TEST_WORK_DIR" ] && [ -d "$TEST_WORK_DIR" ]; then
        log_info "Removing test directory $TEST_WORK_DIR"
        rm -rf "$TEST_WORK_DIR"
    fi
}

# Set up cleanup trap
trap cleanup EXIT

# Check if required binaries exist
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if [ ! -f "../build/bin/cpp_ffi_test" ]; then
        log_error "FFI test binary not found. Please run 'cmake --build build' first from the project root."
        exit 1
    fi

    if [ ! -f "../rust/target/debug/thrift-server" ] && [ ! -f "../rust/target/release/thrift-server" ]; then
        log_error "Thrift server binary not found. Please run 'cargo build --bin thrift-server' first from the rust directory."
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Prepare dedicated directory for test artifacts
setup_test_environment() {
    log_info "Setting up dedicated test directory..."

    if ! TEST_WORK_DIR=$(mktemp -d /tmp/kv_tests_XXXXXX); then
        log_error "Failed to create temporary test directory"
        exit 1
    fi

    TEST_SERVER_LOG="$TEST_WORK_DIR/thrift_server.log"
    TEST_DB_DIR="$TEST_WORK_DIR/db"

    mkdir -p "$TEST_DB_DIR"

    log_info "Test artifacts will be stored in $TEST_WORK_DIR"
}

# Start Thrift server
start_server() {
    log_info "Starting Thrift server on port $THRIFT_SERVER_PORT..."

    # Clean up any existing server process
    pkill -f "thrift-server" 2>/dev/null || true
    sleep 2

    # Find the thrift server binary (prefer release, fallback to debug)
    local thrift_server_bin
    if [ -f "../rust/target/release/thrift-server" ]; then
        thrift_server_bin="../rust/target/release/thrift-server"
    else
        thrift_server_bin="../rust/target/debug/thrift-server"
    fi

    # Start the server in background with custom port
    $thrift_server_bin --port $THRIFT_SERVER_PORT --db-path "$TEST_DB_DIR" > "$TEST_SERVER_LOG" 2>&1 &
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
    if [ -f "$TEST_SERVER_LOG" ]; then
        cat "$TEST_SERVER_LOG"
    fi
    exit 1
}

# Test FFI interface
test_ffi_interface() {
    log_test "Testing C++ FFI interface"

    log_info "Configuring FFI tests to use server port $THRIFT_SERVER_PORT"

    # Run the FFI tests with the correct server configuration
    if KV_TEST_SERVER_PORT="$THRIFT_SERVER_PORT" ../build/bin/cpp_ffi_test; then
        log_success "FFI tests completed successfully"
        return 0
    else
        log_error "FFI tests failed (see output above for details)"
        return 1
    fi
}

# Test Rust workspace
test_rust_workspace() {
    log_test "Testing Rust workspace"

    if [ "$WITH_RSML" = true ]; then
        log_info "Running cargo test --workspace with RSML features in rust/ directory"
        log_warn "RSML testing requires consensus-rsml to be added to workspace temporarily"

        # Check if consensus-rsml crate exists and is buildable
        if [ ! -d "../rust/crates/consensus-rsml" ]; then
            log_error "RSML feature requested but consensus-rsml crate not found"
            log_error "Please ensure the private RSML submodule is properly initialized"
            return 1
        fi

        # Check if RSML can be compiled by testing feature availability
        if ! (cd ../rust && cargo check --features rsml --quiet 2>/dev/null); then
            log_error "RSML feature requested but consensus-rsml cannot be compiled"
            log_error "Please ensure the private RSML submodule is properly initialized and up to date"
            return 1
        fi

        # Change to rust directory and run tests with RSML features
        if (cd ../rust && KV_TEST_SERVER_PORT="$THRIFT_SERVER_PORT" cargo test --workspace --features rsml); then
            log_success "Rust workspace tests with RSML completed successfully"
            return 0
        else
            log_error "Rust workspace tests with RSML failed (see output above for details)"
            return 1
        fi
    else
        log_info "Running cargo test --workspace in rust/ directory"

        # Change to rust directory and run tests with correct server port
        if (cd ../rust && KV_TEST_SERVER_PORT="$THRIFT_SERVER_PORT" cargo test --workspace); then
            log_success "Rust workspace tests completed successfully"
            return 0
        else
            log_error "Rust workspace tests failed (see output above for details)"
            return 1
        fi
    fi
}

# Run all tests
run_all_tests() {
    log_info "Starting comprehensive test suite..."
    echo

    # Initialize counters
    TESTS_PASSED=0
    TESTS_FAILED=0

    # Run FFI tests
    test_ffi_interface

    echo

    # Run Rust workspace tests
    test_rust_workspace

    echo
    log_info "=== Test Results ==="
    log_info "Tests passed: $TESTS_PASSED"
    if [ $TESTS_FAILED -gt 0 ]; then
        log_error "Tests failed: $TESTS_FAILED"
        return 1
    else
        log_success "All tests passed successfully!"
        return 0
    fi
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --no-rsml)
                WITH_RSML=false
                log_info "RSML feature testing disabled via --no-rsml flag"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Main execution
main() {
    echo
    log_info "KV Store Functional Test Suite"
    log_info "=============================="

    if [ "$WITH_RSML" = true ]; then
        log_info "RSML feature testing: ENABLED"
    else
        log_info "RSML feature testing: DISABLED"
    fi
    echo

    check_prerequisites
    setup_test_environment
    start_server

    echo
    if run_all_tests; then
        echo
        log_success "Functional test suite completed successfully!"
        exit 0
    else
        echo
        log_error "Functional test suite failed!"
        exit 1
    fi
}

# Show usage help
show_help() {
    echo "KV Store Test Suite"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --no-rsml       Disable RSML testing"
    echo "  -h, --help      Show this help"
    echo
    echo "Environment variables:"
    echo "  THRIFT_SERVER_PORT: Server port (default: 9097)"
    echo "  WITH_RSML: Enable/disable RSML testing (default: true)"
}

# Parse arguments and execute main function
parse_arguments "$@"
main
