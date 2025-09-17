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

    log_info "Running cargo test --workspace in rust/ directory"

    # Change to rust directory and run tests with correct server port
    if (cd ../rust && KV_TEST_SERVER_PORT="$THRIFT_SERVER_PORT" cargo test --workspace); then
        log_success "Rust workspace tests completed successfully"
        return 0
    else
        log_error "Rust workspace tests failed (see output above for details)"
        return 1
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

# Main execution
main() {
    echo
    log_info "KV Store Functional Test Suite"
    log_info "=============================="
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

# Show usage if help requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "KV Store Functional Test Suite"
    echo
    echo "This script runs comprehensive tests to verify the KV store:"
    echo "  - C++ FFI bindings functionality"
    echo "  - Thrift server integration"
    echo "  - Basic KV operations through FFI"
    echo "  - Rust workspace tests (cargo test --workspace)"
    echo
    echo "Usage: $0"
    echo
    echo "The script will:"
    echo "  1. Start a Thrift server instance on port $THRIFT_SERVER_PORT"
    echo "  2. Run C++ FFI tests"
    echo "  3. Run Rust workspace tests (cargo test --workspace)"
    echo "  4. Clean up automatically"
    echo "  5. Report test results and exit with appropriate code"
    echo
    echo "Environment variables:"
    echo "  THRIFT_SERVER_PORT: Server port (default: 9097)"
    echo "    FFI tests will be automatically configured to use this port"
    echo "    Default 9097 avoids conflicts with production workloads on 9090"
    echo
    echo "Prerequisites:"
    echo "  - Run 'cmake --build build' from project root to build FFI tests"
    echo "  - Run 'cargo build --bin thrift-server' from rust/ directory"
    echo "  - Ensure port $THRIFT_SERVER_PORT is available"
    echo "  - Rust toolchain for workspace tests (cargo test)"
    exit 0
fi

# Execute main function
main "$@"
