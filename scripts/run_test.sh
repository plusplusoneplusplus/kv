#!/bin/bash

# KV Store Comprehensive Test Suite
# Tests basic operations and consistency of the KV store server
# Verifies FFI interface and Rust workspace functionality
# Tests both shard node with C++ FFI bindings and Rust workspace tests

set -e
set -o pipefail

# Configuration
SHARD_SERVER_PORT=${SHARD_SERVER_PORT:-9097}  # Default to 9097, can be overridden with env var
CONSENSUS_BASE_PORT=${CONSENSUS_BASE_PORT:-19200}  # Base port for consensus servers
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

# Server PIDs for cleanup
SHARD_SERVER_PID=""
CONSENSUS_SERVER_PIDS=()

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

    if [ -n "$SHARD_SERVER_PID" ]; then
        log_info "Stopping shard server (PID: $SHARD_SERVER_PID)"
        kill "$SHARD_SERVER_PID" 2>/dev/null || true
        wait "$SHARD_SERVER_PID" 2>/dev/null || true
    fi

    # Stop consensus servers
    for pid in "${CONSENSUS_SERVER_PIDS[@]}"; do
        if [ -n "$pid" ]; then
            log_info "Stopping consensus server (PID: $pid)"
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done

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

    if [ ! -f "../rust/target/debug/shard-server" ] && [ ! -f "../rust/target/release/shard-server" ]; then
        log_error "Shard server binary not found. Please run 'cargo build --bin shard-server' first from the rust directory."
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

    TEST_SERVER_LOG="$TEST_WORK_DIR/shard_server.log"
    TEST_DB_DIR="$TEST_WORK_DIR/db"

    mkdir -p "$TEST_DB_DIR"

    log_info "Test artifacts will be stored in $TEST_WORK_DIR"
}

# Start shard server
start_server() {
    log_info "Starting shard server on port $SHARD_SERVER_PORT..."

    # Clean up any existing server process
    pkill -f "shard-server" 2>/dev/null || true
    sleep 2

    # Find the shard server binary (prefer release, fallback to debug)
    local shard_server_bin
    if [ -f "../rust/target/release/shard-server" ]; then
        shard_server_bin="../rust/target/release/shard-server"
    else
        shard_server_bin="../rust/target/debug/shard-server"
    fi

    # Start the server in background with custom port
    "$shard_server_bin" --port "$SHARD_SERVER_PORT" --db-path "$TEST_DB_DIR" > "$TEST_SERVER_LOG" 2>&1 &
    SHARD_SERVER_PID=$!

    log_info "Shard server started with PID: $SHARD_SERVER_PID"

    # Wait for server to be ready
    log_info "Waiting for shard server to be ready..."
    for i in $(seq 1 "$SERVER_STARTUP_TIMEOUT"); do
        if nc -z localhost "$SHARD_SERVER_PORT" 2>/dev/null; then
            log_success "Shard server is ready"
            return 0
        fi
        sleep 1
    done

    log_error "Shard server failed to start within $SERVER_STARTUP_TIMEOUT seconds"
    if [ -f "$TEST_SERVER_LOG" ]; then
        cat "$TEST_SERVER_LOG"
    fi
    exit 1
}

# Start consensus shard servers for consensus-mock integration tests
start_consensus_servers() {
    log_info "Starting consensus shard servers for integration tests..."

    # Clean up any existing consensus server processes
    pkill -f "mock-consensus-server" 2>/dev/null || true
    sleep 2

    # Check if we have a consensus server binary (for future implementation)
    # For now, the tests expect servers to be started but handle connection failures gracefully
    log_info "Consensus servers configured for ports ${CONSENSUS_BASE_PORT}+ (tests handle missing servers gracefully)"

    # TODO: When implementing actual consensus shard nodes:
    # 1. Build consensus server binaries from consensus-mock crate
    # 2. Start multiple servers on sequential ports (CONSENSUS_BASE_PORT, CONSENSUS_BASE_PORT+1, etc.)
    # 3. Track their PIDs in CONSENSUS_SERVER_PIDS array for cleanup
    # 4. Wait for servers to be ready with port checks

    # Example implementation template:
    # for i in {0..2}; do
    #     local port=$((CONSENSUS_BASE_PORT + i))
    #     local log_file="$TEST_WORK_DIR/consensus_server_${i}.log"
    #
    #     # Start consensus server
    #     consensus-server --node-id "node-${i}" --port "${port}" > "${log_file}" 2>&1 &
    #     local server_pid=$!
    #     CONSENSUS_SERVER_PIDS+=("$server_pid")
    #
    #     log_info "Started consensus server node-${i} on port ${port} with PID: ${server_pid}"
    # done

    log_success "Consensus server environment prepared (tests handle server absence gracefully)"
}

# Test FFI interface
test_ffi_interface() {
    log_test "Testing C++ FFI interface"

    log_info "Configuring FFI tests to use server port $SHARD_SERVER_PORT"

    # Run the FFI tests with the correct server configuration
    if KV_TEST_SERVER_PORT="$SHARD_SERVER_PORT" ../build/bin/cpp_ffi_test; then
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

    # Change to rust directory and run tests with correct server ports
    # Set environment variables for both regular and consensus servers
    if (cd ../rust && KV_TEST_SERVER_PORT="$SHARD_SERVER_PORT" CONSENSUS_BASE_PORT="$CONSENSUS_BASE_PORT" cargo test --workspace); then
        log_success "Rust workspace tests completed successfully"
        return 0
    else
        log_error "Rust workspace tests failed (see output above for details)"
        return 1
    fi
}

# Test consensus integration specifically
test_consensus_integration() {
    log_test "Testing consensus-mock integration tests"
    log_info "Running consensus-mock specific tests with server environment"

    # Change to rust directory and run only consensus-mock tests
    if (cd ../rust && KV_TEST_SERVER_PORT="$SHARD_SERVER_PORT" CONSENSUS_BASE_PORT="$CONSENSUS_BASE_PORT" cargo test --package consensus-mock); then
        log_success "Consensus integration tests completed successfully"
        return 0
    else
        log_error "Consensus integration tests failed (see output above for details)"
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

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
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

    echo

    check_prerequisites
    setup_test_environment
    start_server
    start_consensus_servers

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
    echo "KV Store Functional Test Suite"
    echo
    echo "This script runs comprehensive tests to verify the KV store:"
    echo "  - C++ FFI bindings functionality"
    echo "  - Shard server integration"
    echo "  - Basic KV operations through FFI"
    echo "  - Rust workspace tests (cargo test --workspace)"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  -h, --help      Show this help message"
    echo
    echo "The script will:"
    echo "  1. Start a shard server instance on port $SHARD_SERVER_PORT"
    echo "  2. Run C++ FFI tests"
    echo "  3. Run Rust workspace tests"
    echo "  4. Clean up automatically"
    echo "  5. Report test results and exit with appropriate code"
    echo
    echo "Environment variables:"
    echo "  SHARD_SERVER_PORT: Main server port (default: 9097)"
    echo "    FFI tests will be automatically configured to use this port"
    echo "    Default 9097 avoids conflicts with production workloads on 9090"
    echo "  CONSENSUS_BASE_PORT: Base port for consensus servers (default: 19200)"
    echo "    Consensus tests use sequential ports starting from this base"
    echo
    echo "Prerequisites:"
    echo "  - Run 'cmake --build build' from project root to build FFI tests"
    echo "  - Run 'cargo build --bin shard-server' from rust/ directory"
    echo "  - Ensure port $SHARD_SERVER_PORT is available"
    echo "  - Ensure ports ${CONSENSUS_BASE_PORT}+ are available for consensus servers"
    echo "  - Rust toolchain for workspace tests (cargo test)"
}

# Parse arguments and execute main function
parse_arguments "$@"
main
