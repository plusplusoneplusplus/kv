#!/bin/bash

# KV Store Functional Test Suite
# Tests basic operations and consistency of the KV store server
# Verifies read-after-write, delete operations, and list functionality

set -e

# Configuration
GRPC_SERVER_PORT=50051
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
    
    if [ ! -z "$GRPC_SERVER_PID" ]; then
        log_info "Stopping gRPC server (PID: $GRPC_SERVER_PID)"
        kill $GRPC_SERVER_PID 2>/dev/null || true
        wait $GRPC_SERVER_PID 2>/dev/null || true
    fi
    
    # Clean up test data from database
    if [ -f "../bin/client" ]; then
        log_info "Cleaning up test data..."
        ../bin/client -op delete -key "${TEST_DATA_PREFIX}write_test" >/dev/null 2>&1 || true
        ../bin/client -op delete -key "${TEST_DATA_PREFIX}consistency_test" >/dev/null 2>&1 || true
        ../bin/client -op delete -key "${TEST_DATA_PREFIX}delete_test" >/dev/null 2>&1 || true
        for i in {1..5}; do
            ../bin/client -op delete -key "${TEST_DATA_PREFIX}list_item_$i" >/dev/null 2>&1 || true
        done
        ../bin/client -op delete -key "other_prefix_item" >/dev/null 2>&1 || true
    fi
}

# Set up cleanup trap
trap cleanup EXIT

# Check if required binaries exist
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if [ ! -f "../bin/client" ]; then
        log_error "Client binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    if [ ! -f "../bin/rocksdbserver-rust" ]; then
        log_error "gRPC server binary not found. Please run 'make build' first from the project root."
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Start gRPC server
start_server() {
    log_info "Starting gRPC server on port $GRPC_SERVER_PORT..."
    
    # Clean up any existing server process
    pkill -f "rocksdbserver-rust" 2>/dev/null || true
    sleep 2
    
    # Start the server in background
    ../bin/rocksdbserver-rust > test_server.log 2>&1 &
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
    cat test_server.log
    exit 1
}

# Test basic write operation
test_write_operation() {
    log_test "Testing basic write operation"
    
    local key="${TEST_DATA_PREFIX}write_test"
    local value="test_value_123"
    
    if ../bin/client -op put -key "$key" -value "$value" >/dev/null 2>&1; then
        log_success "Write operation completed successfully"
        return 0
    else
        log_error "Write operation failed"
        return 1
    fi
}

# Test basic read operation and read-after-write consistency
test_read_after_write() {
    log_test "Testing read-after-write consistency"
    
    local key="${TEST_DATA_PREFIX}consistency_test"
    local expected_value="consistency_test_value_456"
    
    # Write the value
    if ! ../bin/client -op put -key "$key" -value "$expected_value" >/dev/null 2>&1; then
        log_error "Failed to write test data for read-after-write test"
        return 1
    fi
    
    # Read the value back
    local actual_value
    actual_value=$(../bin/client -op get -key "$key" 2>/dev/null | grep "Value:" | cut -d' ' -f2- || echo "")
    
    if [ "$actual_value" = "$expected_value" ]; then
        log_success "Read-after-write consistency verified (key: $key, value: $expected_value)"
        return 0
    else
        log_error "Read-after-write consistency failed. Expected: '$expected_value', Got: '$actual_value'"
        return 1
    fi
}

# Test read operation for non-existent key
test_read_nonexistent_key() {
    log_test "Testing read operation for non-existent key"
    
    local key="${TEST_DATA_PREFIX}nonexistent_key_$(date +%s)"
    
    # Attempt to read non-existent key
    local output
    output=$(../bin/client -op get -key "$key" 2>&1 || true)
    
    if echo "$output" | grep -q "not found\|NotFound" >/dev/null 2>&1; then
        log_success "Non-existent key properly returned 'not found' error"
        return 0
    else
        log_error "Non-existent key did not return proper error. Output: $output"
        return 1
    fi
}

# Test delete operation
test_delete_operation() {
    log_test "Testing delete operation"
    
    local key="${TEST_DATA_PREFIX}delete_test"
    local value="value_to_be_deleted"
    
    # First, write a value
    if ! ../bin/client -op put -key "$key" -value "$value" >/dev/null 2>&1; then
        log_error "Failed to write test data for delete operation test"
        return 1
    fi
    
    # Verify it exists
    local check_value
    check_value=$(../bin/client -op get -key "$key" 2>/dev/null | grep "Value:" | cut -d' ' -f2- || echo "")
    if [ "$check_value" != "$value" ]; then
        log_error "Test data not properly written before delete test"
        return 1
    fi
    
    # Delete the key
    if ! ../bin/client -op delete -key "$key" >/dev/null 2>&1; then
        log_error "Delete operation failed"
        return 1
    fi
    
    # Verify it no longer exists
    local output
    output=$(../bin/client -op get -key "$key" 2>&1 || true)
    
    if echo "$output" | grep -q "not found\|NotFound" >/dev/null 2>&1; then
        log_success "Delete operation verified - key no longer exists"
        return 0
    else
        log_error "Delete operation failed - key still exists. Output: $output"
        return 1
    fi
}

# Test list operation with prefix
test_list_operation() {
    log_test "Testing list operation with prefix filtering"
    
    local test_prefix="${TEST_DATA_PREFIX}list_"
    
    # Create several test keys with the prefix
    for i in {1..5}; do
        local key="${test_prefix}item_$i"
        local value="list_test_value_$i"
        if ! ../bin/client -op put -key "$key" -value "$value" >/dev/null 2>&1; then
            log_error "Failed to create test data for list operation (key: $key)"
            return 1
        fi
    done
    
    # Create a key with different prefix
    if ! ../bin/client -op put -key "other_prefix_item" -value "other_value" >/dev/null 2>&1; then
        log_error "Failed to create control data for list operation"
        return 1
    fi
    
    # List keys with the test prefix
    local output
    output=$(../bin/client -op list -prefix "$test_prefix" -limit 10 2>/dev/null || echo "")
    
    # Count how many of our test keys appear in the output
    local found_count=0
    for i in {1..5}; do
        local key="${test_prefix}item_$i"
        if echo "$output" | grep -q "$key"; then
            ((found_count++))
        fi
    done
    
    # Check that our control key with different prefix doesn't appear
    local control_found=false
    if echo "$output" | grep -q "other_prefix_item"; then
        control_found=true
    fi
    
    if [ $found_count -eq 5 ] && [ "$control_found" = false ]; then
        log_success "List operation with prefix filtering working correctly (found $found_count/5 expected keys)"
        return 0
    else
        log_error "List operation failed. Found $found_count/5 expected keys, control key found: $control_found"
        echo "List output:"
        echo "$output"
        return 1
    fi
}

# Test list operation with limit
test_list_with_limit() {
    log_test "Testing list operation with limit parameter"
    
    local test_prefix="${TEST_DATA_PREFIX}list_"
    local limit=3
    
    # We already have 5 items from the previous test, so let's test limiting
    local output
    output=$(../bin/client -op list -prefix "$test_prefix" -limit $limit 2>/dev/null || echo "")
    
    # Count the number of keys returned
    local key_count
    # The client prints:
    #   Found N keys:
    #     <key1>
    #     <key2>
    # So count non-empty lines after the first header line
    key_count=$(echo "$output" | awk 'NR==1{next} NF>0{c++} END{print c+0}')
    
    if [ "$key_count" -le "$limit" ] && [ "$key_count" -gt 0 ]; then
        log_success "List operation with limit working correctly (returned $key_count keys, limit was $limit)"
        return 0
    else
        log_error "List operation with limit failed. Expected <= $limit keys, got $key_count"
        echo "List output:"
        echo "$output"
        return 1
    fi
}

# Run all tests
run_all_tests() {
    log_info "Starting functional test suite..."
    echo
    
    # Initialize counters
    TESTS_PASSED=0
    TESTS_FAILED=0
    
    # Run tests
    test_write_operation
    test_read_after_write
    test_read_nonexistent_key
    test_delete_operation
    test_list_operation
    test_list_with_limit
    
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
    echo "This script runs functional tests to verify basic KV store operations:"
    echo "  - Basic write operations"
    echo "  - Read-after-write consistency"
    echo "  - Reading non-existent keys"
    echo "  - Delete operations"
    echo "  - List operations with prefix filtering"
    echo "  - List operations with limit parameter"
    echo
    echo "Usage: $0"
    echo
    echo "The script will:"
    echo "  1. Start a gRPC server instance"
    echo "  2. Run a series of functional tests"
    echo "  3. Clean up test data automatically"
    echo "  4. Report test results and exit with appropriate code"
    echo
    echo "Prerequisites:"
    echo "  - Run 'make build' from project root to build required binaries"
    echo "  - Ensure port $GRPC_SERVER_PORT is available"
    exit 0
fi

# Execute main function
main "$@"