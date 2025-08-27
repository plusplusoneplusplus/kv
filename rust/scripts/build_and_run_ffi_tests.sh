#!/bin/bash

# Build and run unified FFI tests (C and C++) for the KV store client

set -e

# Parse command line arguments
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -v, --verbose      Verbose output"
            echo "  -h, --help         Show this help"
            echo ""
            echo "This script builds and runs the unified FFI test that includes"
            echo "both C and C++ style tests in a single executable."
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Building KV Store Unified FFI Tests"
echo "==================================="

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUST_DIR="$(dirname "$SCRIPT_DIR")"
CLIENT_DIR="$RUST_DIR/client"

# Build the Rust library first
echo "Building Rust library..."
cd "$CLIENT_DIR"
if [ "$VERBOSE" = true ]; then
    cargo build --release
else
    cargo build --release > /dev/null 2>&1
fi

# Check if the shared library was built (platform-specific extension)
if [[ "$OSTYPE" == "darwin"* ]]; then
    LIBPATH="$CLIENT_DIR/target/release/libkvstore_client.dylib"
else
    LIBPATH="$CLIENT_DIR/target/release/libkvstore_client.so"
fi
if [ ! -f "$LIBPATH" ]; then
    echo "Error: Shared library not found at $LIBPATH"
    exit 1
fi

echo "Rust library built successfully!"
echo ""

# Set library path for runtime (platform-specific)
if [[ "$OSTYPE" == "darwin"* ]]; then
    export DYLD_LIBRARY_PATH="$CLIENT_DIR/target/release:$DYLD_LIBRARY_PATH"
else
    export LD_LIBRARY_PATH="$CLIENT_DIR/target/release:$LD_LIBRARY_PATH"
fi

# Check if the KV server is running and start it if needed
echo "Checking if KV server is running on localhost:9090..."
SERVER_STARTED=false
SERVER_PID=""

if ! nc -z localhost 9090 2>/dev/null; then
    echo "KV server not detected. Starting Thrift server..."
    
    # Navigate to project root to build and start server
    PROJECT_ROOT="$(cd "$RUST_DIR/.." && pwd)"
    cd "$PROJECT_ROOT"
    
    # Build the Thrift server if it doesn't exist
    if [ ! -f "bin/rocksdbserver-thrift" ]; then
        echo "Building Thrift server..."
        if [ "$VERBOSE" = true ]; then
            make rust-deps
            cd rust
            cargo build --release --bin thrift-server
            cd "$PROJECT_ROOT"
        else
            make rust-deps > /dev/null 2>&1
            cd rust
            cargo build --release --bin thrift-server > /dev/null 2>&1
            cd "$PROJECT_ROOT"
        fi
        
        # Copy binary to bin directory
        mkdir -p bin
        cp rust/target/release/thrift-server bin/rocksdbserver-thrift
    fi
    
    # Start the server in the background
    echo "Starting Thrift server on localhost:9090..."
    ./bin/rocksdbserver-thrift > /dev/null 2>&1 &
    SERVER_PID=$!
    SERVER_STARTED=true
    
    # Wait for server to start
    echo "Waiting for server to start..."
    for i in {1..10}; do
        if nc -z localhost 9090 2>/dev/null; then
            echo "Server started successfully!"
            break
        fi
        sleep 1
        if [ $i -eq 10 ]; then
            echo "Error: Server failed to start after 10 seconds"
            if [ -n "$SERVER_PID" ]; then
                kill $SERVER_PID 2>/dev/null || true
            fi
            exit 1
        fi
    done
    echo ""
else
    echo "Server is already running."
    echo ""
fi

# Function to cleanup server on exit
cleanup_server() {
    if [ "$SERVER_STARTED" = true ] && [ -n "$SERVER_PID" ]; then
        echo ""
        echo "Stopping test server..."
        kill $SERVER_PID 2>/dev/null || true
        
        # Wait for server to stop
        for i in {1..5}; do
            if ! kill -0 $SERVER_PID 2>/dev/null; then
                break
            fi
            sleep 1
        done
        
        # Force kill if still running
        kill -9 $SERVER_PID 2>/dev/null || true
        echo "Server stopped."
    fi
}

# Set trap to cleanup server on script exit
trap cleanup_server EXIT

cd "$CLIENT_DIR/tests"

# Function to run unified FFI tests
run_unified_tests() {
    echo "Building and running unified FFI tests..."
    echo "========================================="
    
    # Build the unified test executable
    g++ -std=c++14 -o unified_ffi_test \
        unified_ffi_test.cpp \
        -I"$CLIENT_DIR/include" \
        -L"$CLIENT_DIR/target/release" \
        -lkvstore_client \
        -lpthread \
        -ldl \
        -lm
    
    # Check if compilation succeeded
    if [ ! -f "unified_ffi_test" ]; then
        echo "Error: Failed to build unified test executable"
        return 1
    fi
    
    echo "Build successful! Running unified tests..."
    ./unified_ffi_test
    local result=$?
    
    # Cleanup
    rm -f unified_ffi_test
    
    if [ $result -eq 0 ]; then
        echo "✓ Unified FFI tests PASSED"
    else
        echo "✗ Unified FFI tests FAILED"
    fi
    
    return $result
}

# Run the unified tests
run_unified_tests
result=$?
echo ""

# Summary
echo "=========================================="
echo "FFI Test Results Summary:"

if [ $result -eq 0 ]; then
    echo "  Unified tests: ✓ PASSED"
    echo ""
    echo "🎉 All FFI tests completed successfully!"
    exit 0
else
    echo "  Unified tests: ✗ FAILED"
    echo ""
    echo "❌ Tests failed. Check the output above for details."
    exit 1
fi