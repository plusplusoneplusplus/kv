#!/bin/bash

# Build and run FFI tests (both C and C++) for the KV store client

set -e

# Parse command line arguments
TEST_TYPE="both"
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--c-only)
            TEST_TYPE="c"
            shift
            ;;
        -cpp|--cpp-only)
            TEST_TYPE="cpp"
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -c, --c-only       Run only C tests"
            echo "  -cpp, --cpp-only   Run only C++ tests"
            echo "  -v, --verbose      Verbose output"
            echo "  -h, --help         Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Building KV Store FFI Tests"
echo "=========================="

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
            make rust-deps && cd rust && cargo build --release --bin thrift-server
        else
            make rust-deps > /dev/null 2>&1 && cd rust && cargo build --release --bin thrift-server > /dev/null 2>&1
        fi
        
        # Copy binary to bin directory
        mkdir -p bin
        cp rust/target/release/thrift-server bin/rocksdbserver-thrift
        cd "$PROJECT_ROOT"
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

# Function to run C tests
run_c_tests() {
    echo "Building and running C FFI tests..."
    echo "==================================="
    
    # Build the C test executable
    gcc -o ffi_test \
        ffi_test.c \
        -I"$CLIENT_DIR/include" \
        -L"$CLIENT_DIR/target/release" \
        -lkvstore_client \
        -lpthread \
        -ldl \
        -lm
    
    # Check if compilation succeeded
    if [ ! -f "ffi_test" ]; then
        echo "Error: Failed to build C test executable"
        return 1
    fi
    
    echo "C build successful! Running tests..."
    ./ffi_test
    local c_result=$?
    
    # Cleanup
    rm -f ffi_test
    
    if [ $c_result -eq 0 ]; then
        echo "✓ C FFI tests PASSED"
    else
        echo "✗ C FFI tests FAILED"
    fi
    
    return $c_result
}

# Function to run C++ tests
run_cpp_tests() {
    echo "Building and running C++ FFI tests..."
    echo "====================================="
    
    # Build the C++ test executable
    g++ -std=c++14 -o cpp_ffi_test \
        cpp_ffi_test.cpp \
        -I"$CLIENT_DIR/include" \
        -L"$CLIENT_DIR/target/release" \
        -lkvstore_client \
        -lpthread \
        -ldl \
        -lm
    
    # Check if compilation succeeded
    if [ ! -f "cpp_ffi_test" ]; then
        echo "Error: Failed to build C++ test executable"
        return 1
    fi
    
    echo "C++ build successful! Running tests..."
    ./cpp_ffi_test
    local cpp_result=$?
    
    # Cleanup
    rm -f cpp_ffi_test
    
    if [ $cpp_result -eq 0 ]; then
        echo "✓ C++ FFI tests PASSED"
    else
        echo "✗ C++ FFI tests FAILED"
    fi
    
    return $cpp_result
}

# Run tests based on user selection
c_result=0
cpp_result=0

if [ "$TEST_TYPE" = "c" ] || [ "$TEST_TYPE" = "both" ]; then
    run_c_tests
    c_result=$?
    echo ""
fi

if [ "$TEST_TYPE" = "cpp" ] || [ "$TEST_TYPE" = "both" ]; then
    run_cpp_tests
    cpp_result=$?
    echo ""
fi

# Summary
echo "=========================================="
echo "FFI Test Results Summary:"

if [ "$TEST_TYPE" = "c" ] || [ "$TEST_TYPE" = "both" ]; then
    if [ $c_result -eq 0 ]; then
        echo "  C tests:   ✓ PASSED"
    else
        echo "  C tests:   ✗ FAILED"
    fi
fi

if [ "$TEST_TYPE" = "cpp" ] || [ "$TEST_TYPE" = "both" ]; then
    if [ $cpp_result -eq 0 ]; then
        echo "  C++ tests: ✓ PASSED"
    else
        echo "  C++ tests: ✗ FAILED"
    fi
fi

total_failures=$((c_result + cpp_result))
if [ $total_failures -eq 0 ]; then
    echo ""
    echo "🎉 All FFI tests completed successfully!"
    exit 0
else
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
    exit 1
fi