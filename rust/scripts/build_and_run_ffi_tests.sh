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

# Check if the shared library was built
LIBPATH="$CLIENT_DIR/target/release/libkvstore_client.so"
if [ ! -f "$LIBPATH" ]; then
    echo "Error: Shared library not found at $LIBPATH"
    exit 1
fi

echo "Rust library built successfully!"
echo ""

# Set library path for runtime
export LD_LIBRARY_PATH="$CLIENT_DIR/target/release:$LD_LIBRARY_PATH"

# Check if the KV server is running
echo "Checking if KV server is running on localhost:9090..."
if ! nc -z localhost 9090 2>/dev/null; then
    echo "Warning: KV server not detected on localhost:9090"
    echo "Please start the Thrift server with: ./bin/rocksdbserver-thrift"
    echo "Some tests may fail without a running server."
    echo ""
fi

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