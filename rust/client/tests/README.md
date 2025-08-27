# KV Store Client Tests

This directory contains comprehensive test suites for the KV Store client library, covering both Rust and C/C++ FFI interfaces.

## Test Structure

### Rust Integration Tests
- **`integration_test.rs`** - Comprehensive Rust API tests covering:
  - Basic transaction operations (set, get, commit)
  - Transaction conflict detection
  - Range operations
  - Read-only transactions
  - Versionstamped operations
  - Connection timeout handling
  - Transaction timeout behavior
  - Delete operations
  - Ping functionality

### Unified C/C++ FFI Tests
- **`unified_ffi_test.cpp`** - Combined C and C++ language FFI tests covering:
  - **C-style tests**: Library initialization, client lifecycle, basic transactions, 
    read transactions, error handling, string memory management, future polling,
    ping functionality, and transaction abort operations
  - **C++ style tests**: RAII wrapper classes for safer C++ integration,
    exception-based error handling, multi-threaded concurrent operations,
    and modern C++ best practices (C++14)

## Running the Tests

### Prerequisites
1. **Rust dependencies**: Run `cargo build --release` in the client directory
2. **KV Server**: Start the Thrift server on localhost:9090:
   ```bash
   ./bin/rocksdbserver-thrift
   ```

### Running Rust Tests
```bash
cd /home/yihengtao/kv/rust/client
cargo test
```

### Running FFI Tests

#### Using the Build Script
The FFI test script is located in `/home/yihengtao/kv/rust/scripts/`:

```bash
# Run unified FFI tests
cd /home/yihengtao/kv/rust/scripts
./build_and_run_ffi_tests.sh

# Verbose output
./build_and_run_ffi_tests.sh --verbose

# Show help
./build_and_run_ffi_tests.sh --help
```

#### Using CMake Build System
Alternatively, you can use the CMake build system from the project root:

```bash
# Build all components including the unified FFI test
cmake -B build -S .
cmake --build build

# Run the unified FFI test
cmake --build build --target test_ffi
# or directly:
./build/bin/unified_ffi_test
```

### Running All Tests
```bash
# From the client directory
cargo test
cd ../scripts
./build_and_run_ffi_tests.sh

# Or using CMake from project root
cmake -B build -S .
cmake --build build
cmake --build build --target test_ffi
```

## Test Features

### Comprehensive Coverage
- **API Coverage**: All major client operations (connect, transaction lifecycle, CRUD operations)
- **Error Handling**: Network errors, timeout handling, invalid inputs
- **Memory Safety**: Proper cleanup and string memory management for FFI
- **Concurrency**: Multi-transaction and multi-threaded scenarios
- **Edge Cases**: NULL pointer handling, invalid server addresses

### FFI-Specific Testing
- **Memory Management**: Proper allocation/deallocation of C strings
- **Handle Lifecycle**: Client, transaction, and future handle management
- **Async Integration**: Future polling and result retrieval
- **Type Safety**: Proper type conversions between Rust and C/C++

### C++ Enhancements
- **RAII Wrappers**: Safe automatic resource management
- **Exception Safety**: Exception-based error propagation
- **Modern C++ Features**: Uses C++14 features for cleaner code
- **Thread Safety**: Multi-threaded test scenarios

## Test Architecture

### Future-Based Async Testing
All FFI tests use a polling-based approach to handle the async Rust operations:

```c
// C example
KvFutureHandle future = kv_transaction_begin(client, 30);
while (!kv_future_poll(future)) {
    usleep(1000);  // Wait 1ms
}
KvTransactionHandle tx = kv_future_get_transaction(future);
```

### RAII C++ Wrapper Example
```cpp
// C++ RAII wrapper example
{
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    tx.set("key", "value");
    std::string value = tx.get("key");
    tx.commit();
} // Automatic cleanup
```

## Build Requirements

### System Dependencies
- GCC or Clang with C99/C++14 support
- pthread library
- netcat (for server connectivity checks)

### Library Dependencies
- Built Rust shared library (`libkvstore_client.so`)
- KV store headers (`include/kvstore_client.h`)

## Troubleshooting

### Common Issues
1. **Server not running**: Ensure Thrift server is running on localhost:9090
2. **Library not found**: Check that `cargo build --release` completed successfully
3. **Permission denied**: Ensure test scripts are executable (`chmod +x`)
4. **Connection timeout**: Verify firewall settings and server availability

### Debug Mode
For debugging, build the Rust library in debug mode and use debug symbols:
```bash
cargo build  # Debug build
cd ../scripts
# The script will automatically use the debug build if release is not available
./build_and_run_ffi_tests.sh --verbose
```

## Test Output
All tests provide detailed output including:
- Individual test pass/fail status
- Error messages with context
- Performance timing information
- Memory leak detection (via valgrind if available)
- Server connectivity status
- Summary of all test results with clear pass/fail indicators