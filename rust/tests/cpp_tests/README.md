# C++ FFI Tests - Organized Structure

This directory contains the reorganized C++ FFI tests for the KV Store client library, separated by category for better maintainability and organization.

## File Structure

```
cpp_tests/
├── README.md                       # This file
├── test_common.hpp                 # Common utilities, macros, and RAII wrappers
├── main.cpp                        # Main test runner that executes all tests
├── test_basic_operations.cpp       # Basic CRUD operations and client lifecycle
├── test_configuration.cpp          # Client configuration and debug settings  
├── test_binary_data.cpp            # Binary data handling, null bytes, large data
├── test_range_operations.cpp       # Range queries and iteration
├── test_transactions.cpp           # Transaction management, commit, abort, read transactions
├── test_concurrency.cpp            # Multi-threaded operations and thread safety
└── test_error_handling.cpp         # Error scenarios, edge cases, resource cleanup
```

## Test Categories

### test_basic_operations.cpp
- **C Functions**: `test_c_init_shutdown`, `test_c_client_lifecycle`, `test_c_basic_transaction`, `test_c_client_ping`, `test_c_async_deletion`
- **C++ Functions**: `test_cpp_basic_functionality`, `test_cpp_wrapper`, `test_cpp_deletion_operations`
- **Focus**: Basic initialization, client creation, simple transactions, ping operations, and deletion

### test_configuration.cpp  
- **C Functions**: `test_c_configuration`, `test_c_debug_configuration`, `test_c_invalid_configuration`
- **C++ Functions**: `test_cpp_configuration`, `test_cpp_configuration_errors`
- **Focus**: Client configuration options, debug mode, timeout settings, and error handling for invalid configs

### test_binary_data.cpp
- **C Functions**: `test_c_binary_nulls`, `test_c_large_binary_data`, `test_c_empty_binary_data`
- **C++ Functions**: `test_cpp_binary_data_operations`
- **Focus**: Binary data with null bytes, large data handling, empty values, and STL container integration

### test_range_operations.cpp
- **C Functions**: `test_c_range_operations`, `test_c_range_binary_keys`
- **C++ Functions**: `test_cpp_range_operations`, `test_cpp_advanced_range_operations`
- **Focus**: Range queries, prefix matching, bounded ranges, hierarchical data, and lexicographic ordering

### test_transactions.cpp
- **C Functions**: `test_c_read_transaction`, `test_c_transaction_abort`, `test_c_transaction_conflict`, `test_c_transaction_timeout`
- **C++ Functions**: `test_cpp_transaction_lifecycle`, `test_cpp_nested_operations`, `test_cpp_transaction_error_recovery`
- **Focus**: Transaction management, conflict detection, timeouts, rollback, and recovery scenarios

### test_concurrency.cpp
- **C++ Functions**: `test_cpp_concurrent_operations`, `test_cpp_concurrent_read_write`, `test_cpp_concurrent_range_operations`, `test_cpp_thread_safety`
- **Focus**: Multi-threaded access, concurrent reads/writes, range operations under concurrency, and thread safety validation

### test_error_handling.cpp
- **C++ Functions**: `test_cpp_error_handling`, `test_cpp_invalid_operations`, `test_cpp_network_errors`, `test_cpp_resource_cleanup`, `test_cpp_timeout_scenarios`
- **Focus**: Error conditions, invalid parameters, network failures, resource management, and timeout handling

## Common Utilities (test_common.hpp)

### Macros
- `TEST_ASSERT(condition, message)` - Assert with error reporting
- `TEST_PASS()` - Mark test as passed

### Classes
- `FFITest` - Test framework class with assertions
- `KvConfigWrapper` - RAII wrapper for configuration handles
- `KvClientWrapper` - RAII wrapper for client handles  
- `KvTransactionWrapper` - RAII wrapper for transaction handles with automatic cleanup

### Helper Functions
- `wait_for_future_c()` - C-style future polling
- `wait_for_future_cpp()` - C++-style future polling with timeout

## Building and Running

### Using CMake (Recommended)
```bash
# From project root
cmake -B build -S .
cmake --build build --target cpp_ffi_test
cmake --build build --target test_ffi  # Runs the tests
```

### Manual Compilation
```bash
cd rust/client/tests
g++ -std=c++14 -o cpp_ffi_test \
    cpp_tests/main.cpp \
    cpp_tests/test_basic_operations.cpp \
    cpp_tests/test_configuration.cpp \
    cpp_tests/test_binary_data.cpp \
    cpp_tests/test_range_operations.cpp \
    cpp_tests/test_transactions.cpp \
    cpp_tests/test_concurrency.cpp \
    cpp_tests/test_error_handling.cpp \
    -I../include \
    -Icpp_tests \
    -L../target/release \
    -lkvstore_client \
    -lpthread
```

## Prerequisites

1. **KV Server Running**: Tests require a Thrift server running on `localhost:9090`
   ```bash
   ./target/debug/thrift-server --verbose --port 9090
   ```

2. **Rust Client Library**: The FFI library must be built
   ```bash
   cd rust/client
   cargo build --release --features ffi
   ```

## Test Output

The organized tests provide clear output by category:
- C-style tests run first, organized by functionality
- C++ tests follow, with clear categorization  
- Each test reports PASS/FAIL status
- Summary shows overall results

## Advantages of Organized Structure

1. **Maintainability**: Each category is self-contained and easier to modify
2. **Readability**: Related tests are grouped together
3. **Debugging**: Easier to isolate issues to specific functionality
4. **Extensibility**: New test categories can be added easily
5. **Parallel Development**: Multiple developers can work on different categories
6. **Selective Testing**: Can focus on specific functionality during development

## Migration from Unified Tests

This organized structure replaces the previous single-file approach:
- Better organization by test category
- Shared utilities and RAII wrappers
- Consistent naming and structure
- Easier maintenance and extension