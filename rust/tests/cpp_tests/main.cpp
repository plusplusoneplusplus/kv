#include "test_common.hpp"

// Include test declarations
// Basic Operations Tests
int test_c_init_shutdown();
int test_c_client_lifecycle();
int test_c_basic_transaction();
int test_c_client_ping();
int test_c_async_deletion();

// Configuration Tests  
int test_c_configuration();
int test_c_debug_configuration();
int test_c_invalid_configuration();

// Binary Data Tests
int test_c_binary_nulls();
int test_c_large_binary_data();
int test_c_empty_binary_data();

// Range Operations Tests
int test_c_range_operations();
int test_c_range_binary_keys();

// Transaction Tests
int test_c_read_transaction();
int test_c_read_transaction_binary_keys();
int test_c_transaction_abort();
int test_c_transaction_conflict();
int test_c_transaction_timeout();

// C++ test declarations
void test_cpp_basic_functionality();
void test_cpp_wrapper();
void test_cpp_deletion_operations();
void test_cpp_configuration();
void test_cpp_configuration_errors();
void test_cpp_binary_data_operations();
void test_cpp_range_operations();
void test_cpp_advanced_range_operations();
void test_cpp_transaction_lifecycle();
void test_cpp_nested_operations();
void test_cpp_transaction_error_recovery();
void test_cpp_concurrent_operations();
void test_cpp_concurrent_read_write();
void test_cpp_concurrent_range_operations();
void test_cpp_thread_safety();
void test_cpp_error_handling();
void test_cpp_invalid_operations();
void test_cpp_network_errors();
void test_cpp_resource_cleanup();
void test_cpp_timeout_scenarios();

// Versionstamped Operations Tests
void run_versionstamped_tests();

//==============================================================================
// Main Test Runner
//==============================================================================

int main() {
    std::cout << "Running KV Store Separated C/C++ FFI Tests" << std::endl;
    std::cout << "===========================================" << std::endl;
    
    // C-style test functions
    typedef int (*c_test_func)();
    typedef struct {
        const char* name;
        c_test_func func;
    } c_test_case;
    
    c_test_case c_tests[] = {
        // Basic Operations
        {"C Init/Shutdown", test_c_init_shutdown},
        {"C Client Lifecycle", test_c_client_lifecycle},
        {"C Basic Transaction", test_c_basic_transaction},
        {"C Client Ping", test_c_client_ping},
        {"C Async Deletion", test_c_async_deletion},
        
        // Configuration
        {"C Configuration", test_c_configuration},
        {"C Debug Configuration", test_c_debug_configuration},
        {"C Invalid Configuration", test_c_invalid_configuration},
        
        // Binary Data
        {"C Binary Nulls", test_c_binary_nulls},
        {"C Large Binary Data", test_c_large_binary_data},
        {"C Empty Binary Data", test_c_empty_binary_data},
        
        // Range Operations
        {"C Range Operations", test_c_range_operations},
        {"C Range Binary Keys", test_c_range_binary_keys},
        
        // Transactions
        {"C Read Transaction", test_c_read_transaction},
        {"C Read Transaction Binary Keys", test_c_read_transaction_binary_keys},
        {"C Transaction Abort", test_c_transaction_abort},
        {"C Transaction Conflict", test_c_transaction_conflict},
        {"C Transaction Timeout", test_c_transaction_timeout},
        
        {NULL, NULL}
    };
    
    // C++-style test functions
    std::vector<std::pair<std::string, std::function<void()>>> cpp_tests = {
        // Basic Operations
        {"C++ Basic Functionality", test_cpp_basic_functionality},
        {"C++ RAII Wrapper", test_cpp_wrapper},
        {"C++ Deletion Operations", test_cpp_deletion_operations},
        
        // Configuration
        {"C++ Configuration", test_cpp_configuration},
        {"C++ Configuration Errors", test_cpp_configuration_errors},
        
        // Binary Data
        {"C++ Binary Data Operations", test_cpp_binary_data_operations},
        
        // Range Operations
        {"C++ Range Operations", test_cpp_range_operations},
        {"C++ Advanced Range Operations", test_cpp_advanced_range_operations},
        
        // Transactions
        {"C++ Transaction Lifecycle", test_cpp_transaction_lifecycle},
        {"C++ Nested Operations", test_cpp_nested_operations},
        {"C++ Transaction Error Recovery", test_cpp_transaction_error_recovery},
        
        // Concurrency
        {"C++ Concurrent Operations", test_cpp_concurrent_operations},
        {"C++ Concurrent Read/Write", test_cpp_concurrent_read_write},
        {"C++ Concurrent Range Operations", test_cpp_concurrent_range_operations},
        {"C++ Thread Safety", test_cpp_thread_safety},
        
        // Error Handling
        {"C++ Error Handling", test_cpp_error_handling},
        {"C++ Invalid Operations", test_cpp_invalid_operations},
        {"C++ Network Errors", test_cpp_network_errors},
        {"C++ Resource Cleanup", test_cpp_resource_cleanup},
        {"C++ Timeout Scenarios", test_cpp_timeout_scenarios}
    };
    
    int passed = 0;
    int failed = 0;
    int test_counter = 1;
    
    // Run versionstamped tests first (as they are newer functionality)
    std::cout << "\n=== Versionstamped Operations Tests ===" << std::endl;
    try {
        run_versionstamped_tests();
        passed++;
        std::cout << "✅ All versionstamped tests passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "❌ Versionstamped tests failed: " << e.what() << std::endl;
        failed++;
    }
    
    std::cout << "\n=== C-Style Tests ===" << std::endl;
    
    // Run C-style tests
    for (int i = 0; c_tests[i].name != NULL; i++) {
        std::cout << "\n[" << test_counter++ << "] Running " << c_tests[i].name << "..." << std::endl;
        try {
            int result = c_tests[i].func();
            if (result == 0) {
                passed++;
            } else {
                failed++;
            }
        } catch (const std::exception& e) {
            std::cerr << "FAIL: " << c_tests[i].name << " - " << e.what() << std::endl;
            failed++;
        }
    }
    
    std::cout << "\n=== C++ Style Tests ===" << std::endl;
    
    // Run C++-style tests
    for (size_t i = 0; i < cpp_tests.size(); i++) {
        std::cout << "\n[" << test_counter++ << "] Running " << cpp_tests[i].first << "..." << std::endl;
        try {
            cpp_tests[i].second();
            passed++;
        } catch (const std::exception& e) {
            std::cerr << "FAIL: " << cpp_tests[i].first << " - " << e.what() << std::endl;
            failed++;
        }
    }
    
    std::cout << "\n=========================================" << std::endl;
    std::cout << "Test Results: " << passed << " passed, " << failed << " failed" << std::endl;
    
    if (failed > 0) {
        std::cout << "\nNote: Some tests may fail if the KV server is not running on localhost:9090" << std::endl;
        std::cout << "Start the server with: ./target/debug/thrift-server --verbose --port 9090" << std::endl;
        return 1;
    }
    
    std::cout << "\nAll tests passed! 🎉" << std::endl;
    return 0;
}