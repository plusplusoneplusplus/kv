#include "test_common.hpp"

//==============================================================================
// C-style Configuration Tests
//==============================================================================

// Test configuration functionality
int test_c_configuration() {
    kv_init();
    
    // Test creating default config
    KvConfigHandle config = kv_config_create();
    TEST_ASSERT(config != NULL, "Failed to create default config");
    
    // Test setting configuration values
    int result = kv_config_set_connection_timeout(config, 60);
    TEST_ASSERT(result == 0, "Failed to set connection timeout");
    
    result = kv_config_set_request_timeout(config, 20);
    TEST_ASSERT(result == 0, "Failed to set request timeout");
    
    result = kv_config_set_max_retries(config, 5);
    TEST_ASSERT(result == 0, "Failed to set max retries");
    
    result = kv_config_enable_debug(config);
    TEST_ASSERT(result == 0, "Failed to enable debug");
    
    // Test creating client with config
    KvClientHandle client = kv_client_create_with_config("localhost:9090", config);
    TEST_ASSERT(client != NULL, "Failed to create client with config");
    
    // Test that the client works normally with configuration
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction with configured client");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Simple set/get operation to verify the client works
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("config_test_key"), KV_STR("config_test_value"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed");
    
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_client_destroy(client);
    kv_config_destroy(config);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test debug-enabled configuration
int test_c_debug_configuration() {
    kv_init();
    
    // Test creating debug config
    KvConfigHandle debug_config = kv_config_create_with_debug();
    TEST_ASSERT(debug_config != NULL, "Failed to create debug config");
    
    // Test creating client with debug config
    KvClientHandle client = kv_client_create_with_config("localhost:9090", debug_config);
    TEST_ASSERT(client != NULL, "Failed to create client with debug config");
    
    // Test basic operation with debug enabled (should produce debug output)
    printf("Testing debug configuration - you should see debug output below:\n");
    
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction with debug client");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("debug_test_key"), KV_STR("debug_test_value"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed");
    
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    printf("Debug configuration test completed (check for debug output above)\n");
    
    // Cleanup
    kv_client_destroy(client);
    kv_config_destroy(debug_config);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test invalid configuration scenarios
int test_c_invalid_configuration() {
    kv_init();
    
    // Test null config handle operations
    int result = kv_config_enable_debug(NULL);
    TEST_ASSERT(result == -1, "Should fail with NULL config");
    
    result = kv_config_set_connection_timeout(NULL, 60);
    TEST_ASSERT(result == -1, "Should fail with NULL config for timeout");
    
    result = kv_config_set_max_retries(NULL, 5);
    TEST_ASSERT(result == -1, "Should fail with NULL config for retries");
    
    // Test client creation with NULL config
    KvClientHandle client = kv_client_create_with_config("localhost:9090", NULL);
    TEST_ASSERT(client == NULL, "Should fail with NULL config");
    
    // Test client creation with invalid config handle
    KvConfigHandle invalid_config = (KvConfigHandle)0x12345;
    client = kv_client_create_with_config("localhost:9090", invalid_config);
    TEST_ASSERT(client == NULL, "Should fail with invalid config handle");
    
    kv_shutdown();
    TEST_PASS();
    return 0;
}

//==============================================================================
// C++ Configuration Tests
//==============================================================================

// Test C++ configuration functionality
void test_cpp_configuration() {
    FFITest test("C++ Configuration");
    
    kv_init();
    
    try {
        // Test basic config creation and usage
        {
            KvConfigWrapper config;
            config.set_connection_timeout(60)
                  .set_request_timeout(20)
                  .set_max_retries(5);
            
            KvClientWrapper client("localhost:9090", config);
            
            // Test basic operation with configured client
            KvTransactionWrapper tx(client);
            tx.set("cpp_config_key", "cpp_config_value");
            
            std::string retrieved = tx.get("cpp_config_key");
            test.assert_true(retrieved == "cpp_config_value", "Config client failed to store/retrieve");
            
            tx.commit();
        }
        
        // Test debug configuration
        {
            std::cout << "Testing C++ debug configuration - expect debug output below:" << std::endl;
            
            KvConfigWrapper debug_config(true);  // Create with debug enabled
            debug_config.set_connection_timeout(30);
            
            KvClientWrapper client("localhost:9090", debug_config);
            
            KvTransactionWrapper tx(client);
            tx.set("cpp_debug_key", "cpp_debug_value");
            
            std::string retrieved = tx.get("cpp_debug_key");
            test.assert_true(retrieved == "cpp_debug_value", "Debug config client failed");
            
            tx.commit();
            
            std::cout << "C++ debug configuration test completed" << std::endl;
        }
        
        // Test chained configuration
        {
            KvConfigWrapper config;
            config.set_connection_timeout(45)
                  .set_request_timeout(15)
                  .set_max_retries(3)
                  .enable_debug();
            
            KvClientWrapper client("localhost:9090", config);
            
            KvTransactionWrapper tx(client);
            tx.set("cpp_chained_key", "cpp_chained_value");
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test C++ configuration error handling
void test_cpp_configuration_errors() {
    FFITest test("C++ Configuration Errors");
    
    kv_init();
    
    try {
        // Test invalid config creation scenarios
        bool exception_thrown = false;
        
        try {
            KvConfigWrapper config;
            // Try to create client with destroyed config (this should be safe due to RAII)
            // But let's test invalid address with config
            KvClientWrapper client("invalid:99999", config);
        } catch (const std::exception& e) {
            exception_thrown = true;
            std::cout << "Expected exception caught: " << e.what() << std::endl;
        }
        
        test.assert_true(exception_thrown, "Should have thrown exception for invalid server address");
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Unexpected exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}