#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstring>
#include <functional>
#include <unistd.h>
#include "../include/kvstore_client.h"

// Test framework macros and classes
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "FAIL: %s - %s\n", __func__, message); \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf("PASS: %s\n", __func__); \
    } while(0)

class FFITest {
private:
    std::string test_name;
    
public:
    FFITest(const std::string& name) : test_name(name) {}
    
    void assert_true(bool condition, const std::string& message) {
        if (!condition) {
            std::cerr << "FAIL: " << test_name << " - " << message << std::endl;
            throw std::runtime_error("Test assertion failed");
        }
    }
    
    void pass() {
        std::cout << "PASS: " << test_name << std::endl;
    }
};

// Helper function to wait for future completion (C-style)
int wait_for_future_c(KvFutureHandle future) {
    int max_polls = 1000;  // Maximum number of polls
    for (int i = 0; i < max_polls; i++) {
        int status = kv_future_poll(future);
        if (status == 1) {
            return 1;  // Ready
        } else if (status == -1) {
            return -1;  // Error
        }
        usleep(1000);  // Sleep 1ms
    }
    return 0;  // Timeout
}

// Helper function to wait for future completion (C++-style)
bool wait_for_future_cpp(KvFutureHandle future, int timeout_ms = 5000) {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        int status = kv_future_poll(future);
        if (status == 1) {
            return true;  // Ready
        } else if (status == -1) {
            return false;  // Error
        }
        
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (elapsed > timeout_ms) {
            return false;  // Timeout
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

//==============================================================================
// C-style Tests (converted from ffi_test.c)
//==============================================================================

// Test basic initialization and cleanup
int test_c_init_shutdown() {
    int result = kv_init();
    TEST_ASSERT(result == 0, "kv_init failed");
    
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test client creation and destruction
int test_c_client_lifecycle() {
    kv_init();
    
    // Test invalid address
    KvClientHandle client = kv_client_create(NULL);
    TEST_ASSERT(client == NULL, "Should fail with NULL address");
    
    // Test valid connection (assuming server is running on localhost:9090)
    client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test basic transaction operations
int test_c_basic_transaction() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Set a key-value pair (using new binary interface with convenience macro)
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("test_key"), KV_STR("test_value"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed");
    
    // Get the value back (using new binary interface)
    KvFutureHandle get_future = kv_transaction_get(tx, KV_STR("test_key"), NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create get future");
    
    ready = wait_for_future_c(get_future);
    TEST_ASSERT(ready == 1, "Get future not ready");
    
    KvBinaryData value;
    KvResult get_result = kv_future_get_value_result(get_future, &value);
    TEST_ASSERT(get_result.success == 1, "Get operation failed");
    TEST_ASSERT(value.data != NULL, "Got NULL value data");
    TEST_ASSERT(value.length == strlen("test_value"), "Value length mismatch");
    TEST_ASSERT(memcmp(value.data, "test_value", strlen("test_value")) == 0, "Value mismatch");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup (using new binary cleanup)
    kv_binary_free(&value);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test read transaction
int test_c_read_transaction() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // First, set up some data with a regular transaction
    KvFutureHandle setup_tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(setup_tx_future != NULL, "Failed to begin setup transaction");
    
    int ready = wait_for_future_c(setup_tx_future);
    TEST_ASSERT(ready == 1, "Setup transaction begin future not ready");
    
    KvTransactionHandle setup_tx = kv_future_get_transaction(setup_tx_future);
    TEST_ASSERT(setup_tx != NULL, "Failed to get setup transaction handle");
    
    KvFutureHandle set_future = kv_transaction_set(setup_tx, KV_STR("read_test_key"), KV_STR("read_test_value"), NULL);
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Setup set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Setup set operation failed");
    
    KvFutureHandle commit_future = kv_transaction_commit(setup_tx);
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Setup commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Setup commit operation failed");
    
    // Now test read transaction
    KvFutureHandle read_tx_future = kv_read_transaction_begin(client, -1);
    TEST_ASSERT(read_tx_future != NULL, "Failed to begin read transaction");
    
    ready = wait_for_future_c(read_tx_future);
    TEST_ASSERT(ready == 1, "Read transaction begin future not ready");
    
    KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
    TEST_ASSERT(read_tx != NULL, "Failed to get read transaction handle");
    
    // Read the value
    KvFutureHandle get_future = kv_read_transaction_get(read_tx, KV_STR("read_test_key"), NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create read get future");
    
    ready = wait_for_future_c(get_future);
    TEST_ASSERT(ready == 1, "Read get future not ready");
    
    KvBinaryData value;
    KvResult get_result = kv_future_get_value_result(get_future, &value);
    TEST_ASSERT(get_result.success == 1, "Read get operation failed");
    TEST_ASSERT(value.data != NULL, "Got NULL value from read transaction");
    TEST_ASSERT(value.length == strlen("read_test_value"), "Read value length mismatch");
    TEST_ASSERT(memcmp(value.data, "read_test_value", strlen("read_test_value")) == 0, "Read value mismatch");
    
    // Cleanup
    kv_binary_free(&value);
    kv_read_transaction_destroy(read_tx);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

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

// Test client ping functionality
int test_c_client_ping() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Test ping with NULL message
    KvFutureHandle ping_future1 = kv_client_ping(client, NULL, 0);
    TEST_ASSERT(ping_future1 != NULL, "Failed to create ping future with NULL message");
    
    int ready = wait_for_future_c(ping_future1);
    TEST_ASSERT(ready == 1, "Ping future not ready");
    
    KvBinaryData response;
    KvResult ping_result1 = kv_future_get_value_result(ping_future1, &response);
    TEST_ASSERT(ping_result1.success == 1, "Ping operation failed");
    TEST_ASSERT(response.data != NULL, "Got NULL ping response");
    printf("Ping response: %.*s\n", response.length, (char*)response.data);
    kv_binary_free(&response);
    
    // Test ping with custom message
    const char* msg = "Hello from C FFI test!";
    KvFutureHandle ping_future2 = kv_client_ping(client, (uint8_t*)msg, strlen(msg));
    TEST_ASSERT(ping_future2 != NULL, "Failed to create ping future with custom message");
    
    ready = wait_for_future_c(ping_future2);
    TEST_ASSERT(ready == 1, "Ping future not ready");
    
    KvBinaryData response2;
    KvResult ping_result2 = kv_future_get_value_result(ping_future2, &response2);
    TEST_ASSERT(ping_result2.success == 1, "Ping operation with custom message failed");
    TEST_ASSERT(response2.data != NULL, "Got NULL ping response with custom message");
    printf("Ping response with custom message: %.*s\n", response2.length, (char*)response2.data);
    kv_binary_free(&response2);
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test transaction abort functionality
int test_c_transaction_abort() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Set a key-value pair in the transaction
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("abort_test_key_c"), KV_STR("abort_test_value_c"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed");
    
    // Abort the transaction instead of committing
    KvFutureHandle abort_future = kv_transaction_abort(tx);
    TEST_ASSERT(abort_future != NULL, "Failed to create abort future");
    
    ready = wait_for_future_c(abort_future);
    TEST_ASSERT(ready == 1, "Abort future not ready");
    
    KvResult abort_result = kv_future_get_void_result(abort_future);
    TEST_ASSERT(abort_result.success == 1, "Abort operation failed");
    printf("Transaction abort FFI call succeeded\n");
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test range operations (C-style)
int test_c_range_operations() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // First, set up test data
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin setup transaction");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Setup transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get setup transaction handle");
    
    // Set up test data with predictable ordering
    const char* test_keys[] = {"range_key_001", "range_key_002", "range_key_003", "range_key_004", NULL};
    const char* test_values[] = {"value1", "value2", "value3", "value4", NULL};
    
    for (int i = 0; test_keys[i] != NULL; i++) {
        KvFutureHandle set_future = kv_transaction_set(tx, 
                                                        (const uint8_t*)test_keys[i], strlen(test_keys[i]),
                                                        (const uint8_t*)test_values[i], strlen(test_values[i]),
                                                        NULL);
        TEST_ASSERT(set_future != NULL, "Failed to create set future for test data");
        
        ready = wait_for_future_c(set_future);
        TEST_ASSERT(ready == 1, "Set future not ready for test data");
        
        KvResult set_result = kv_future_get_void_result(set_future);
        TEST_ASSERT(set_result.success == 1, "Set operation failed for test data");
    }
    
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Setup commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Setup commit operation failed");
    
    // Now test range operations using read transaction
    KvFutureHandle read_tx_future = kv_read_transaction_begin(client, -1);
    TEST_ASSERT(read_tx_future != NULL, "Failed to begin read transaction");
    
    ready = wait_for_future_c(read_tx_future);
    TEST_ASSERT(ready == 1, "Read transaction begin future not ready");
    
    KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
    TEST_ASSERT(read_tx != NULL, "Failed to get read transaction handle");
    
    // Test 1: Range query with prefix
    KvFutureHandle range_future = kv_read_transaction_get_range(read_tx, 
                                                                 KV_STR("range_key"), 
                                                                 NULL, 0, 
                                                                 10, NULL);
    TEST_ASSERT(range_future != NULL, "Failed to create range future");
    
    ready = wait_for_future_c(range_future);
    TEST_ASSERT(ready == 1, "Range future not ready");
    
    KvPairArray pairs = {NULL, 0};
    KvResult range_result = kv_future_get_kv_array_result(range_future, &pairs);
    TEST_ASSERT(range_result.success == 1, "Range operation failed");
    TEST_ASSERT(pairs.count == 4, "Expected 4 pairs from range query");
    TEST_ASSERT(pairs.pairs != NULL, "Pairs array should not be NULL");
    
    // Verify the returned data
    for (size_t i = 0; i < pairs.count; i++) {
        TEST_ASSERT(pairs.pairs[i].key.data != NULL, "Key should not be NULL");
        TEST_ASSERT(pairs.pairs[i].value.data != NULL, "Value should not be NULL");
        printf("Range result [%zu]: %.*s = %.*s\n", i, 
               pairs.pairs[i].key.length, (char*)pairs.pairs[i].key.data,
               pairs.pairs[i].value.length, (char*)pairs.pairs[i].value.data);
    }
    
    kv_pair_array_free(&pairs);
    
    // Test 2: Range query with start and end key
    KvFutureHandle bounded_range_future = kv_read_transaction_get_range(read_tx, 
                                                                         KV_STR("range_key_001"), 
                                                                         KV_STR("range_key_003"), 
                                                                         10, NULL);
    TEST_ASSERT(bounded_range_future != NULL, "Failed to create bounded range future");
    
    ready = wait_for_future_c(bounded_range_future);
    TEST_ASSERT(ready == 1, "Bounded range future not ready");
    
    KvPairArray bounded_pairs = {NULL, 0};
    KvResult bounded_result = kv_future_get_kv_array_result(bounded_range_future, &bounded_pairs);
    TEST_ASSERT(bounded_result.success == 1, "Bounded range operation failed");
    TEST_ASSERT(bounded_pairs.count == 2, "Expected 2 pairs from bounded range query (exclusive end)");
    
    kv_pair_array_free(&bounded_pairs);
    
    // Test 3: Limited range query
    KvFutureHandle limited_range_future = kv_read_transaction_get_range(read_tx, 
                                                                         KV_STR("range_key"), 
                                                                         NULL, 0, 
                                                                         2, NULL);
    TEST_ASSERT(limited_range_future != NULL, "Failed to create limited range future");
    
    ready = wait_for_future_c(limited_range_future);
    TEST_ASSERT(ready == 1, "Limited range future not ready");
    
    KvPairArray limited_pairs = {NULL, 0};
    KvResult limited_result = kv_future_get_kv_array_result(limited_range_future, &limited_pairs);
    TEST_ASSERT(limited_result.success == 1, "Limited range operation failed");
    TEST_ASSERT(limited_pairs.count == 2, "Expected 2 pairs from limited range query");
    
    kv_pair_array_free(&limited_pairs);
    
    // Cleanup
    kv_read_transaction_destroy(read_tx);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test deletion operations with async API
int test_c_async_deletion() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Set up test data using async operations
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("async_delete_test_key"), KV_STR("async_delete_test_value"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Async set operation failed");
    
    // Verify the key exists before deletion
    KvFutureHandle get_future = kv_transaction_get(tx, KV_STR("async_delete_test_key"), NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create get future");
    ready = wait_for_future_c(get_future);
    TEST_ASSERT(ready == 1, "Get future not ready");
    KvBinaryData value;
    KvResult get_result = kv_future_get_value_result(get_future, &value);
    TEST_ASSERT(get_result.success == 1, "Async get operation failed");
    TEST_ASSERT(value.data != NULL, "Got NULL value");
    TEST_ASSERT(value.length == strlen("async_delete_test_value"), "Value length mismatch");
    TEST_ASSERT(memcmp(value.data, "async_delete_test_value", strlen("async_delete_test_value")) == 0, "Value mismatch");
    kv_binary_free(&value);
    
    // Delete the key using async operation
    KvFutureHandle delete_future = kv_transaction_delete(tx, KV_STR("async_delete_test_key"), NULL);
    TEST_ASSERT(delete_future != NULL, "Failed to create delete future");
    ready = wait_for_future_c(delete_future);
    TEST_ASSERT(ready == 1, "Delete future not ready");
    KvResult delete_result = kv_future_get_void_result(delete_future);
    TEST_ASSERT(delete_result.success == 1, "Async delete operation failed");
    
    // Verify the key no longer exists
    KvFutureHandle get_future2 = kv_transaction_get(tx, KV_STR("async_delete_test_key"), NULL);
    TEST_ASSERT(get_future2 != NULL, "Failed to create get future after delete");
    ready = wait_for_future_c(get_future2);
    TEST_ASSERT(ready == 1, "Get future after delete not ready");
    KvBinaryData value2;
    KvResult get_result2 = kv_future_get_value_result(get_future2, &value2);
    TEST_ASSERT(get_result2.success == 1, "Get after delete failed");
    TEST_ASSERT(value2.data == NULL || value2.length == 0, "Key should be empty after deletion");
    
    // Commit the transaction using async operation
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Async commit operation failed");
    
    printf("Async deletion operations test completed successfully\n");
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

//==============================================================================
// C++ Style Tests (converted from cpp_ffi_test.cpp)
//==============================================================================

// RAII wrapper classes
class KvConfigWrapper {
private:
    KvConfigHandle handle;
    
public:
    explicit KvConfigWrapper(bool with_debug = false) {
        handle = with_debug ? kv_config_create_with_debug() : kv_config_create();
        if (!handle) {
            throw std::runtime_error("Failed to create KV config");
        }
    }
    
    ~KvConfigWrapper() {
        if (handle) {
            kv_config_destroy(handle);
        }
    }
    
    KvConfigHandle get() const { return handle; }
    
    KvConfigWrapper& set_connection_timeout(uint64_t timeout_seconds) {
        int result = kv_config_set_connection_timeout(handle, timeout_seconds);
        if (result != 0) {
            throw std::runtime_error("Failed to set connection timeout");
        }
        return *this;
    }
    
    KvConfigWrapper& set_request_timeout(uint64_t timeout_seconds) {
        int result = kv_config_set_request_timeout(handle, timeout_seconds);
        if (result != 0) {
            throw std::runtime_error("Failed to set request timeout");
        }
        return *this;
    }
    
    KvConfigWrapper& set_max_retries(uint32_t retries) {
        int result = kv_config_set_max_retries(handle, retries);
        if (result != 0) {
            throw std::runtime_error("Failed to set max retries");
        }
        return *this;
    }
    
    KvConfigWrapper& enable_debug() {
        int result = kv_config_enable_debug(handle);
        if (result != 0) {
            throw std::runtime_error("Failed to enable debug");
        }
        return *this;
    }
    
    KvConfigWrapper(const KvConfigWrapper&) = delete;
    KvConfigWrapper& operator=(const KvConfigWrapper&) = delete;
};

class KvClientWrapper {
private:
    KvClientHandle handle;
    
public:
    explicit KvClientWrapper(const std::string& address) {
        handle = kv_client_create(address.c_str());
        if (!handle) {
            throw std::runtime_error("Failed to create KV client");
        }
    }
    
    explicit KvClientWrapper(const std::string& address, KvConfigWrapper& config) {
        handle = kv_client_create_with_config(address.c_str(), config.get());
        if (!handle) {
            throw std::runtime_error("Failed to create KV client with config");
        }
    }
    
    ~KvClientWrapper() {
        if (handle) {
            kv_client_destroy(handle);
        }
    }
    
    KvClientHandle get() const { return handle; }
    
    KvClientWrapper(const KvClientWrapper&) = delete;
    KvClientWrapper& operator=(const KvClientWrapper&) = delete;
};

class KvTransactionWrapper {
private:
    KvTransactionHandle handle;
    bool committed;
    
public:
    explicit KvTransactionWrapper(KvClientWrapper& client, int timeout_seconds = 30) 
        : handle(nullptr), committed(false) {
        KvFutureHandle future = kv_transaction_begin(client.get(), timeout_seconds);
        if (!future) {
            throw std::runtime_error("Failed to begin transaction");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Transaction begin timeout");
        }
        
        handle = kv_future_get_transaction(future);
        if (!handle) {
            throw std::runtime_error("Failed to get transaction handle");
        }
    }
    
    ~KvTransactionWrapper() {
        // Auto-rollback if not committed (we don't have abort in FFI currently)
    }
    
    KvTransactionHandle get() const { return handle; }
    
    void set(const std::string& key, const std::string& value, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_set(handle,
                                                   (const uint8_t*)key.data(), key.size(),
                                                   (const uint8_t*)value.data(), value.size(),
                                                   cf);
        if (!future) {
            throw std::runtime_error("Failed to create set future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Set operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Set operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
    }
    
    std::string get(const std::string& key, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_get(handle,
                                                   (const uint8_t*)key.data(), key.size(),
                                                   cf);
        if (!future) {
            throw std::runtime_error("Failed to create get future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Get operation timeout");
        }
        
        KvBinaryData value;
        KvResult result = kv_future_get_value_result(future, &value);
        if (!result.success) {
            std::string error = "Get operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        std::string str_value;
        if (value.data && value.length > 0) {
            str_value = std::string((char*)value.data, value.length);
            kv_binary_free(&value);
        }
        
        return str_value;
    }
    
    void delete_key(const std::string& key, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_delete(handle,
                                                      (const uint8_t*)key.data(), key.size(),
                                                      cf);
        if (!future) {
            throw std::runtime_error("Failed to create delete future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Delete operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Delete operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
    }
    
    
    std::vector<std::pair<std::string, std::string>> get_range(const std::string& start_key, 
                                                                const std::string* end_key = nullptr, 
                                                                int limit = -1,
                                                                const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        
        const uint8_t* end_key_data = end_key ? (const uint8_t*)end_key->data() : nullptr;
        int end_key_len = end_key ? end_key->size() : 0;
        
        KvFutureHandle future = kv_transaction_get_range(handle,
                                                         (const uint8_t*)start_key.data(), start_key.size(),
                                                         end_key_data, end_key_len,
                                                         limit, cf);
        if (!future) {
            throw std::runtime_error("Failed to create get_range future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Get range operation timeout");
        }
        
        KvPairArray pairs = {nullptr, 0};
        KvResult result = kv_future_get_kv_array_result(future, &pairs);
        if (!result.success) {
            std::string error = "Get range operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        std::vector<std::pair<std::string, std::string>> kv_vec;
        for (size_t i = 0; i < pairs.count; i++) {
            std::string key_str = pairs.pairs[i].key.data && pairs.pairs[i].key.length > 0 ?
                std::string((char*)pairs.pairs[i].key.data, pairs.pairs[i].key.length) : "";
            std::string value_str = pairs.pairs[i].value.data && pairs.pairs[i].value.length > 0 ?
                std::string((char*)pairs.pairs[i].value.data, pairs.pairs[i].value.length) : "";
            kv_vec.emplace_back(key_str, value_str);
        }
        
        kv_pair_array_free(&pairs);
        return kv_vec;
    }
    
    void commit() {
        KvFutureHandle future = kv_transaction_commit(handle);
        if (!future) {
            throw std::runtime_error("Failed to create commit future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Commit operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Commit operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        committed = true;
    }
    
    KvTransactionWrapper(const KvTransactionWrapper&) = delete;
    KvTransactionWrapper& operator=(const KvTransactionWrapper&) = delete;
};

// Test C++ basic functionality
void test_cpp_basic_functionality() {
    FFITest test("C++ Basic Functionality");
    
    int init_result = kv_init();
    test.assert_true(init_result == 0, "kv_init failed");
    
    KvClientHandle client = kv_client_create("localhost:9090");
    test.assert_true(client != nullptr, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    test.assert_true(tx_future != nullptr, "Failed to begin transaction");
    
    bool ready = wait_for_future_cpp(tx_future);
    test.assert_true(ready, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    test.assert_true(tx != nullptr, "Failed to get transaction handle");
    
    // Set a key-value pair
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("cpp_test_key"), KV_STR("cpp_test_value"), nullptr);
    test.assert_true(set_future != nullptr, "Failed to create set future");
    
    ready = wait_for_future_cpp(set_future);
    test.assert_true(ready, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    test.assert_true(set_result.success == 1, "Set operation failed");
    
    // Get the value back
    KvFutureHandle get_future = kv_transaction_get(tx, KV_STR("cpp_test_key"), nullptr);
    test.assert_true(get_future != nullptr, "Failed to create get future");
    
    ready = wait_for_future_cpp(get_future);
    test.assert_true(ready, "Get future not ready");
    
    KvBinaryData value;
    KvResult get_result = kv_future_get_value_result(get_future, &value);
    test.assert_true(get_result.success == 1, "Get operation failed");
    test.assert_true(value.data != nullptr, "Got NULL value");
    test.assert_true(value.length == strlen("cpp_test_value"), "Value length mismatch");
    test.assert_true(memcmp(value.data, "cpp_test_value", strlen("cpp_test_value")) == 0, "Value mismatch");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    test.assert_true(commit_future != nullptr, "Failed to create commit future");
    
    ready = wait_for_future_cpp(commit_future);
    test.assert_true(ready, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    test.assert_true(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_binary_free(&value);
    kv_client_destroy(client);
    kv_shutdown();
    
    test.pass();
}

// Test C++ RAII wrapper
void test_cpp_wrapper() {
    FFITest test("C++ RAII Wrapper");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        {
            KvTransactionWrapper tx(client);
            
            // Test multiple operations
            tx.set("wrapper_key_1", "wrapper_value_1");
            tx.set("wrapper_key_2", "wrapper_value_2");
            
            std::string value1 = tx.get("wrapper_key_1");
            std::string value2 = tx.get("wrapper_key_2");
            
            test.assert_true(value1 == "wrapper_value_1", "Value1 mismatch");
            test.assert_true(value2 == "wrapper_value_2", "Value2 mismatch");
            
            tx.commit();
        } // Transaction auto-destructs here
        
        // Verify data persisted
        {
            KvTransactionWrapper tx(client);
            
            std::string value1 = tx.get("wrapper_key_1");
            test.assert_true(value1 == "wrapper_value_1", "Persisted value1 mismatch");
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test error handling
void test_cpp_error_handling() {
    FFITest test("C++ Error Handling");
    
    kv_init();
    
    // Test with invalid client
    KvFutureHandle future = kv_transaction_begin(nullptr, 30);
    test.assert_true(future == nullptr, "Should fail with NULL client");
    
    // Test with non-existent server
    KvClientHandle bad_client = kv_client_create("localhost:19999");
    test.assert_true(bad_client == nullptr, "Should fail with bad address");
    
    // Test polling NULL future
    int result = kv_future_poll(nullptr);
    test.assert_true(result == -1, "Should return -1 for NULL future");
    
    kv_shutdown();
    test.pass();
}

// Test concurrent operations
void test_cpp_concurrent_operations() {
    FFITest test("C++ Concurrent Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Create multiple transactions in parallel
        std::vector<std::thread> threads;
        std::vector<bool> results(4, false);
        
        for (int i = 0; i < 4; i++) {
            threads.emplace_back([&, i]() {
                try {
                    KvTransactionWrapper tx(client);
                    
                    std::string key = "concurrent_key_" + std::to_string(i);
                    std::string value = "concurrent_value_" + std::to_string(i);
                    
                    tx.set(key, value);
                    
                    std::string retrieved = tx.get(key);
                    if (retrieved == value) {
                        tx.commit();
                        results[i] = true;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Thread " << i << " error: " << e.what() << std::endl;
                }
            });
        }
        
        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
        
        // Check results
        for (int i = 0; i < 4; i++) {
            test.assert_true(results[i], "Thread " + std::to_string(i) + " failed");
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test C++ range operations
void test_cpp_range_operations() {
    FFITest test("C++ Range Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Set up test data
        {
            KvTransactionWrapper tx(client);
            
            tx.set("cpp_range_key_001", "cpp_value1");
            tx.set("cpp_range_key_002", "cpp_value2");
            tx.set("cpp_range_key_003", "cpp_value3");
            tx.set("cpp_range_key_004", "cpp_value4");
            tx.set("other_prefix", "other_value");  // Should not appear in range
            
            tx.commit();
        }
        
        // Test range operations
        {
            KvTransactionWrapper tx(client);
            
            // Test 1: Basic range query with prefix
            auto range_results = tx.get_range("cpp_range_key");
            test.assert_true(range_results.size() == 4, "Expected 4 results from prefix range");
            
            for (size_t i = 0; i < range_results.size(); i++) {
                std::cout << "C++ Range result [" << i << "]: " << range_results[i].first 
                          << " = " << range_results[i].second << std::endl;
            }
            
            // Test 2: Range query with end key
            std::string end_key = "cpp_range_key_003";
            auto bounded_results = tx.get_range("cpp_range_key_001", &end_key);
            test.assert_true(bounded_results.size() == 2, "Expected 2 results from bounded range (exclusive end)");
            test.assert_true(bounded_results[0].first == "cpp_range_key_001", "First key should be cpp_range_key_001");
            test.assert_true(bounded_results[1].first == "cpp_range_key_002", "Second key should be cpp_range_key_002");
            
            // Test 3: Limited range query
            auto limited_results = tx.get_range("cpp_range_key", nullptr, 2);
            test.assert_true(limited_results.size() == 2, "Expected 2 results from limited range");
            
            // Test 4: Empty range query
            auto empty_results = tx.get_range("nonexistent_prefix");
            test.assert_true(empty_results.size() == 0, "Expected 0 results from empty range");
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

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

// Test C++ deletion operations
void test_cpp_deletion_operations() {
    FFITest test("C++ Deletion Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test 1: Basic deletion using async method
        {
            KvTransactionWrapper tx(client);
            
            tx.set("cpp_delete_test1", "cpp_delete_value1");
            
            // Verify key exists
            std::string retrieved = tx.get("cpp_delete_test1");
            test.assert_true(retrieved == "cpp_delete_value1", "Key should exist before deletion");
            
            // Delete the key
            tx.delete_key("cpp_delete_test1");
            
            // Verify key no longer exists (should return empty string)
            std::string after_delete = tx.get("cpp_delete_test1");
            test.assert_true(after_delete.empty(), "Key should not exist after deletion");
            
            tx.commit();
        }
        
        // Test 2: Multiple deletions in same transaction
        {
            KvTransactionWrapper tx(client);
            
            tx.set("cpp_multi_delete1", "value1");
            tx.set("cpp_multi_delete2", "value2");
            tx.set("cpp_multi_delete3", "value3");
            
            // Verify all keys exist
            test.assert_true(tx.get("cpp_multi_delete1") == "value1", "Key 1 should exist");
            test.assert_true(tx.get("cpp_multi_delete2") == "value2", "Key 2 should exist");
            test.assert_true(tx.get("cpp_multi_delete3") == "value3", "Key 3 should exist");
            
            // Delete all keys using async method
            tx.delete_key("cpp_multi_delete1");
            tx.delete_key("cpp_multi_delete2");
            tx.delete_key("cpp_multi_delete3");
            
            // Verify all keys are deleted
            test.assert_true(tx.get("cpp_multi_delete1").empty(), "Key 1 should be deleted");
            test.assert_true(tx.get("cpp_multi_delete2").empty(), "Key 2 should be deleted");
            test.assert_true(tx.get("cpp_multi_delete3").empty(), "Key 3 should be deleted");
            
            tx.commit();
        }
        
        // Test 3: Delete non-existent key (should not fail)
        {
            KvTransactionWrapper tx(client);
            
            // This should not throw an exception
            tx.delete_key("non_existent_key");
            tx.delete_key("another_non_existent_key");
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test binary data with null bytes
int test_c_binary_nulls() {
    if (kv_init() != 0) {
        printf("FAIL: test_c_binary_nulls - kv_init failed\n");
        return 1;
    }
    
    KvClientHandle client = kv_client_create("localhost:9090");
    if (client == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to create client\n");
        kv_shutdown();
        return 1;
    }
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    if (tx_future == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to begin transaction\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (wait_for_future_c(tx_future) != 1) {
        printf("FAIL: test_c_binary_nulls - Transaction begin timeout\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    if (tx == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to get transaction handle\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    // Test binary data with null bytes
    const char test_key[] = "key\x00with\x00nulls\x00bytes";
    int key_len = 23; // Length including null bytes
    
    const char test_value[] = "value\x00has\x00null\x00bytes\x00inside";
    int value_len = 31; // Length including null bytes
    
    // Set the key-value pair with binary data containing null bytes
    KvFutureHandle set_future = kv_transaction_set(tx, 
        (const uint8_t*)test_key, key_len,
        (const uint8_t*)test_value, value_len,
        NULL);
    
    if (set_future == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to create set future\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (wait_for_future_c(set_future) != 1) {
        printf("FAIL: test_c_binary_nulls - Set operation timeout\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    KvResult set_result = kv_future_get_void_result(set_future);
    if (set_result.success != 1) {
        printf("FAIL: test_c_binary_nulls - Set operation failed\n");
        if (set_result.error_message) {
            printf("Error: %s\n", set_result.error_message);
        }
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    // Get the value back
    KvFutureHandle get_future = kv_transaction_get(tx, 
        (const uint8_t*)test_key, key_len, NULL);
    
    if (get_future == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to create get future\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (wait_for_future_c(get_future) != 1) {
        printf("FAIL: test_c_binary_nulls - Get operation timeout\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    KvBinaryData retrieved_value;
    KvResult get_result = kv_future_get_value_result(get_future, &retrieved_value);
    if (get_result.success != 1) {
        printf("FAIL: test_c_binary_nulls - Get operation failed\n");
        if (get_result.error_message) {
            printf("Error: %s\n", get_result.error_message);
        }
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    // Verify the retrieved value
    if (retrieved_value.data == NULL) {
        printf("FAIL: test_c_binary_nulls - Retrieved value is null\n");
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (retrieved_value.length != value_len) {
        printf("FAIL: test_c_binary_nulls - Value length mismatch - expected %d, got %d\n", 
               value_len, (int)retrieved_value.length);
        kv_binary_free(&retrieved_value);
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (memcmp(retrieved_value.data, test_value, value_len) != 0) {
        printf("FAIL: test_c_binary_nulls - Value content mismatch\n");
        kv_binary_free(&retrieved_value);
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    if (commit_future == NULL) {
        printf("FAIL: test_c_binary_nulls - Failed to create commit future\n");
        kv_binary_free(&retrieved_value);
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    if (wait_for_future_c(commit_future) != 1) {
        printf("FAIL: test_c_binary_nulls - Commit timeout\n");
        kv_binary_free(&retrieved_value);
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    if (commit_result.success != 1) {
        printf("FAIL: test_c_binary_nulls - Commit failed\n");
        if (commit_result.error_message) {
            printf("Error: %s\n", commit_result.error_message);
        }
        kv_binary_free(&retrieved_value);
        kv_client_destroy(client);
        kv_shutdown();
        return 1;
    }
    
    // Clean up
    kv_binary_free(&retrieved_value);
    kv_client_destroy(client);
    kv_shutdown();
    
    printf("PASS: test_c_binary_nulls\n");
    return 0;
}

//==============================================================================
// Main Test Runner
//===============================================================================

int main() {
    std::cout << "Running KV Store Unified C/C++ FFI Tests" << std::endl;
    std::cout << "=========================================" << std::endl;
    
    // C-style test functions
    typedef int (*c_test_func)();
    typedef struct {
        const char* name;
        c_test_func func;
    } c_test_case;
    
    c_test_case c_tests[] = {
        {"C Init/Shutdown", test_c_init_shutdown},
        {"C Client Lifecycle", test_c_client_lifecycle},
        {"C Configuration", test_c_configuration},
        {"C Debug Configuration", test_c_debug_configuration},
        {"C Invalid Configuration", test_c_invalid_configuration},
        {"C Basic Transaction", test_c_basic_transaction},
        {"C Read Transaction", test_c_read_transaction},
        {"C Client Ping", test_c_client_ping},
        {"C Transaction Abort", test_c_transaction_abort},
        {"C Range Operations", test_c_range_operations},
        {"C Async Deletion", test_c_async_deletion},
        {"C Binary Nulls", test_c_binary_nulls},
        {NULL, NULL}
    };
    
    // C++-style test functions
    std::vector<std::pair<std::string, std::function<void()>>> cpp_tests = {
        {"C++ Basic Functionality", test_cpp_basic_functionality},
        {"C++ Configuration", test_cpp_configuration},
        {"C++ Configuration Errors", test_cpp_configuration_errors},
        {"C++ RAII Wrapper", test_cpp_wrapper},
        {"C++ Error Handling", test_cpp_error_handling},
        {"C++ Concurrent Operations", test_cpp_concurrent_operations},
        {"C++ Range Operations", test_cpp_range_operations},
        {"C++ Deletion Operations", test_cpp_deletion_operations}
    };
    
    int passed = 0;
    int failed = 0;
    int test_counter = 1;
    
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
        std::cout << "Start the server with: ./bin/rocksdbserver-thrift" << std::endl;
        return 1;
    }
    
    return 0;
}