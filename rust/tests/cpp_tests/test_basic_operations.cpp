#include "test_common.hpp"

//==============================================================================
// Basic C-style Tests
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
// Basic C++ Tests  
//==============================================================================

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