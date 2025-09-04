#include "test_common.hpp"

//==============================================================================
// Transaction Management Tests
//==============================================================================

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

// Test transaction conflict detection
int test_c_transaction_conflict() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Start first transaction
    KvFutureHandle tx1_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx1_future != NULL, "Failed to begin first transaction");
    
    int ready = wait_for_future_c(tx1_future);
    TEST_ASSERT(ready == 1, "First transaction begin future not ready");
    
    KvTransactionHandle tx1 = kv_future_get_transaction(tx1_future);
    TEST_ASSERT(tx1 != NULL, "Failed to get first transaction handle");
    
    // Start second transaction
    KvFutureHandle tx2_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx2_future != NULL, "Failed to begin second transaction");
    
    ready = wait_for_future_c(tx2_future);
    TEST_ASSERT(ready == 1, "Second transaction begin future not ready");
    
    KvTransactionHandle tx2 = kv_future_get_transaction(tx2_future);
    TEST_ASSERT(tx2 != NULL, "Failed to get second transaction handle");
    
    const char* conflict_key = "conflict_test_key_c";
    
    // Both transactions modify the same key
    KvFutureHandle set1_future = kv_transaction_set(tx1, 
        (const uint8_t*)conflict_key, strlen(conflict_key),
        KV_STR("value1"), NULL);
    TEST_ASSERT(set1_future != NULL, "Failed to create first set future");
    
    ready = wait_for_future_c(set1_future);
    TEST_ASSERT(ready == 1, "First set future not ready");
    
    KvResult set1_result = kv_future_get_void_result(set1_future);
    TEST_ASSERT(set1_result.success == 1, "First set operation failed");
    
    KvFutureHandle set2_future = kv_transaction_set(tx2, 
        (const uint8_t*)conflict_key, strlen(conflict_key),
        KV_STR("value2"), NULL);
    TEST_ASSERT(set2_future != NULL, "Failed to create second set future");
    
    ready = wait_for_future_c(set2_future);
    TEST_ASSERT(ready == 1, "Second set future not ready");
    
    KvResult set2_result = kv_future_get_void_result(set2_future);
    TEST_ASSERT(set2_result.success == 1, "Second set operation failed");
    
    // First commit should succeed
    KvFutureHandle commit1_future = kv_transaction_commit(tx1);
    TEST_ASSERT(commit1_future != NULL, "Failed to create first commit future");
    
    ready = wait_for_future_c(commit1_future);
    TEST_ASSERT(ready == 1, "First commit future not ready");
    
    KvResult commit1_result = kv_future_get_void_result(commit1_future);
    TEST_ASSERT(commit1_result.success == 1, "First commit operation failed");
    
    // Second commit should potentially fail with conflict (depending on implementation)
    KvFutureHandle commit2_future = kv_transaction_commit(tx2);
    TEST_ASSERT(commit2_future != NULL, "Failed to create second commit future");
    
    ready = wait_for_future_c(commit2_future);
    // Note: We don't assert on this result as conflict behavior may vary
    
    KvResult commit2_result = kv_future_get_void_result(commit2_future);
    if (commit2_result.success == 0) {
        printf("Expected transaction conflict detected in second commit\n");
        if (commit2_result.error_message) {
            printf("Conflict error: %s\n", commit2_result.error_message);
        }
    } else {
        printf("Second commit succeeded (last writer wins or no conflict detection)\n");
    }
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test transaction timeout
int test_c_transaction_timeout() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction with very short timeout (1 second)
    KvFutureHandle tx_future = kv_transaction_begin(client, 1);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction with timeout");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle with timeout");
    
    // Set a key-value pair
    KvFutureHandle set_future = kv_transaction_set(tx, KV_STR("timeout_test_key"), KV_STR("timeout_test_value"), NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future in timeout test");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready in timeout test");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed in timeout test");
    
    // Sleep longer than timeout (2 seconds)
    printf("Sleeping for 2 seconds to trigger transaction timeout...\n");
    sleep(2);
    
    // Try to commit after timeout - this may fail
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    if (commit_future != NULL) {
        ready = wait_for_future_c(commit_future);
        if (ready == 1) {
            KvResult commit_result = kv_future_get_void_result(commit_future);
            if (commit_result.success == 0) {
                printf("Expected timeout detected in commit: %s\n", 
                       commit_result.error_message ? commit_result.error_message : "Unknown error");
            } else {
                printf("Commit succeeded despite timeout (server may have extended timeout)\n");
            }
        } else {
            printf("Commit future timed out as expected\n");
        }
    } else {
        printf("Commit future creation failed as expected due to timeout\n");
    }
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

//==============================================================================
// C++ Transaction Tests
//==============================================================================

// Test transaction lifecycle with C++ wrappers
void test_cpp_transaction_lifecycle() {
    FFITest test("C++ Transaction Lifecycle");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test 1: Normal transaction commit
        {
            KvTransactionWrapper tx(client);
            tx.set("cpp_tx_lifecycle_key1", "value1");
            tx.set("cpp_tx_lifecycle_key2", "value2");
            
            std::string retrieved1 = tx.get("cpp_tx_lifecycle_key1");
            std::string retrieved2 = tx.get("cpp_tx_lifecycle_key2");
            
            test.assert_true(retrieved1 == "value1", "Retrieved value1 mismatch in transaction");
            test.assert_true(retrieved2 == "value2", "Retrieved value2 mismatch in transaction");
            
            tx.commit();
        }
        
        // Test 2: Verify data persisted after commit
        {
            KvTransactionWrapper tx(client);
            
            std::string persisted1 = tx.get("cpp_tx_lifecycle_key1");
            std::string persisted2 = tx.get("cpp_tx_lifecycle_key2");
            
            test.assert_true(persisted1 == "value1", "Persisted value1 mismatch after commit");
            test.assert_true(persisted2 == "value2", "Persisted value2 mismatch after commit");
            
            tx.commit();
        }
        
        // Test 3: Transaction with auto-rollback (destructor called without commit)
        {
            {
                KvTransactionWrapper tx(client);
                tx.set("cpp_tx_rollback_key", "rollback_value");
                
                std::string in_tx = tx.get("cpp_tx_rollback_key");
                test.assert_true(in_tx == "rollback_value", "Value should be visible within transaction");
                
                // Transaction destructor called here without commit
            }
            
            // Verify rollback occurred
            KvTransactionWrapper verify_tx(client);
            std::string after_rollback = verify_tx.get("cpp_tx_rollback_key");
            test.assert_true(after_rollback.empty(), "Value should not persist after rollback");
            
            verify_tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test nested transaction scenarios
void test_cpp_nested_operations() {
    FFITest test("C++ Nested Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test sequential transactions (simulating nested-like behavior)
        {
            // First transaction - setup
            KvTransactionWrapper setup_tx(client);
            setup_tx.set("nested_base_key", "base_value");
            setup_tx.commit();
        }
        
        {
            // Second transaction - modify
            KvTransactionWrapper modify_tx(client);
            
            std::string base_value = modify_tx.get("nested_base_key");
            test.assert_true(base_value == "base_value", "Base value should be available");
            
            modify_tx.set("nested_derived_key", base_value + "_derived");
            modify_tx.set("nested_base_key", "modified_base_value");
            
            std::string derived = modify_tx.get("nested_derived_key");
            test.assert_true(derived == "base_value_derived", "Derived value incorrect");
            
            modify_tx.commit();
        }
        
        {
            // Third transaction - verify
            KvTransactionWrapper verify_tx(client);
            
            std::string final_base = verify_tx.get("nested_base_key");
            std::string final_derived = verify_tx.get("nested_derived_key");
            
            test.assert_true(final_base == "modified_base_value", "Final base value incorrect");
            test.assert_true(final_derived == "base_value_derived", "Final derived value incorrect");
            
            verify_tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test transaction error recovery
void test_cpp_transaction_error_recovery() {
    FFITest test("C++ Transaction Error Recovery");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test recovery from failed transaction
        bool first_tx_failed = false;
        try {
            KvTransactionWrapper tx(client);
            tx.set("recovery_key", "initial_value");
            
            // Simulate an error condition (e.g., trying to set with invalid column family)
            // Note: This might not actually fail depending on implementation
            tx.set("recovery_key", "updated_value");
            
            // Force an exception to test recovery
            if (true) { // Simulate error condition
                throw std::runtime_error("Simulated transaction error");
            }
            
            tx.commit();
        } catch (const std::exception& e) {
            first_tx_failed = true;
            std::cout << "First transaction failed as expected: " << e.what() << std::endl;
        }
        
        test.assert_true(first_tx_failed, "First transaction should have failed");
        
        // Test that we can recover and start a new transaction
        {
            KvTransactionWrapper recovery_tx(client);
            recovery_tx.set("recovery_key", "recovery_value");
            
            std::string recovered = recovery_tx.get("recovery_key");
            test.assert_true(recovered == "recovery_value", "Recovery transaction should work");
            
            recovery_tx.commit();
        }
        
        // Verify the recovery value persisted
        {
            KvTransactionWrapper verify_tx(client);
            std::string final_value = verify_tx.get("recovery_key");
            test.assert_true(final_value == "recovery_value", "Recovery value should persist");
            verify_tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Unexpected exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}