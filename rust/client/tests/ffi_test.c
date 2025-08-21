#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include "../include/kvstore_client.h"

// Test runner macros
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "FAIL: %s - %s\n", __func__, message); \
            return -1; \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf("PASS: %s\n", __func__); \
        return 0; \
    } while(0)

// Helper function to wait for future completion
int wait_for_future(KvFutureHandle future) {
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

// Test basic initialization and cleanup
int test_init_shutdown() {
    int result = kv_init();
    TEST_ASSERT(result == 1, "kv_init failed");
    
    kv_shutdown();
    TEST_PASS();
}

// Test client creation and destruction
int test_client_lifecycle() {
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
}

// Test basic transaction operations
int test_basic_transaction() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction");
    
    int ready = wait_for_future(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Set a key-value pair
    KvFutureHandle set_future = kv_transaction_set(tx, "test_key", "test_value", NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future");
    
    ready = wait_for_future(set_future);
    TEST_ASSERT(ready == 1, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Set operation failed");
    
    // Get the value back
    KvFutureHandle get_future = kv_transaction_get(tx, "test_key", NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create get future");
    
    ready = wait_for_future(get_future);
    TEST_ASSERT(ready == 1, "Get future not ready");
    
    char* value = NULL;
    KvResult get_result = kv_future_get_string_result(get_future, &value);
    TEST_ASSERT(get_result.success == 1, "Get operation failed");
    TEST_ASSERT(value != NULL, "Got NULL value");
    TEST_ASSERT(strcmp(value, "test_value") == 0, "Value mismatch");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_string_free(value);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
}

// Test read transaction
int test_read_transaction() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // First, set up some data with a regular transaction
    KvFutureHandle setup_tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(setup_tx_future != NULL, "Failed to begin setup transaction");
    
    int ready = wait_for_future(setup_tx_future);
    TEST_ASSERT(ready == 1, "Setup transaction begin future not ready");
    
    KvTransactionHandle setup_tx = kv_future_get_transaction(setup_tx_future);
    TEST_ASSERT(setup_tx != NULL, "Failed to get setup transaction handle");
    
    KvFutureHandle set_future = kv_transaction_set(setup_tx, "read_test_key", "read_test_value", NULL);
    ready = wait_for_future(set_future);
    TEST_ASSERT(ready == 1, "Setup set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Setup set operation failed");
    
    KvFutureHandle commit_future = kv_transaction_commit(setup_tx);
    ready = wait_for_future(commit_future);
    TEST_ASSERT(ready == 1, "Setup commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Setup commit operation failed");
    
    // Now test read transaction
    KvFutureHandle read_tx_future = kv_read_transaction_begin(client, -1);
    TEST_ASSERT(read_tx_future != NULL, "Failed to begin read transaction");
    
    ready = wait_for_future(read_tx_future);
    TEST_ASSERT(ready == 1, "Read transaction begin future not ready");
    
    KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
    TEST_ASSERT(read_tx != NULL, "Failed to get read transaction handle");
    
    // Read the value (note: using regular get for now, snapshot_get might need different FFI binding)
    KvFutureHandle get_future = kv_read_transaction_get(read_tx, "read_test_key", NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create read get future");
    
    ready = wait_for_future(get_future);
    TEST_ASSERT(ready == 1, "Read get future not ready");
    
    char* value = NULL;
    KvResult get_result = kv_future_get_string_result(get_future, &value);
    TEST_ASSERT(get_result.success == 1, "Read get operation failed");
    TEST_ASSERT(value != NULL, "Got NULL value from read transaction");
    TEST_ASSERT(strcmp(value, "read_test_value") == 0, "Read value mismatch");
    
    // Cleanup
    kv_string_free(value);
    kv_read_transaction_destroy(read_tx);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
}

// Test error handling
int test_error_handling() {
    kv_init();
    
    // Test with invalid client
    KvFutureHandle future = kv_transaction_begin(NULL, 30);
    TEST_ASSERT(future == NULL, "Should fail with NULL client");
    
    // Test with non-existent server
    KvClientHandle bad_client = kv_client_create("localhost:19999");
    TEST_ASSERT(bad_client == NULL, "Should fail with bad address");
    
    kv_shutdown();
    TEST_PASS();
}

// Test string memory management
int test_string_memory() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    int ready = wait_for_future(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Set and get multiple keys to test string management
    const char* keys[] = {"mem_test_1", "mem_test_2", "mem_test_3"};
    const char* values[] = {"value_1", "value_2", "value_3"};
    char* retrieved_values[3] = {NULL, NULL, NULL};
    
    for (int i = 0; i < 3; i++) {
        // Set
        KvFutureHandle set_future = kv_transaction_set(tx, keys[i], values[i], NULL);
        ready = wait_for_future(set_future);
        TEST_ASSERT(ready == 1, "Set future not ready");
        
        KvResult set_result = kv_future_get_void_result(set_future);
        TEST_ASSERT(set_result.success == 1, "Set operation failed");
        
        // Get
        KvFutureHandle get_future = kv_transaction_get(tx, keys[i], NULL);
        ready = wait_for_future(get_future);
        TEST_ASSERT(ready == 1, "Get future not ready");
        
        KvResult get_result = kv_future_get_string_result(get_future, &retrieved_values[i]);
        TEST_ASSERT(get_result.success == 1, "Get operation failed");
        TEST_ASSERT(retrieved_values[i] != NULL, "Got NULL value");
        TEST_ASSERT(strcmp(retrieved_values[i], values[i]) == 0, "Value mismatch");
    }
    
    // Commit
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    ready = wait_for_future(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Free all strings
    for (int i = 0; i < 3; i++) {
        kv_string_free(retrieved_values[i]);
    }
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
}

// Test future polling edge cases
int test_future_polling() {
    kv_init();
    
    // Test polling NULL future
    int result = kv_future_poll(NULL);
    TEST_ASSERT(result == -1, "Should return -1 for NULL future");
    
    // Test getting result from NULL future
    KvResult void_result = kv_future_get_void_result(NULL);
    TEST_ASSERT(void_result.success == 0, "Should fail for NULL future");
    
    char* value = NULL;
    KvResult string_result = kv_future_get_string_result(NULL, &value);
    TEST_ASSERT(string_result.success == 0, "Should fail for NULL future");
    
    kv_shutdown();
    TEST_PASS();
}

// Test multiple concurrent transactions
int test_concurrent_transactions() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Start two transactions
    KvFutureHandle tx1_future = kv_transaction_begin(client, 30);
    KvFutureHandle tx2_future = kv_transaction_begin(client, 30);
    
    int ready1 = wait_for_future(tx1_future);
    int ready2 = wait_for_future(tx2_future);
    TEST_ASSERT(ready1 == 1 && ready2 == 1, "Transaction futures not ready");
    
    KvTransactionHandle tx1 = kv_future_get_transaction(tx1_future);
    KvTransactionHandle tx2 = kv_future_get_transaction(tx2_future);
    TEST_ASSERT(tx1 != NULL && tx2 != NULL, "Failed to get transaction handles");
    
    // Use different keys to avoid conflicts
    KvFutureHandle set1_future = kv_transaction_set(tx1, "concurrent_key_1", "value_1", NULL);
    KvFutureHandle set2_future = kv_transaction_set(tx2, "concurrent_key_2", "value_2", NULL);
    
    ready1 = wait_for_future(set1_future);
    ready2 = wait_for_future(set2_future);
    TEST_ASSERT(ready1 == 1 && ready2 == 1, "Set futures not ready");
    
    KvResult set1_result = kv_future_get_void_result(set1_future);
    KvResult set2_result = kv_future_get_void_result(set2_future);
    TEST_ASSERT(set1_result.success == 1 && set2_result.success == 1, "Set operations failed");
    
    // Commit both transactions
    KvFutureHandle commit1_future = kv_transaction_commit(tx1);
    KvFutureHandle commit2_future = kv_transaction_commit(tx2);
    
    ready1 = wait_for_future(commit1_future);
    ready2 = wait_for_future(commit2_future);
    TEST_ASSERT(ready1 == 1 && ready2 == 1, "Commit futures not ready");
    
    KvResult commit1_result = kv_future_get_void_result(commit1_future);
    KvResult commit2_result = kv_future_get_void_result(commit2_future);
    TEST_ASSERT(commit1_result.success == 1 && commit2_result.success == 1, "Commit operations failed");
    
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
}

// Main test runner
int main() {
    printf("Running KV Store C FFI Tests\n");
    printf("=============================\n");
    
    typedef int (*test_func)();
    typedef struct {
        const char* name;
        test_func func;
    } test_case;
    
    test_case tests[] = {
        {"Init/Shutdown", test_init_shutdown},
        {"Client Lifecycle", test_client_lifecycle},
        {"Basic Transaction", test_basic_transaction},
        {"Read Transaction", test_read_transaction},
        {"Error Handling", test_error_handling},
        {"String Memory Management", test_string_memory},
        {"Future Polling", test_future_polling},
        {"Concurrent Transactions", test_concurrent_transactions},
        {NULL, NULL}
    };
    
    int passed = 0;
    int failed = 0;
    
    for (int i = 0; tests[i].name != NULL; i++) {
        printf("\n[%d] Running %s...\n", i + 1, tests[i].name);
        int result = tests[i].func();
        if (result == 0) {
            passed++;
        } else {
            failed++;
        }
    }
    
    printf("\n=============================\n");
    printf("Test Results: %d passed, %d failed\n", passed, failed);
    
    if (failed > 0) {
        printf("\nNote: Some tests may fail if the KV server is not running on localhost:9090\n");
        printf("Start the server with: ./bin/rocksdbserver-thrift\n");
        return 1;
    }
    
    return 0;
}