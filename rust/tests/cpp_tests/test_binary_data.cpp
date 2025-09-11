#include "test_common.hpp"

//==============================================================================
// Binary Data Tests
//==============================================================================

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

// Test large binary data handling
int test_c_large_binary_data() {
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
    
    // Create large binary data (1MB)
    const size_t large_size = 1024 * 1024;
    uint8_t* large_data = (uint8_t*)malloc(large_size);
    TEST_ASSERT(large_data != NULL, "Failed to allocate large data buffer");
    
    // Fill with pattern including null bytes
    for (size_t i = 0; i < large_size; i++) {
        large_data[i] = (uint8_t)(i % 256);
    }
    
    // Set large binary data
    KvFutureHandle set_future = kv_transaction_set(tx, 
        KV_STR("large_binary_key"),
        large_data, large_size, NULL);
    TEST_ASSERT(set_future != NULL, "Failed to create set future for large data");
    
    ready = wait_for_future_c(set_future);
    TEST_ASSERT(ready == 1, "Large data set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    TEST_ASSERT(set_result.success == 1, "Large data set operation failed");
    
    // Retrieve and verify large binary data
    KvFutureHandle get_future = kv_transaction_get(tx, KV_STR("large_binary_key"), NULL);
    TEST_ASSERT(get_future != NULL, "Failed to create get future for large data");
    
    ready = wait_for_future_c(get_future);
    TEST_ASSERT(ready == 1, "Large data get future not ready");
    
    KvBinaryData retrieved_data;
    KvResult get_result = kv_future_get_value_result(get_future, &retrieved_data);
    TEST_ASSERT(get_result.success == 1, "Large data get operation failed");
    TEST_ASSERT(retrieved_data.data != NULL, "Retrieved large data is null");
    TEST_ASSERT(retrieved_data.length == large_size, "Large data length mismatch");
    TEST_ASSERT(memcmp(retrieved_data.data, large_data, large_size) == 0, "Large data content mismatch");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_binary_free(&retrieved_data);
    free(large_data);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test empty and zero-length binary data
int test_c_empty_binary_data() {
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
    
    // Test 1: Empty value (zero length)
    KvFutureHandle set_future1 = kv_transaction_set(tx, 
        KV_STR("empty_value_key"),
        NULL, 0, NULL);
    TEST_ASSERT(set_future1 != NULL, "Failed to create set future for empty value");
    
    ready = wait_for_future_c(set_future1);
    TEST_ASSERT(ready == 1, "Empty value set future not ready");
    
    KvResult set_result1 = kv_future_get_void_result(set_future1);
    TEST_ASSERT(set_result1.success == 1, "Empty value set operation failed");
    
    // Retrieve empty value
    KvFutureHandle get_future1 = kv_transaction_get(tx, KV_STR("empty_value_key"), NULL);
    TEST_ASSERT(get_future1 != NULL, "Failed to create get future for empty value");
    
    ready = wait_for_future_c(get_future1);
    TEST_ASSERT(ready == 1, "Empty value get future not ready");
    
    KvBinaryData empty_value;
    KvResult get_result1 = kv_future_get_value_result(get_future1, &empty_value);
    TEST_ASSERT(get_result1.success == 1, "Empty value get operation failed");
    // Empty values may return NULL or empty data - both are valid
    TEST_ASSERT(empty_value.length == 0, "Empty value should have zero length");
    
    if (empty_value.data) {
        kv_binary_free(&empty_value);
    }
    
    // Test 2: Single null byte value
    const uint8_t null_byte = 0;
    KvFutureHandle set_future2 = kv_transaction_set(tx, 
        KV_STR("null_byte_key"),
        &null_byte, 1, NULL);
    TEST_ASSERT(set_future2 != NULL, "Failed to create set future for null byte value");
    
    ready = wait_for_future_c(set_future2);
    TEST_ASSERT(ready == 1, "Null byte set future not ready");
    
    KvResult set_result2 = kv_future_get_void_result(set_future2);
    TEST_ASSERT(set_result2.success == 1, "Null byte set operation failed");
    
    // Retrieve null byte value
    KvFutureHandle get_future2 = kv_transaction_get(tx, KV_STR("null_byte_key"), NULL);
    TEST_ASSERT(get_future2 != NULL, "Failed to create get future for null byte");
    
    ready = wait_for_future_c(get_future2);
    TEST_ASSERT(ready == 1, "Null byte get future not ready");
    
    KvBinaryData null_value;
    KvResult get_result2 = kv_future_get_value_result(get_future2, &null_value);
    TEST_ASSERT(get_result2.success == 1, "Null byte get operation failed");
    TEST_ASSERT(null_value.data != NULL, "Null byte value data should not be null");
    TEST_ASSERT(null_value.length == 1, "Null byte value should have length 1");
    TEST_ASSERT(((uint8_t*)null_value.data)[0] == 0, "Null byte value should be 0");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    TEST_ASSERT(commit_future != NULL, "Failed to create commit future");
    
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_binary_free(&null_value);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

// Test C++ binary data operations with STL containers
void test_cpp_binary_data_operations() {
    FFITest test("C++ Binary Data Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Test 1: Binary data using std::vector
        std::vector<uint8_t> binary_data;
        for (int i = 0; i < 256; i++) {
            binary_data.push_back(static_cast<uint8_t>(i));
        }
        
        // Store binary data
        KvFutureHandle set_future = kv_transaction_set(tx.get(),
            KV_STR("cpp_binary_vector"),
            binary_data.data(), binary_data.size(), nullptr);
        test.assert_true(set_future != nullptr, "Failed to create set future for binary vector");
        
        bool ready = wait_for_future_cpp(set_future);
        test.assert_true(ready, "Binary vector set future not ready");
        
        KvResult set_result = kv_future_get_void_result(set_future);
        test.assert_true(set_result.success == 1, "Binary vector set operation failed");
        
        // Retrieve binary data
        KvFutureHandle get_future = kv_transaction_get(tx.get(), KV_STR("cpp_binary_vector"), nullptr);
        test.assert_true(get_future != nullptr, "Failed to create get future for binary vector");
        
        ready = wait_for_future_cpp(get_future);
        test.assert_true(ready, "Binary vector get future not ready");
        
        KvBinaryData retrieved_data;
        KvResult get_result = kv_future_get_value_result(get_future, &retrieved_data);
        test.assert_true(get_result.success == 1, "Binary vector get operation failed");
        test.assert_true(retrieved_data.data != nullptr, "Retrieved binary data is null");
        test.assert_true(retrieved_data.length == binary_data.size(), "Binary data length mismatch");
        
        // Verify content
        bool content_matches = memcmp(retrieved_data.data, binary_data.data(), binary_data.size()) == 0;
        test.assert_true(content_matches, "Binary data content mismatch");
        
        // Test 2: String with embedded nulls using raw string literals
        std::string null_string = "Hello\x00World\x00Test\x00String";
        // Note: We need to explicitly set the size since std::string constructor
        // will stop at the first null when constructed from c-string
        null_string = std::string("Hello\x00World\x00Test\x00String", 23);
        
        KvFutureHandle set_future2 = kv_transaction_set(tx.get(),
            KV_STR("cpp_null_string"),
            (const uint8_t*)null_string.data(), null_string.size(), nullptr);
        test.assert_true(set_future2 != nullptr, "Failed to create set future for null string");
        
        ready = wait_for_future_cpp(set_future2);
        test.assert_true(ready, "Null string set future not ready");
        
        KvResult set_result2 = kv_future_get_void_result(set_future2);
        test.assert_true(set_result2.success == 1, "Null string set operation failed");
        
        // Retrieve string with nulls
        KvFutureHandle get_future2 = kv_transaction_get(tx.get(), KV_STR("cpp_null_string"), nullptr);
        test.assert_true(get_future2 != nullptr, "Failed to create get future for null string");
        
        ready = wait_for_future_cpp(get_future2);
        test.assert_true(ready, "Null string get future not ready");
        
        KvBinaryData retrieved_string;
        KvResult get_result2 = kv_future_get_value_result(get_future2, &retrieved_string);
        test.assert_true(get_result2.success == 1, "Null string get operation failed");
        test.assert_true(retrieved_string.data != nullptr, "Retrieved null string is null");
        test.assert_true(retrieved_string.length == null_string.size(), "Null string length mismatch");
        
        // Verify null string content
        std::string retrieved_null_string((char*)retrieved_string.data, retrieved_string.length);
        test.assert_true(retrieved_null_string == null_string, "Null string content mismatch");
        test.assert_true(retrieved_null_string.size() == 23, "Null string should have full length including nulls");
        
        tx.commit();
        
        // Cleanup
        kv_binary_free(&retrieved_data);
        kv_binary_free(&retrieved_string);
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}