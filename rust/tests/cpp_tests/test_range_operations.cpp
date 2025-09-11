#include "test_common.hpp"

//==============================================================================
// Range Operations Tests
//==============================================================================

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

// Test range operations with different data types
int test_c_range_binary_keys() {
    kv_init();
    
    KvClientHandle client = kv_client_create("localhost:9090");
    TEST_ASSERT(client != NULL, "Failed to create client");
    
    // Begin transaction to set up test data
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    TEST_ASSERT(tx_future != NULL, "Failed to begin transaction");
    
    int ready = wait_for_future_c(tx_future);
    TEST_ASSERT(ready == 1, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    TEST_ASSERT(tx != NULL, "Failed to get transaction handle");
    
    // Create binary keys with different patterns
    const char binary_keys[][20] = {
        "bin\x00key\x01",     // 8 bytes
        "bin\x00key\x02",     // 8 bytes
        "bin\x00key\x03",     // 8 bytes
        "bin\x01prefix\x01",  // 11 bytes
        "bin\x01prefix\x02",  // 11 bytes
    };
    const int key_lengths[] = {8, 8, 8, 11, 11};
    const char* values[] = {"val1", "val2", "val3", "val4", "val5"};
    
    for (int i = 0; i < 5; i++) {
        KvFutureHandle set_future = kv_transaction_set(tx,
            (const uint8_t*)binary_keys[i], key_lengths[i],
            (const uint8_t*)values[i], strlen(values[i]),
            NULL);
        TEST_ASSERT(set_future != NULL, "Failed to create set future for binary key");
        
        ready = wait_for_future_c(set_future);
        TEST_ASSERT(ready == 1, "Set future not ready for binary key");
        
        KvResult set_result = kv_future_get_void_result(set_future);
        TEST_ASSERT(set_result.success == 1, "Set operation failed for binary key");
    }
    
    // Commit setup transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    ready = wait_for_future_c(commit_future);
    TEST_ASSERT(ready == 1, "Setup commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    TEST_ASSERT(commit_result.success == 1, "Setup commit operation failed");
    
    // Test range queries on binary keys
    KvFutureHandle read_tx_future = kv_read_transaction_begin(client, -1);
    TEST_ASSERT(read_tx_future != NULL, "Failed to begin read transaction");
    
    ready = wait_for_future_c(read_tx_future);
    TEST_ASSERT(ready == 1, "Read transaction begin future not ready");
    
    KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
    TEST_ASSERT(read_tx != NULL, "Failed to get read transaction handle");
    
    // Test 1: Range with binary prefix "bin\x00key"
    const char prefix1[] = "bin\x00key";
    KvFutureHandle range_future1 = kv_read_transaction_get_range(read_tx,
        (const uint8_t*)prefix1, 7,
        NULL, 0, 10, NULL);
    TEST_ASSERT(range_future1 != NULL, "Failed to create binary range future 1");
    
    ready = wait_for_future_c(range_future1);
    TEST_ASSERT(ready == 1, "Binary range future 1 not ready");
    
    KvPairArray pairs1 = {NULL, 0};
    KvResult range_result1 = kv_future_get_kv_array_result(range_future1, &pairs1);
    TEST_ASSERT(range_result1.success == 1, "Binary range operation 1 failed");
    TEST_ASSERT(pairs1.count == 3, "Expected 3 pairs from binary prefix range");
    
    printf("Binary prefix range results:\n");
    for (size_t i = 0; i < pairs1.count; i++) {
        printf("  Key length: %d, Value: %.*s\n", 
               (int)pairs1.pairs[i].key.length,
               pairs1.pairs[i].value.length, (char*)pairs1.pairs[i].value.data);
    }
    
    kv_pair_array_free(&pairs1);
    
    // Test 2: Range with binary prefix "bin\x01prefix"
    const char prefix2[] = "bin\x01prefix";
    KvFutureHandle range_future2 = kv_read_transaction_get_range(read_tx,
        (const uint8_t*)prefix2, 10,
        NULL, 0, 10, NULL);
    TEST_ASSERT(range_future2 != NULL, "Failed to create binary range future 2");
    
    ready = wait_for_future_c(range_future2);
    TEST_ASSERT(ready == 1, "Binary range future 2 not ready");
    
    KvPairArray pairs2 = {NULL, 0};
    KvResult range_result2 = kv_future_get_kv_array_result(range_future2, &pairs2);
    TEST_ASSERT(range_result2.success == 1, "Binary range operation 2 failed");
    TEST_ASSERT(pairs2.count == 2, "Expected 2 pairs from second binary prefix range");
    
    kv_pair_array_free(&pairs2);
    
    // Cleanup
    kv_read_transaction_destroy(read_tx);
    kv_client_destroy(client);
    kv_shutdown();
    TEST_PASS();
    return 0;
}

//==============================================================================
// C++ Range Operations Tests
//==============================================================================

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

// Test advanced C++ range operations with custom comparisons
void test_cpp_advanced_range_operations() {
    FFITest test("C++ Advanced Range Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Set up hierarchical test data
        {
            KvTransactionWrapper tx(client);
            
            // Create hierarchical keys like a directory structure
            tx.set("dir/file1.txt", "content1");
            tx.set("dir/file2.txt", "content2");
            tx.set("dir/subdir/file3.txt", "content3");
            tx.set("dir/subdir/file4.txt", "content4");
            tx.set("dir2/file5.txt", "content5");
            tx.set("root.txt", "root_content");
            
            // Numeric-like keys for ordering tests
            tx.set("item_001", "first");
            tx.set("item_010", "second");
            tx.set("item_100", "third");
            tx.set("item_999", "fourth");
            
            tx.commit();
        }
        
        // Test hierarchical queries
        {
            KvTransactionWrapper tx(client);
            
            // Test 1: Get all files in "dir/" (including subdirectories)
            auto dir_results = tx.get_range("dir/");
            test.assert_true(dir_results.size() == 4, "Expected 4 items under dir/");
            
            std::cout << "Files under 'dir/':" << std::endl;
            for (const auto& pair : dir_results) {
                std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
            }
            
            // Test 2: Get only direct children of "dir/" (exclude subdirectories)
            // In lexicographic order: "dir/file1.txt", "dir/file2.txt" come before "dir/subdir/"
            // Use "dir/s" as end key to stop before "dir/subdir/"
            std::string dir_end = "dir/s";  
            auto direct_children = tx.get_range("dir/", &dir_end);
            test.assert_true(direct_children.size() == 2, "Expected 2 direct children of dir/");
            
            // Test 3: Get items with numeric ordering
            auto numeric_results = tx.get_range("item_");
            test.assert_true(numeric_results.size() == 4, "Expected 4 numeric items");
            
            std::cout << "Numeric items (lexicographic order):" << std::endl;
            for (const auto& pair : numeric_results) {
                std::cout << "  " << pair.first << " -> " << pair.second << std::endl;
            }
            
            // Verify lexicographic ordering (001, 010, 100, 999)
            test.assert_true(numeric_results[0].first == "item_001", "First should be item_001");
            test.assert_true(numeric_results[1].first == "item_010", "Second should be item_010");
            test.assert_true(numeric_results[2].first == "item_100", "Third should be item_100");
            test.assert_true(numeric_results[3].first == "item_999", "Fourth should be item_999");
            
            // Test 4: Range with specific bounds
            std::string range_end = "item_100";
            auto bounded_numeric = tx.get_range("item_001", &range_end);
            test.assert_true(bounded_numeric.size() == 2, "Expected 2 items in bounded range (exclusive end)");
            test.assert_true(bounded_numeric[0].first == "item_001", "Bounded range first should be item_001");
            test.assert_true(bounded_numeric[1].first == "item_010", "Bounded range second should be item_010");
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}