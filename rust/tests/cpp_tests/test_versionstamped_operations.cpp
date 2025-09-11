/**
 * C++ FFI Tests for Versionstamped Operations
 * Tests versionstamped key and value functionality through the FFI interface
 */

#include "test_common.hpp"
#include <vector>
#include <set>

// Test versionstamped key operations
void test_versionstamped_key_basic() {
    FFITest test("test_versionstamped_key_basic");
    
    // Create client and transaction
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    // Set a versionstamped key - key prefix will be appended with version
    std::string key_prefix = "user_score_";
    std::string value = "100";
    
    int result = kv_transaction_set_versionstamped_key(
        tx.get(),
        KV_STR(key_prefix.c_str()),
        KV_STR(value.c_str()),
        nullptr  // no column family
    );
    
    test.assert_true(result == 1, "versionstamped key operation should succeed");
    
    // Commit with results to get generated keys
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    test.assert_true(commit_future != nullptr, "commit future should be created");
    test.assert_true(wait_for_future_cpp(commit_future), "commit should complete");
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    test.assert_true(commit_result.success == 1, "commit should succeed");
    test.assert_true(commit_result.generated_keys.count == 1, "should have one generated key");
    test.assert_true(commit_result.generated_values.count == 0, "should have no generated values for key operation");
    
    // Verify generated key structure
    KvBinaryData& generated_key = commit_result.generated_keys.data[0];
    test.assert_true(generated_key.length > key_prefix.length(), "generated key should be longer than prefix");
    test.assert_true(memcmp(generated_key.data, key_prefix.data(), key_prefix.length()) == 0,
                    "generated key should start with prefix");
    test.assert_true(generated_key.length == key_prefix.length() + 10, 
                    "generated key should be prefix + 10 bytes");
    
    // Verify the key was actually stored by reading it back
    KvTransactionWrapper read_tx(client);
    KvFutureHandle get_future = kv_transaction_get(read_tx.get(),
                                                   generated_key.data, generated_key.length,
                                                   nullptr);
    test.assert_true(get_future != nullptr, "get future should be created");
    test.assert_true(wait_for_future_cpp(get_future), "get should complete");
    
    KvBinaryData retrieved_value;
    KvResult get_result = kv_future_get_value_result(get_future, &retrieved_value);
    test.assert_true(get_result.success == 1, "get should succeed");
    test.assert_true(retrieved_value.length == value.length(), "value length should match");
    test.assert_true(memcmp(retrieved_value.data, value.data(), value.length()) == 0,
                    "retrieved value should match original");
    
    // Clean up
    kv_binary_free(&retrieved_value);
    kv_commit_result_free(&commit_result);
    
    test.pass();
}

// Test versionstamped value operations
void test_versionstamped_value_basic() {
    FFITest test("test_versionstamped_value_basic");
    
    // Create client and transaction
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    // Set a versionstamped value - value prefix will be appended with version
    std::string key = "user_session";
    std::string value_prefix = "session_";
    
    int result = kv_transaction_set_versionstamped_value(
        tx.get(),
        KV_STR(key.c_str()),
        KV_STR(value_prefix.c_str()),
        nullptr  // no column family
    );
    
    test.assert_true(result == 1, "versionstamped value operation should succeed");
    
    // Commit with results to get generated values
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    test.assert_true(commit_future != nullptr, "commit future should be created");
    test.assert_true(wait_for_future_cpp(commit_future), "commit should complete");
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    test.assert_true(commit_result.success == 1, "commit should succeed");
    test.assert_true(commit_result.generated_keys.count == 0, "should have no generated keys for value operation");
    test.assert_true(commit_result.generated_values.count == 1, "should have one generated value");
    
    // Verify generated value structure
    KvBinaryData& generated_value = commit_result.generated_values.data[0];
    test.assert_true(generated_value.length > value_prefix.length(), "generated value should be longer than prefix");
    test.assert_true(memcmp(generated_value.data, value_prefix.data(), value_prefix.length()) == 0,
                    "generated value should start with prefix");
    test.assert_true(generated_value.length == value_prefix.length() + 10, 
                    "generated value should be prefix + 10 bytes");
    
    // Verify the value was actually stored by reading it back
    KvTransactionWrapper read_tx(client);
    KvFutureHandle get_future = kv_transaction_get(read_tx.get(),
                                                   KV_STR(key.c_str()),
                                                   nullptr);
    test.assert_true(get_future != nullptr, "get future should be created");
    test.assert_true(wait_for_future_cpp(get_future), "get should complete");
    
    KvBinaryData retrieved_value;
    KvResult get_result = kv_future_get_value_result(get_future, &retrieved_value);
    test.assert_true(get_result.success == 1, "get should succeed");
    test.assert_true(retrieved_value.length == generated_value.length, "retrieved value length should match generated");
    test.assert_true(memcmp(retrieved_value.data, generated_value.data, generated_value.length) == 0,
                    "retrieved value should match generated value");
    
    // Clean up
    kv_binary_free(&retrieved_value);
    kv_commit_result_free(&commit_result);
    
    test.pass();
}

// Test mixed versionstamped operations in a single transaction
void test_mixed_versionstamped_operations() {
    FFITest test("test_mixed_versionstamped_operations");
    
    // Create client and transaction
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    // Set both versionstamped key and value, plus regular operation
    std::string key_prefix = "log_entry_";
    std::string key_value = "event_data";
    std::string value_key = "event_value";
    std::string value_prefix = "data_";
    std::string regular_key = "regular_key";
    std::string regular_value = "regular_value";
    
    // Versionstamped key
    int result1 = kv_transaction_set_versionstamped_key(
        tx.get(),
        KV_STR(key_prefix.c_str()),
        KV_STR(key_value.c_str()),
        nullptr
    );
    test.assert_true(result1 == 1, "versionstamped key operation should succeed");
    
    // Versionstamped value
    int result2 = kv_transaction_set_versionstamped_value(
        tx.get(),
        KV_STR(value_key.c_str()),
        KV_STR(value_prefix.c_str()),
        nullptr
    );
    test.assert_true(result2 == 1, "versionstamped value operation should succeed");
    
    // Regular set operation
    tx.set(regular_key, regular_value);
    
    // Commit with results
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    test.assert_true(commit_future != nullptr, "commit future should be created");
    test.assert_true(wait_for_future_cpp(commit_future), "commit should complete");
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    test.assert_true(commit_result.success == 1, "commit should succeed");
    test.assert_true(commit_result.generated_keys.count == 1, "should have one generated key");
    test.assert_true(commit_result.generated_values.count == 1, "should have one generated value");
    
    KvBinaryData& generated_key = commit_result.generated_keys.data[0];
    KvBinaryData& generated_value = commit_result.generated_values.data[0];
    
    // Verify formats
    test.assert_true(memcmp(generated_key.data, key_prefix.data(), key_prefix.length()) == 0,
                    "generated key should start with prefix");
    test.assert_true(memcmp(generated_value.data, value_prefix.data(), value_prefix.length()) == 0,
                    "generated value should start with prefix");
    
    // Verify all data was stored correctly
    KvTransactionWrapper read_tx(client);
    
    // Check versionstamped key
    KvFutureHandle get_key_future = kv_transaction_get(read_tx.get(),
                                                       generated_key.data, generated_key.length,
                                                       nullptr);
    test.assert_true(wait_for_future_cpp(get_key_future), "get generated key should complete");
    
    KvBinaryData key_value_result;
    KvResult get_key_result = kv_future_get_value_result(get_key_future, &key_value_result);
    test.assert_true(get_key_result.success == 1, "get generated key should succeed");
    test.assert_true(key_value_result.length == key_value.length(), "key value should match");
    test.assert_true(memcmp(key_value_result.data, key_value.data(), key_value.length()) == 0,
                    "key value should match");
    
    // Check versionstamped value
    KvFutureHandle get_value_future = kv_transaction_get(read_tx.get(),
                                                         KV_STR(value_key.c_str()),
                                                         nullptr);
    test.assert_true(wait_for_future_cpp(get_value_future), "get value key should complete");
    
    KvBinaryData stored_value_result;
    KvResult get_value_result = kv_future_get_value_result(get_value_future, &stored_value_result);
    test.assert_true(get_value_result.success == 1, "get value key should succeed");
    test.assert_true(stored_value_result.length == generated_value.length, "stored value should match generated");
    test.assert_true(memcmp(stored_value_result.data, generated_value.data, generated_value.length) == 0,
                    "stored value should match generated");
    
    // Check regular key
    std::string retrieved_regular = read_tx.get(regular_key);
    test.assert_true(retrieved_regular == regular_value, "regular key should have correct value");
    
    // Clean up
    kv_binary_free(&key_value_result);
    kv_binary_free(&stored_value_result);
    kv_commit_result_free(&commit_result);
    
    test.pass();
}

// Test versionstamp uniqueness across transactions
void test_versionstamp_uniqueness() {
    FFITest test("test_versionstamp_uniqueness");
    
    KvClientWrapper client("localhost:9090");
    std::string key_prefix = "unique_test_";
    
    // First transaction
    KvTransactionWrapper tx1(client);
    int result1 = kv_transaction_set_versionstamped_key(tx1.get(),
                                                        KV_STR(key_prefix.c_str()),
                                                        KV_STR("value1"),
                                                        nullptr);
    test.assert_true(result1 == 1, "first versionstamped key should succeed");
    
    KvFutureHandle commit1_future = kv_transaction_commit_with_results(tx1.get());
    test.assert_true(wait_for_future_cpp(commit1_future), "first commit should complete");
    
    KvCommitResult result1_commit = kv_future_get_commit_result(commit1_future);
    test.assert_true(result1_commit.success == 1, "first commit should succeed");
    test.assert_true(result1_commit.generated_keys.count == 1, "first commit should have one generated key");
    
    // Second transaction
    KvTransactionWrapper tx2(client);
    int result2 = kv_transaction_set_versionstamped_key(tx2.get(),
                                                        KV_STR(key_prefix.c_str()),
                                                        KV_STR("value2"),
                                                        nullptr);
    test.assert_true(result2 == 1, "second versionstamped key should succeed");
    
    KvFutureHandle commit2_future = kv_transaction_commit_with_results(tx2.get());
    test.assert_true(wait_for_future_cpp(commit2_future), "second commit should complete");
    
    KvCommitResult result2_commit = kv_future_get_commit_result(commit2_future);
    test.assert_true(result2_commit.success == 1, "second commit should succeed");
    test.assert_true(result2_commit.generated_keys.count == 1, "second commit should have one generated key");
    
    // Keys should be different (different versions)
    KvBinaryData& key1 = result1_commit.generated_keys.data[0];
    KvBinaryData& key2 = result2_commit.generated_keys.data[0];
    
    test.assert_true(key1.length == key2.length, "keys should have same length");
    test.assert_true(memcmp(key1.data, key2.data, key1.length) != 0, 
                    "keys from different transactions should be unique");
    
    // Both should have the same prefix
    test.assert_true(memcmp(key1.data, key_prefix.data(), key_prefix.length()) == 0,
                    "first key should have correct prefix");
    test.assert_true(memcmp(key2.data, key_prefix.data(), key_prefix.length()) == 0,
                    "second key should have correct prefix");
    
    // Clean up
    kv_commit_result_free(&result1_commit);
    kv_commit_result_free(&result2_commit);
    
    test.pass();
}

// Test backward compatibility with regular commit
void test_backward_compatibility_commit() {
    FFITest test("test_backward_compatibility_commit");
    
    // Create client and transaction
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    // Set a versionstamped key
    std::string key_prefix = "compat_test_";
    std::string value = "data";
    
    int result = kv_transaction_set_versionstamped_key(
        tx.get(),
        KV_STR(key_prefix.c_str()),
        KV_STR(value.c_str()),
        nullptr
    );
    
    test.assert_true(result == 1, "versionstamped key operation should succeed");
    
    // Use the old commit method (should still work)
    KvFutureHandle commit_future = kv_transaction_commit(tx.get());
    test.assert_true(commit_future != nullptr, "commit future should be created");
    test.assert_true(wait_for_future_cpp(commit_future), "commit should complete");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    test.assert_true(commit_result.success == 1, "commit should succeed");
    
    // We can't verify the generated key with the old commit method,
    // but it should not fail
    test.pass();
}

// Test error handling for invalid parameters
void test_versionstamped_error_handling() {
    FFITest test("test_versionstamped_error_handling");
    
    KvClientWrapper client("localhost:9090");
    KvTransactionWrapper tx(client);
    
    // Test null transaction handle
    int result1 = kv_transaction_set_versionstamped_key(
        nullptr,  // invalid transaction
        KV_STR("prefix_"),
        KV_STR("value"),
        nullptr
    );
    test.assert_true(result1 == 0, "null transaction should fail");
    
    // Test null key prefix
    int result2 = kv_transaction_set_versionstamped_key(
        tx.get(),
        nullptr, 0,  // invalid key prefix
        KV_STR("value"),
        nullptr
    );
    test.assert_true(result2 == 0, "null key prefix should fail");
    
    // Test negative lengths
    int result3 = kv_transaction_set_versionstamped_key(
        tx.get(),
        KV_STR("prefix_"),
        (const uint8_t*)"value", -1,  // invalid length
        nullptr
    );
    test.assert_true(result3 == 0, "negative value length should fail");
    
    // Test empty prefix is allowed
    int result4 = kv_transaction_set_versionstamped_key(
        tx.get(),
        (const uint8_t*)"", 0,  // empty prefix should be allowed
        KV_STR("value"),
        nullptr
    );
    test.assert_true(result4 == 1, "empty prefix should be allowed");
    
    test.pass();
}

// Main function declarations (to be called from main.cpp)

void run_versionstamped_tests() {
    std::cout << "\n=== Running Versionstamped Operations Tests ===" << std::endl;
    
    try {
        test_versionstamped_key_basic();
        test_versionstamped_value_basic();
        test_mixed_versionstamped_operations();
        test_versionstamp_uniqueness();
        test_backward_compatibility_commit();
        test_versionstamped_error_handling();
        
        std::cout << "✅ All versionstamped operations tests passed!" << std::endl;
    } catch (const std::exception& e) {
        std::cout << "❌ Versionstamped operations test failed: " << e.what() << std::endl;
        throw;
    }
}