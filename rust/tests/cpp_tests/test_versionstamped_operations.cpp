/**
 * Google Test-based C++ FFI Tests for Versionstamped Operations
 * Tests versionstamped key and value functionality through the FFI interface
 */

#include <gtest/gtest.h>
#include "test_common.hpp"
#include <vector>
#include <set>

class VersionstampedOperationsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Common setup for each test
    }
    
    void TearDown() override {
        // Common cleanup for each test
    }
};

// Test versionstamped key operations
TEST_F(VersionstampedOperationsTest, BasicVersionstampedKey) {
    // Create client and transaction
    KvClientWrapper client;
    KvTransactionWrapper tx(client);
    
    // Set a versionstamped key - key buffer with placeholder for 10-byte versionstamp
    std::vector<uint8_t> key_buffer = {'u','s','e','r','_','s','c','o','r','e','_',0,0,0,0,0,0,0,0,0,0}; // 11 prefix + 10 placeholder bytes
    std::string value = "100";
    
    int result = kv_transaction_set_versionstamped_key(
        tx.get(),
        key_buffer.data(), key_buffer.size(),
        KV_STR(value.c_str()),
        nullptr  // no column family
    );
    
    EXPECT_EQ(result, KV_FUNCTION_SUCCESS) << "versionstamped key operation should succeed";
    
    // Commit with results to get generated keys
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    ASSERT_NE(commit_future, nullptr) << "commit future should be created";
    ASSERT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    EXPECT_EQ(commit_result.success, KV_FUNCTION_SUCCESS) << "commit should succeed";
    EXPECT_EQ(commit_result.generated_keys.count, 1) << "should have one generated key";
    EXPECT_EQ(commit_result.generated_values.count, 0) << "should have no generated values for key operation";
    
    // Clean up
    kv_commit_result_free(&commit_result);
}

// Test versionstamped value operations
TEST_F(VersionstampedOperationsTest, BasicVersionstampedValue) {
    // Create client and transaction
    KvClientWrapper client;
    KvTransactionWrapper tx(client);
    
    // Set a versionstamped value - value buffer with placeholder for 10-byte versionstamp
    std::string key = "user_session";
    std::vector<uint8_t> value_buffer = {'s','e','s','s','i','o','n','_',0,0,0,0,0,0,0,0,0,0}; // 8 prefix + 10 placeholder bytes
    
    int result = kv_transaction_set_versionstamped_value(
        tx.get(),
        KV_STR(key.c_str()),
        value_buffer.data(), value_buffer.size(),
        nullptr  // no column family
    );
    
    EXPECT_EQ(result, KV_FUNCTION_SUCCESS) << "versionstamped value operation should succeed";
    
    // Commit with results to get generated values
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    ASSERT_NE(commit_future, nullptr) << "commit future should be created";
    ASSERT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    EXPECT_EQ(commit_result.success, KV_FUNCTION_SUCCESS) << "commit should succeed";
    EXPECT_EQ(commit_result.generated_keys.count, 0) << "should have no generated keys for value operation";
    EXPECT_EQ(commit_result.generated_values.count, 1) << "should have one generated value";
    
    // Clean up
    kv_commit_result_free(&commit_result);
}