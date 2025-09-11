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
    
    EXPECT_EQ(result, 1) << "versionstamped key operation should succeed";
    
    // Commit with results to get generated keys
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    ASSERT_NE(commit_future, nullptr) << "commit future should be created";
    ASSERT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    EXPECT_EQ(commit_result.success, 1) << "commit should succeed";
    EXPECT_EQ(commit_result.generated_keys.count, 1) << "should have one generated key";
    EXPECT_EQ(commit_result.generated_values.count, 0) << "should have no generated values for key operation";
    
    // Clean up
    kv_commit_result_free(&commit_result);
}

// Test versionstamped value operations
TEST_F(VersionstampedOperationsTest, BasicVersionstampedValue) {
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
    
    EXPECT_EQ(result, 1) << "versionstamped value operation should succeed";
    
    // Commit with results to get generated values
    KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
    ASSERT_NE(commit_future, nullptr) << "commit future should be created";
    ASSERT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";
    
    // Get commit result
    KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
    EXPECT_EQ(commit_result.success, 1) << "commit should succeed";
    EXPECT_EQ(commit_result.generated_keys.count, 0) << "should have no generated keys for value operation";
    EXPECT_EQ(commit_result.generated_values.count, 1) << "should have one generated value";
    
    // Clean up
    kv_commit_result_free(&commit_result);
}