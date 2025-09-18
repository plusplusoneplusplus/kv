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

    // Helper function to set versionstamped key and get generated key
    std::pair<KvCommitResult, std::vector<uint8_t>> SetVersionstampedKeyAndCommit(
        const std::vector<uint8_t>& key_buffer,
        const std::string& value) {

        KvClientWrapper client;
        KvTransactionWrapper tx(client);

        int result = kv_transaction_set_versionstamped_key(
            tx.get(),
            key_buffer.data(), key_buffer.size(),
            KV_STR(value.c_str()),
            nullptr
        );

        EXPECT_EQ(result, KV_FUNCTION_SUCCESS) << "versionstamped key operation should succeed";

        KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
        EXPECT_NE(commit_future, nullptr) << "commit future should be created";
        EXPECT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";

        KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
        EXPECT_EQ(commit_result.success, KV_FUNCTION_SUCCESS) << "commit should succeed";
        EXPECT_EQ(commit_result.generated_keys.count, 1) << "should have one generated key";

        // Extract generated key data
        std::vector<uint8_t> generated_key;
        if (commit_result.generated_keys.count > 0) {
            const uint8_t* key_data = commit_result.generated_keys.data[0].data;
            size_t key_size = commit_result.generated_keys.data[0].length;
            generated_key.assign(key_data, key_data + key_size);
        }

        return std::make_pair(commit_result, generated_key);
    }

    // Helper function to set versionstamped value and get generated value
    std::pair<KvCommitResult, std::vector<uint8_t>> SetVersionstampedValueAndCommit(
        const std::string& key,
        const std::vector<uint8_t>& value_buffer) {

        KvClientWrapper client;
        KvTransactionWrapper tx(client);

        int result = kv_transaction_set_versionstamped_value(
            tx.get(),
            KV_STR(key.c_str()),
            value_buffer.data(), value_buffer.size(),
            nullptr
        );

        EXPECT_EQ(result, KV_FUNCTION_SUCCESS) << "versionstamped value operation should succeed";

        KvFutureHandle commit_future = kv_transaction_commit_with_results(tx.get());
        EXPECT_NE(commit_future, nullptr) << "commit future should be created";
        EXPECT_TRUE(wait_for_future_cpp(commit_future)) << "commit should complete";

        KvCommitResult commit_result = kv_future_get_commit_result(commit_future);
        EXPECT_EQ(commit_result.success, KV_FUNCTION_SUCCESS) << "commit should succeed";
        EXPECT_EQ(commit_result.generated_values.count, 1) << "should have one generated value";

        // Extract generated value data
        std::vector<uint8_t> generated_value;
        if (commit_result.generated_values.count > 0) {
            const uint8_t* value_data = commit_result.generated_values.data[0].data;
            size_t value_size = commit_result.generated_values.data[0].length;
            generated_value.assign(value_data, value_data + value_size);
        }

        return std::make_pair(commit_result, generated_value);
    }

    // Helper function to verify versionstamp data properties
    void VerifyVersionstampData(const std::vector<uint8_t>& data, size_t expected_size, const std::string& description) {
        EXPECT_EQ(data.size(), expected_size) << description << " should be exactly " << expected_size << " bytes";

        bool has_non_zero_data = false;
        for (uint8_t byte : data) {
            if (byte != 0) {
                has_non_zero_data = true;
                break;
            }
        }
        EXPECT_TRUE(has_non_zero_data) << description << " should contain non-zero data";
    }

    // Helper function to read back using regular get
    std::vector<uint8_t> ReadBackWithGet(const std::vector<uint8_t>& key) {
        KvClientWrapper client;
        KvTransactionWrapper tx(client);

        KvFutureHandle get_future = kv_transaction_get(
            tx.get(),
            key.data(), key.size(),
            nullptr
        );

        EXPECT_NE(get_future, nullptr) << "get future should be created";
        EXPECT_TRUE(wait_for_future_cpp(get_future)) << "get should complete";

        KvBinaryData get_value;
        KvResult get_result = kv_future_get_value_result(get_future, &get_value);
        EXPECT_EQ(get_result.success, KV_FUNCTION_SUCCESS) << "get should succeed";

        return std::vector<uint8_t>(get_value.data, get_value.data + get_value.length);
    }

    // Helper function to read back using regular get (string key overload)
    std::vector<uint8_t> ReadBackWithGet(const std::string& key) {
        KvClientWrapper client;
        KvTransactionWrapper tx(client);

        KvFutureHandle get_future = kv_transaction_get(
            tx.get(),
            KV_STR(key.c_str()),
            nullptr
        );

        EXPECT_NE(get_future, nullptr) << "get future should be created";
        EXPECT_TRUE(wait_for_future_cpp(get_future)) << "get should complete";

        KvBinaryData get_value;
        KvResult get_result = kv_future_get_value_result(get_future, &get_value);
        EXPECT_EQ(get_result.success, KV_FUNCTION_SUCCESS) << "get should succeed";

        return std::vector<uint8_t>(get_value.data, get_value.data + get_value.length);
    }

    // Helper function to read back using snapshot get
    std::vector<uint8_t> ReadBackWithSnapshotGet(const std::vector<uint8_t>& key) {
        KvClientWrapper client;

        KvFutureHandle read_tx_future = kv_read_transaction_begin(client.get(), -1);
        EXPECT_NE(read_tx_future, nullptr) << "read transaction future should be created";
        EXPECT_TRUE(wait_for_future_cpp(read_tx_future)) << "read transaction should begin";

        KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
        EXPECT_NE(read_tx, nullptr) << "read transaction should be created";

        KvFutureHandle snapshot_get_future = kv_read_transaction_get(
            read_tx,
            key.data(), key.size(),
            nullptr
        );

        EXPECT_NE(snapshot_get_future, nullptr) << "snapshot get future should be created";
        EXPECT_TRUE(wait_for_future_cpp(snapshot_get_future)) << "snapshot get should complete";

        KvBinaryData snapshot_get_value;
        KvResult snapshot_get_result = kv_future_get_value_result(snapshot_get_future, &snapshot_get_value);
        EXPECT_EQ(snapshot_get_result.success, KV_FUNCTION_SUCCESS) << "snapshot get should succeed";

        std::vector<uint8_t> result(snapshot_get_value.data, snapshot_get_value.data + snapshot_get_value.length);
        kv_read_transaction_destroy(read_tx);
        return result;
    }

    // Helper function to read back using snapshot get (string key overload)
    std::vector<uint8_t> ReadBackWithSnapshotGet(const std::string& key) {
        KvClientWrapper client;

        KvFutureHandle read_tx_future = kv_read_transaction_begin(client.get(), -1);
        EXPECT_NE(read_tx_future, nullptr) << "read transaction future should be created";
        EXPECT_TRUE(wait_for_future_cpp(read_tx_future)) << "read transaction should begin";

        KvReadTransactionHandle read_tx = kv_future_get_read_transaction(read_tx_future);
        EXPECT_NE(read_tx, nullptr) << "read transaction should be created";

        KvFutureHandle snapshot_get_future = kv_read_transaction_get(
            read_tx,
            KV_STR(key.c_str()),
            nullptr
        );

        EXPECT_NE(snapshot_get_future, nullptr) << "snapshot get future should be created";
        EXPECT_TRUE(wait_for_future_cpp(snapshot_get_future)) << "snapshot get should complete";

        KvBinaryData snapshot_get_value;
        KvResult snapshot_get_result = kv_future_get_value_result(snapshot_get_future, &snapshot_get_value);
        EXPECT_EQ(snapshot_get_result.success, KV_FUNCTION_SUCCESS) << "snapshot get should succeed";

        std::vector<uint8_t> result(snapshot_get_value.data, snapshot_get_value.data + snapshot_get_value.length);
        kv_read_transaction_destroy(read_tx);
        return result;
    }
};

// Test versionstamped key operations
TEST_F(VersionstampedOperationsTest, BasicVersionstampedKey) {
    std::vector<uint8_t> key_buffer = {'u','s','e','r','_','s','c','o','r','e','_',0,0,0,0,0,0,0,0,0,0}; // 11 prefix + 10 placeholder bytes
    std::string value = "100";

    auto [commit_result, generated_key] = SetVersionstampedKeyAndCommit(key_buffer, value);

    EXPECT_EQ(commit_result.generated_values.count, 0) << "should have no generated values for key operation";

    kv_commit_result_free(&commit_result);
}

// Test versionstamped value operations
TEST_F(VersionstampedOperationsTest, BasicVersionstampedValue) {
    std::string key = "user_session";
    std::vector<uint8_t> value_buffer = {'s','e','s','s','i','o','n','_',0,0,0,0,0,0,0,0,0,0}; // 8 prefix + 10 placeholder bytes

    auto [commit_result, generated_value] = SetVersionstampedValueAndCommit(key, value_buffer);

    EXPECT_EQ(commit_result.generated_keys.count, 0) << "should have no generated keys for value operation";

    kv_commit_result_free(&commit_result);
}

// Test that versionstamped key generates exactly 10 bytes of versionstamp data
TEST_F(VersionstampedOperationsTest, VersionstampedKeyGenerates10Bytes) {
    std::vector<uint8_t> key_buffer = {'u','s','e','r','_','s','c','o','r','e','_',0,0,0,0,0,0,0,0,0,0}; // 11 prefix + 10 placeholder bytes
    std::string value = "100";

    auto [commit_result, generated_key] = SetVersionstampedKeyAndCommit(key_buffer, value);

    // Verify the generated key has exactly the expected size (prefix + 10-byte versionstamp)
    EXPECT_EQ(generated_key.size(), 21) << "generated key should be 21 bytes (11 prefix + 10 versionstamp)";

    // Check that prefix matches
    std::vector<uint8_t> expected_prefix = {'u','s','e','r','_','s','c','o','r','e','_'};
    EXPECT_EQ(memcmp(generated_key.data(), expected_prefix.data(), expected_prefix.size()), 0)
        << "generated key should have correct prefix";

    // Verify the versionstamp portion (last 10 bytes) contains non-zero data
    VerifyVersionstampData(std::vector<uint8_t>(generated_key.end() - 10, generated_key.end()), 10, "versionstamp portion");

    kv_commit_result_free(&commit_result);
}

// Test that versionstamped value generates exactly 10 bytes of versionstamp data
TEST_F(VersionstampedOperationsTest, VersionstampedValueGenerates10Bytes) {
    std::string key = "user_session";
    std::vector<uint8_t> value_buffer = {'s','e','s','s','i','o','n','_',0,0,0,0,0,0,0,0,0,0}; // 8 prefix + 10 placeholder bytes

    auto [commit_result, generated_value] = SetVersionstampedValueAndCommit(key, value_buffer);

    // Verify the generated value has exactly the expected size (prefix + 10-byte versionstamp)
    EXPECT_EQ(generated_value.size(), 18) << "generated value should be 18 bytes (8 prefix + 10 versionstamp)";

    // Check that prefix matches
    std::vector<uint8_t> expected_prefix = {'s','e','s','s','i','o','n','_'};
    EXPECT_EQ(memcmp(generated_value.data(), expected_prefix.data(), expected_prefix.size()), 0)
        << "generated value should have correct prefix";

    // Verify the versionstamp portion (last 10 bytes) contains non-zero data
    VerifyVersionstampData(std::vector<uint8_t>(generated_value.end() - 10, generated_value.end()), 10, "versionstamp portion");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped key with exactly 10 bytes (pure versionstamp, no prefix)
TEST_F(VersionstampedOperationsTest, VersionstampedKeyExactly10Bytes) {
    std::vector<uint8_t> key_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes
    std::string value = "test_value";

    auto [commit_result, generated_key] = SetVersionstampedKeyAndCommit(key_buffer, value);

    VerifyVersionstampData(generated_key, 10, "generated key");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped value with exactly 10 bytes (pure versionstamp, no prefix)
TEST_F(VersionstampedOperationsTest, VersionstampedValueExactly10Bytes) {
    std::string key = "test_key";
    std::vector<uint8_t> value_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes

    auto [commit_result, generated_value] = SetVersionstampedValueAndCommit(key, value_buffer);

    VerifyVersionstampData(generated_value, 10, "generated value");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped key read-back with get() - validate 10-byte versionstamp in actual stored data
TEST_F(VersionstampedOperationsTest, VersionstampedKeyReadBackWithGet) {
    std::vector<uint8_t> key_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes
    std::string value = "test_value_for_readback";

    auto [commit_result, generated_key] = SetVersionstampedKeyAndCommit(key_buffer, value);

    // Read back using the generated key
    std::vector<uint8_t> read_value = ReadBackWithGet(generated_key);

    // Verify the read value matches what we stored
    EXPECT_EQ(read_value.size(), value.length()) << "value length should match";
    EXPECT_EQ(memcmp(read_value.data(), value.c_str(), value.length()), 0) << "value content should match";

    // Verify the key is exactly 10 bytes with versionstamp data
    VerifyVersionstampData(generated_key, 10, "10-byte versionstamp key");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped key read-back with snapshot get() - validate 10-byte versionstamp in actual stored data
TEST_F(VersionstampedOperationsTest, VersionstampedKeyReadBackWithSnapshotGet) {
    std::vector<uint8_t> key_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes
    std::string value = "test_value_for_snapshot_readback";

    auto [commit_result, generated_key] = SetVersionstampedKeyAndCommit(key_buffer, value);

    // Read back using snapshot get with the generated key
    std::vector<uint8_t> read_value = ReadBackWithSnapshotGet(generated_key);

    // Verify the read value matches what we stored
    EXPECT_EQ(read_value.size(), value.length()) << "value length should match";
    EXPECT_EQ(memcmp(read_value.data(), value.c_str(), value.length()), 0) << "value content should match";

    // Verify the key is exactly 10 bytes with versionstamp data
    VerifyVersionstampData(generated_key, 10, "10-byte versionstamp key");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped value read-back with get() - validate 10-byte versionstamp in actual stored data
TEST_F(VersionstampedOperationsTest, VersionstampedValueReadBackWithGet) {
    std::string key = "test_key_for_value_readback";
    std::vector<uint8_t> value_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes

    auto [commit_result, generated_value] = SetVersionstampedValueAndCommit(key, value_buffer);

    // Read back using the key
    std::vector<uint8_t> read_value = ReadBackWithGet(key);

    // Verify the read value matches the generated versionstamped value and is exactly 10 bytes
    EXPECT_EQ(read_value.size(), 10) << "read value should be exactly 10 bytes";
    EXPECT_EQ(read_value.size(), generated_value.size()) << "read value length should match generated value length";
    EXPECT_EQ(memcmp(read_value.data(), generated_value.data(), generated_value.size()), 0) << "read value should match generated versionstamped value";

    // Verify the versionstamp contains non-zero data
    VerifyVersionstampData(read_value, 10, "10-byte versionstamp value");

    kv_commit_result_free(&commit_result);
}

// Test versionstamped value read-back with snapshot get() - validate 10-byte versionstamp in actual stored data
TEST_F(VersionstampedOperationsTest, VersionstampedValueReadBackWithSnapshotGet) {
    std::string key = "test_key_for_value_snapshot_readback";
    std::vector<uint8_t> value_buffer = {0,0,0,0,0,0,0,0,0,0}; // Exactly 10 placeholder bytes

    auto [commit_result, generated_value] = SetVersionstampedValueAndCommit(key, value_buffer);

    // Read back using snapshot get with the key
    std::vector<uint8_t> read_value = ReadBackWithSnapshotGet(key);

    // Verify the read value matches the generated versionstamped value and is exactly 10 bytes
    EXPECT_EQ(read_value.size(), 10) << "read value should be exactly 10 bytes";
    EXPECT_EQ(read_value.size(), generated_value.size()) << "read value length should match generated value length";
    EXPECT_EQ(memcmp(read_value.data(), generated_value.data(), generated_value.size()), 0) << "read value should match generated versionstamped value";

    // Verify the versionstamp contains non-zero data
    VerifyVersionstampData(read_value, 10, "10-byte versionstamp value");

    kv_commit_result_free(&commit_result);
}