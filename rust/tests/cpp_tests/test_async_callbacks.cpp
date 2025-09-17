#include <gtest/gtest.h>
#include "test_common.hpp"
#include <atomic>
#include <thread>
#include <vector>

// Global callback test context
CallbackTestContext g_callback_context;
CallbackCounter g_callback_counter;

// Simple callback function implementation
extern "C" void test_callback_simple(KvFutureHandle future, void* user_context) {
    g_callback_context.notify_callback(future, user_context);
}

// Callback with custom context implementation
extern "C" void test_callback_with_context(KvFutureHandle future, void* user_context) {
    auto* context = static_cast<CallbackTestContext*>(user_context);
    if (context) {
        context->notify_callback(future, user_context);
    }
}

// Counting callback implementation
extern "C" void test_callback_counting(KvFutureHandle future, void* user_context) {
    g_callback_counter.increment();
}

class AsyncCallbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Reset test contexts
        g_callback_context.reset();
        g_callback_counter.reset();
    }

    void TearDown() override {
        kv_shutdown();
    }
};

TEST_F(AsyncCallbackTest, BasicCallbackExecution) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Perform a set operation to get a future
    const std::string key = "callback_test_key";
    const std::string value = "callback_test_value";

    KvFutureHandle future = kv_transaction_set(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               (const uint8_t*)value.data(), value.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create set future";

    // Set callback with test context
    void* test_context = (void*)0x12345678;
    int callback_result = kv_future_set_callback(future, test_callback_simple, test_context);
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback";

    // Wait for the callback to be invoked
    bool callback_received = g_callback_context.wait_for_callback();
    ASSERT_TRUE(callback_received) << "Callback was not invoked within timeout";

    // Verify callback was called with correct parameters
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, test_context);

    // Verify the operation actually completed successfully
    KvResult result = kv_future_get_void_result(future);
    EXPECT_EQ(result.success, 1) << "Set operation failed";
    if (result.error_message) {
        kv_result_free(&result);
    }
}

TEST_F(AsyncCallbackTest, CallbackWithCustomContext) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Create a custom callback context
    CallbackTestContext custom_context;

    // Perform a get operation
    const std::string key = "nonexistent_key";

    KvFutureHandle future = kv_transaction_get(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create get future";

    // Set callback with custom context
    int callback_result = kv_future_set_callback(future, test_callback_with_context, &custom_context);
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback";

    // Wait for the callback to be invoked
    bool callback_received = custom_context.wait_for_callback();
    ASSERT_TRUE(callback_received) << "Callback was not invoked within timeout";

    // Verify callback was called with correct parameters
    EXPECT_NE(custom_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(custom_context.user_context, &custom_context);
}

TEST_F(AsyncCallbackTest, ImmediateCallbackOnCompletedFuture) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Perform a simple operation and wait for it to complete
    const std::string key = "immediate_test_key";
    const std::string value = "immediate_test_value";

    KvFutureHandle future = kv_transaction_set(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               (const uint8_t*)value.data(), value.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create set future";

    // Wait for the operation to complete using polling
    bool completed = wait_for_future_cpp(future);
    ASSERT_TRUE(completed) << "Future did not complete within timeout";

    // Now set the callback on the already-completed future
    void* test_context = (void*)0xABCDEF00;
    int callback_result = kv_future_set_callback(future, test_callback_simple, test_context);
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback on completed future";

    // The callback should be invoked immediately
    bool callback_received = g_callback_context.wait_for_callback(1000); // Short timeout
    ASSERT_TRUE(callback_received) << "Immediate callback was not invoked";

    // Verify callback parameters
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, test_context);
}

TEST_F(AsyncCallbackTest, DeferredCallbackOnPendingFuture) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Perform an operation but don't wait for completion
    const std::string key = "deferred_test_key";
    const std::string value = "deferred_test_value";

    KvFutureHandle future = kv_transaction_set(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               (const uint8_t*)value.data(), value.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create set future";

    // Set callback immediately (before completion)
    void* test_context = (void*)0xDEADBEEF;
    int callback_result = kv_future_set_callback(future, test_callback_simple, test_context);
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback";

    // Callback should not be invoked immediately
    EXPECT_FALSE(g_callback_context.callback_called) << "Callback was invoked too early";

    // Wait for the callback to be invoked when the operation completes
    bool callback_received = g_callback_context.wait_for_callback();
    ASSERT_TRUE(callback_received) << "Deferred callback was not invoked";

    // Verify callback parameters
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, test_context);
}

TEST_F(AsyncCallbackTest, CallbackOnFailedOperation) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Try to get from a very long key that might cause failure
    std::string very_long_key(10000, 'x'); // 10KB key which might be rejected

    KvFutureHandle future = kv_transaction_get(tx.get(),
                                               (const uint8_t*)very_long_key.data(), very_long_key.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create get future";

    // Set callback on this future
    void* test_context = (void*)0xDEAD123;
    int callback_result = kv_future_set_callback(future, test_callback_simple, test_context);
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback";

    // Wait for the callback to be invoked
    bool callback_received = g_callback_context.wait_for_callback();
    ASSERT_TRUE(callback_received) << "Callback was not invoked for operation";

    // Verify callback was called
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, test_context);

    // Check if the operation succeeded or failed - we just want to verify callback works
    KvBinaryData value;
    KvResult result = kv_future_get_value_result(future, &value);
    // We don't assert on success/failure here since the operation might actually succeed
    // The important thing is that the callback was invoked
    if (result.error_message) {
        kv_result_free(&result);
    }
    if (value.data) {
        kv_binary_free(&value);
    }
}

TEST_F(AsyncCallbackTest, ConcurrentCallbacks) {
    KvClientWrapper client;

    const int num_operations = 10;
    std::vector<KvFutureHandle> futures;
    futures.reserve(num_operations);

    // Create multiple concurrent operations
    for (int i = 0; i < num_operations; i++) {
        KvTransactionWrapper tx(client);
        std::string key = "concurrent_key_" + std::to_string(i);
        std::string value = "concurrent_value_" + std::to_string(i);

        KvFutureHandle future = kv_transaction_set(tx.get(),
                                                   (const uint8_t*)key.data(), key.size(),
                                                   (const uint8_t*)value.data(), value.size(),
                                                   nullptr);
        ASSERT_NE(future, nullptr) << "Failed to create future " << i;

        // Set callback on each future
        int callback_result = kv_future_set_callback(future, test_callback_counting, nullptr);
        ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback " << i;

        futures.push_back(future);
    }

    // Wait for all callbacks to complete
    auto start_time = std::chrono::steady_clock::now();
    while (g_callback_counter.get_count() < num_operations) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time).count();
        if (elapsed > 10000) { // 10 second timeout
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    // Verify all callbacks were invoked
    EXPECT_EQ(g_callback_counter.get_count(), num_operations)
        << "Not all concurrent callbacks were invoked";
    EXPECT_FALSE(g_callback_counter.has_error()) << "Error occurred in concurrent callbacks";
}

TEST_F(AsyncCallbackTest, CallbackReplacement) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Create an operation
    const std::string key = "replacement_test_key";
    const std::string value = "replacement_test_value";

    KvFutureHandle future = kv_transaction_set(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               (const uint8_t*)value.data(), value.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create set future";

    // Set first callback
    void* context1 = (void*)0x1111;
    int result1 = kv_future_set_callback(future, test_callback_simple, context1);
    ASSERT_EQ(result1, KV_FUNCTION_SUCCESS) << "Failed to set first callback";

    // Set second callback (should replace the first)
    void* context2 = (void*)0x2222;
    int result2 = kv_future_set_callback(future, test_callback_simple, context2);
    ASSERT_EQ(result2, KV_FUNCTION_SUCCESS) << "Failed to set second callback";

    // Wait for callback
    bool callback_received = g_callback_context.wait_for_callback();
    ASSERT_TRUE(callback_received) << "Callback was not invoked";

    // Verify the second callback was used (with context2)
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, context2)
        << "Second callback context should be used, not first";
}

TEST_F(AsyncCallbackTest, CallbackAfterPolling) {
    KvClientWrapper client;
    KvTransactionWrapper tx(client);

    // Create an operation
    const std::string key = "polling_test_key";
    const std::string value = "polling_test_value";

    KvFutureHandle future = kv_transaction_set(tx.get(),
                                               (const uint8_t*)key.data(), key.size(),
                                               (const uint8_t*)value.data(), value.size(),
                                               nullptr);
    ASSERT_NE(future, nullptr) << "Failed to create set future";

    // Poll the future first
    bool completed = wait_for_future_cpp(future);
    ASSERT_TRUE(completed) << "Future should have completed";

    // Get the result
    KvResult result = kv_future_get_void_result(future);
    EXPECT_EQ(result.success, 1) << "Operation should have succeeded";
    if (result.error_message) {
        kv_result_free(&result);
    }

    // Now set a callback after polling and getting the result
    void* test_context = (void*)0x9999;
    int callback_result = kv_future_set_callback(future, test_callback_simple, test_context);
    // Note: Setting callback after the result has been consumed may not be supported
    if (callback_result == KV_FUNCTION_ERROR) {
        GTEST_SKIP() << "Setting callback after consuming result is not supported";
    }
    ASSERT_EQ(callback_result, KV_FUNCTION_SUCCESS) << "Failed to set callback after polling";

    // Callback should still be invoked immediately
    bool callback_received = g_callback_context.wait_for_callback(1000);
    ASSERT_TRUE(callback_received) << "Callback should be invoked even after polling";

    // Verify callback parameters
    EXPECT_NE(g_callback_context.future_handle, nullptr) << "Callback received null future handle";
    EXPECT_EQ(g_callback_context.user_context, test_context);
}