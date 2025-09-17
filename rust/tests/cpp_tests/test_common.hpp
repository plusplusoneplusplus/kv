#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstring>
#include <functional>
#include <unistd.h>
#include <cstdlib>
#include "../../src/client/kvstore_client.h"

// Helper function to get server address from environment
inline std::string get_server_address() {
    const char* env_addr = std::getenv("KV_TEST_SERVER_ADDRESS");
    if (env_addr) {
        return std::string(env_addr);
    }

    const char* env_port = std::getenv("KV_TEST_SERVER_PORT");
    if (env_port) {
        return std::string("localhost:") + env_port;
    }

    // Default fallback
    return "localhost:9090";
}

// Test framework macros and classes
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            fprintf(stderr, "FAIL: %s - %s\n", __func__, message); \
            throw std::runtime_error("Test assertion failed"); \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf("PASS: %s\n", __func__); \
    } while(0)

class FFITest {
private:
    std::string test_name;
    
public:
    FFITest(const std::string& name) : test_name(name) {}
    
    void assert_true(bool condition, const std::string& message) {
        if (!condition) {
            std::cerr << "FAIL: " << test_name << " - " << message << std::endl;
            throw std::runtime_error("Test assertion failed");
        }
    }
    
    void pass() {
        std::cout << "PASS: " << test_name << std::endl;
    }
};


// RAII wrapper classes
class KvConfigWrapper {
private:
    KvConfigHandle handle;
    
public:
    explicit KvConfigWrapper(bool with_debug = false) {
        handle = with_debug ? kv_config_create_with_debug() : kv_config_create();
        if (!handle) {
            throw std::runtime_error("Failed to create KV config");
        }
    }
    
    ~KvConfigWrapper() {
        if (handle) {
            kv_config_destroy(handle);
        }
    }
    
    KvConfigHandle get() const { return handle; }
    
    KvConfigWrapper& set_connection_timeout(uint64_t timeout_seconds) {
        int result = kv_config_set_connection_timeout(handle, timeout_seconds);
        if (result != 0) {
            throw std::runtime_error("Failed to set connection timeout");
        }
        return *this;
    }
    
    KvConfigWrapper& set_request_timeout(uint64_t timeout_seconds) {
        int result = kv_config_set_request_timeout(handle, timeout_seconds);
        if (result != 0) {
            throw std::runtime_error("Failed to set request timeout");
        }
        return *this;
    }
    
    KvConfigWrapper& set_max_retries(uint32_t retries) {
        int result = kv_config_set_max_retries(handle, retries);
        if (result != 0) {
            throw std::runtime_error("Failed to set max retries");
        }
        return *this;
    }
    
    KvConfigWrapper& enable_debug() {
        int result = kv_config_enable_debug(handle);
        if (result != 0) {
            throw std::runtime_error("Failed to enable debug");
        }
        return *this;
    }
    
    KvConfigWrapper(const KvConfigWrapper&) = delete;
    KvConfigWrapper& operator=(const KvConfigWrapper&) = delete;
};

class KvClientWrapper {
private:
    KvClientHandle handle;

public:
    // Default constructor uses environment-configured server address
    KvClientWrapper() : KvClientWrapper(get_server_address()) {
    }

    explicit KvClientWrapper(const std::string& address) {
        handle = kv_client_create(address.c_str());
        if (!handle) {
            throw std::runtime_error("Failed to create KV client");
        }
    }

    explicit KvClientWrapper(const std::string& address, KvConfigWrapper& config) {
        handle = kv_client_create_with_config(address.c_str(), config.get());
        if (!handle) {
            throw std::runtime_error("Failed to create KV client with config");
        }
    }
    
    ~KvClientWrapper() {
        if (handle) {
            kv_client_destroy(handle);
        }
    }
    
    KvClientHandle get() const { return handle; }
    
    KvClientWrapper(const KvClientWrapper&) = delete;
    KvClientWrapper& operator=(const KvClientWrapper&) = delete;
};

// Forward declarations for helper functions
inline int wait_for_future_c(KvFutureHandle future);
inline bool wait_for_future_cpp(KvFutureHandle future, int timeout_ms = 5000);

class KvTransactionWrapper {
private:
    KvTransactionHandle handle;
    bool committed;
    
public:
    explicit KvTransactionWrapper(KvClientWrapper& client, int timeout_seconds = 30) 
        : handle(nullptr), committed(false) {
        KvFutureHandle future = kv_transaction_begin(client.get(), timeout_seconds);
        if (!future) {
            throw std::runtime_error("Failed to begin transaction");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Transaction begin timeout");
        }
        
        handle = kv_future_get_transaction(future);
        if (!handle) {
            throw std::runtime_error("Failed to get transaction handle");
        }
    }
    
    ~KvTransactionWrapper() {
        // Auto-rollback if not committed (we don't have abort in FFI currently)
    }
    
    KvTransactionHandle get() const { return handle; }
    
    void set(const std::string& key, const std::string& value, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_set(handle,
                                                   (const uint8_t*)key.data(), key.size(),
                                                   (const uint8_t*)value.data(), value.size(),
                                                   cf);
        if (!future) {
            throw std::runtime_error("Failed to create set future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Set operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Set operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
    }
    
    std::string get(const std::string& key, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_get(handle,
                                                   (const uint8_t*)key.data(), key.size(),
                                                   cf);
        if (!future) {
            throw std::runtime_error("Failed to create get future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Get operation timeout");
        }
        
        KvBinaryData value;
        KvResult result = kv_future_get_value_result(future, &value);
        if (!result.success) {
            std::string error = "Get operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        std::string str_value;
        if (value.data && value.length > 0) {
            str_value = std::string((char*)value.data, value.length);
            kv_binary_free(&value);
        }
        
        return str_value;
    }
    
    void delete_key(const std::string& key, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_delete(handle,
                                                      (const uint8_t*)key.data(), key.size(),
                                                      cf);
        if (!future) {
            throw std::runtime_error("Failed to create delete future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Delete operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Delete operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
    }
    
    std::vector<std::pair<std::string, std::string>> get_range(const std::string* start_key = nullptr,
                                                                const std::string* end_key = nullptr,
                                                                int begin_offset = 0,
                                                                bool begin_or_equal = true,
                                                                int end_offset = 0,
                                                                bool end_or_equal = false,
                                                                int limit = -1,
                                                                const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        
        const uint8_t* start_key_data = start_key ? (const uint8_t*)start_key->data() : nullptr;
        int start_key_len = start_key ? start_key->size() : 0;
        const uint8_t* end_key_data = end_key ? (const uint8_t*)end_key->data() : nullptr;
        int end_key_len = end_key ? end_key->size() : 0;
        
        KvFutureHandle future = kv_transaction_get_range(handle,
                                                         start_key_data, start_key_len,
                                                         end_key_data, end_key_len,
                                                         begin_offset, begin_or_equal ? 1 : 0,
                                                         end_offset, end_or_equal ? 1 : 0,
                                                         limit, cf);
        if (!future) {
            throw std::runtime_error("Failed to create get_range future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Get range operation timeout");
        }
        
        KvPairArray pairs = {nullptr, 0};
        KvResult result = kv_future_get_kv_array_result(future, &pairs);
        if (!result.success) {
            std::string error = "Get range operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        std::vector<std::pair<std::string, std::string>> kv_vec;
        for (size_t i = 0; i < pairs.count; i++) {
            std::string key_str = pairs.pairs[i].key.data && pairs.pairs[i].key.length > 0 ?
                std::string((char*)pairs.pairs[i].key.data, pairs.pairs[i].key.length) : "";
            std::string value_str = pairs.pairs[i].value.data && pairs.pairs[i].value.length > 0 ?
                std::string((char*)pairs.pairs[i].value.data, pairs.pairs[i].value.length) : "";
            kv_vec.emplace_back(key_str, value_str);
        }
        
        kv_pair_array_free(&pairs);
        return kv_vec;
    }
    
    void commit() {
        KvFutureHandle future = kv_transaction_commit(handle);
        if (!future) {
            throw std::runtime_error("Failed to create commit future");
        }
        
        if (!wait_for_future_cpp(future)) {
            throw std::runtime_error("Commit operation timeout");
        }
        
        KvResult result = kv_future_get_void_result(future);
        if (!result.success) {
            std::string error = "Commit operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        committed = true;
    }
    
    KvTransactionWrapper(const KvTransactionWrapper&) = delete;
    KvTransactionWrapper& operator=(const KvTransactionWrapper&) = delete;
};

// Generic callback-based future awaiter for upgrading from polling
class FutureAwaiter {
private:
    std::mutex mutex;
    std::condition_variable cv;
    bool completed = false;
    bool error_occurred = false;

    static void awaiter_callback(KvFutureHandle future, void* context) {
        auto* awaiter = static_cast<FutureAwaiter*>(context);
        awaiter->signal_completion();
    }

    void signal_completion() {
        std::lock_guard<std::mutex> lock(mutex);
        completed = true;
        cv.notify_all();
    }

public:
    void reset() {
        std::lock_guard<std::mutex> lock(mutex);
        completed = false;
        error_occurred = false;
    }

    bool wait(KvFutureHandle future, int timeout_ms = 5000) {
        reset();

        // Set callback first
        int callback_result = kv_future_set_callback(future, awaiter_callback, this);
        if (callback_result != KV_FUNCTION_SUCCESS) {
            // Fallback to polling if callback fails
            return wait_with_polling_fallback(future, timeout_ms);
        }

        // Wait for callback to signal completion
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                          [this] { return completed; });
    }

private:
    // Fallback to polling if callback setup fails
    bool wait_with_polling_fallback(KvFutureHandle future, int timeout_ms) {
        auto start = std::chrono::steady_clock::now();
        while (true) {
            int status = kv_future_poll(future);
            if (status == KV_FUNCTION_SUCCESS) {
                return true;  // Ready
            } else if (status == KV_FUNCTION_ERROR) {
                return false;  // Error
            }

            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - start).count();
            if (elapsed > timeout_ms) {
                return false;  // Timeout
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
};

// Helper function to wait for future completion (C-style)
// Now uses callbacks with polling fallback for compatibility
inline int wait_for_future_c(KvFutureHandle future) {
    static thread_local FutureAwaiter awaiter;
    bool success = awaiter.wait(future, 5000);  // 5 second timeout
    if (success) {
        return KV_FUNCTION_SUCCESS;
    } else {
        return KV_FUNCTION_FAILURE;  // Could be timeout or error
    }
}

// Helper function to wait for future completion (C++-style)
// Now uses callbacks with polling fallback for compatibility
inline bool wait_for_future_cpp(KvFutureHandle future, int timeout_ms) {
    static thread_local FutureAwaiter awaiter;
    return awaiter.wait(future, timeout_ms);
}

// Callback testing utilities
struct CallbackTestContext {
    bool callback_called = false;
    KvFutureHandle future_handle = nullptr;
    void* user_context = nullptr;
    std::chrono::steady_clock::time_point callback_time;
    std::mutex mutex;
    std::condition_variable cv;

    void reset() {
        std::lock_guard<std::mutex> lock(mutex);
        callback_called = false;
        future_handle = nullptr;
        user_context = nullptr;
    }

    void notify_callback(KvFutureHandle future, void* context) {
        std::lock_guard<std::mutex> lock(mutex);
        callback_called = true;
        future_handle = future;
        user_context = context;
        callback_time = std::chrono::steady_clock::now();
        cv.notify_all();
    }

    bool wait_for_callback(int timeout_ms = 5000) {
        std::unique_lock<std::mutex> lock(mutex);
        return cv.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                          [this] { return callback_called; });
    }
};

// Global callback test context for simple tests
extern CallbackTestContext g_callback_context;

// Simple callback function for testing
extern "C" void test_callback_simple(KvFutureHandle future, void* user_context);

// Callback with custom context
extern "C" void test_callback_with_context(KvFutureHandle future, void* user_context);

// Thread-safe callback counter for concurrency tests
struct CallbackCounter {
    std::atomic<int> count{0};
    std::atomic<bool> error_occurred{false};

    void reset() {
        count.store(0);
        error_occurred.store(false);
    }

    void increment() {
        count.fetch_add(1);
    }

    void set_error() {
        error_occurred.store(true);
    }

    int get_count() const {
        return count.load();
    }

    bool has_error() const {
        return error_occurred.load();
    }
};

extern CallbackCounter g_callback_counter;

// Counting callback for concurrency tests
extern "C" void test_callback_counting(KvFutureHandle future, void* user_context);