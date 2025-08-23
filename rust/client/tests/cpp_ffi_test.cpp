#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <cassert>
#include <cstring>
#include <functional>
#include "../include/kvstore_client.h"

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

// Helper function to wait for future completion with timeout
bool wait_for_future(KvFutureHandle future, int timeout_ms = 5000) {
    auto start = std::chrono::steady_clock::now();
    while (true) {
        int status = kv_future_poll(future);
        if (status == 1) {
            return true;  // Ready
        } else if (status == -1) {
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

// Test basic functionality
void test_basic_functionality() {
    FFITest test("Basic Functionality");
    
    int init_result = kv_init();
    test.assert_true(init_result == 0, "kv_init failed");
    
    KvClientHandle client = kv_client_create("localhost:9090");
    test.assert_true(client != nullptr, "Failed to create client");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    test.assert_true(tx_future != nullptr, "Failed to begin transaction");
    
    bool ready = wait_for_future(tx_future);
    test.assert_true(ready, "Transaction begin future not ready");
    
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    test.assert_true(tx != nullptr, "Failed to get transaction handle");
    
    // Set a key-value pair
    KvFutureHandle set_future = kv_transaction_set(tx, "cpp_test_key", "cpp_test_value", nullptr);
    test.assert_true(set_future != nullptr, "Failed to create set future");
    
    ready = wait_for_future(set_future);
    test.assert_true(ready, "Set future not ready");
    
    KvResult set_result = kv_future_get_void_result(set_future);
    test.assert_true(set_result.success == 1, "Set operation failed");
    if (set_result.error_message) {
        std::cerr << "Set error: " << set_result.error_message << std::endl;
    }
    
    // Get the value back
    KvFutureHandle get_future = kv_transaction_get(tx, "cpp_test_key", nullptr);
    test.assert_true(get_future != nullptr, "Failed to create get future");
    
    ready = wait_for_future(get_future);
    test.assert_true(ready, "Get future not ready");
    
    char* value = nullptr;
    KvResult get_result = kv_future_get_string_result(get_future, &value);
    test.assert_true(get_result.success == 1, "Get operation failed");
    test.assert_true(value != nullptr, "Got NULL value");
    test.assert_true(std::strcmp(value, "cpp_test_value") == 0, "Value mismatch");
    
    // Commit transaction
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    test.assert_true(commit_future != nullptr, "Failed to create commit future");
    
    ready = wait_for_future(commit_future);
    test.assert_true(ready, "Commit future not ready");
    
    KvResult commit_result = kv_future_get_void_result(commit_future);
    test.assert_true(commit_result.success == 1, "Commit operation failed");
    
    // Cleanup
    kv_string_free(value);
    kv_client_destroy(client);
    kv_shutdown();
    
    test.pass();
}

// Test RAII wrapper for better C++ integration
class KvClientWrapper {
private:
    KvClientHandle handle;
    
public:
    explicit KvClientWrapper(const std::string& address) {
        handle = kv_client_create(address.c_str());
        if (!handle) {
            throw std::runtime_error("Failed to create KV client");
        }
    }
    
    ~KvClientWrapper() {
        if (handle) {
            kv_client_destroy(handle);
        }
    }
    
    KvClientHandle get() const { return handle; }
    
    // Delete copy constructor and assignment operator
    KvClientWrapper(const KvClientWrapper&) = delete;
    KvClientWrapper& operator=(const KvClientWrapper&) = delete;
};

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
        
        if (!wait_for_future(future)) {
            throw std::runtime_error("Transaction begin timeout");
        }
        
        handle = kv_future_get_transaction(future);
        if (!handle) {
            throw std::runtime_error("Failed to get transaction handle");
        }
    }
    
    ~KvTransactionWrapper() {
        // Auto-rollback if not committed
        if (handle && !committed) {
            // Note: We don't have abort in the current FFI, so we just clean up
        }
    }
    
    KvTransactionHandle get() const { return handle; }
    
    void set(const std::string& key, const std::string& value, const std::string* column_family = nullptr) {
        const char* cf = column_family ? column_family->c_str() : nullptr;
        KvFutureHandle future = kv_transaction_set(handle, key.c_str(), value.c_str(), cf);
        if (!future) {
            throw std::runtime_error("Failed to create set future");
        }
        
        if (!wait_for_future(future)) {
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
        KvFutureHandle future = kv_transaction_get(handle, key.c_str(), cf);
        if (!future) {
            throw std::runtime_error("Failed to create get future");
        }
        
        if (!wait_for_future(future)) {
            throw std::runtime_error("Get operation timeout");
        }
        
        char* value = nullptr;
        KvResult result = kv_future_get_string_result(future, &value);
        if (!result.success) {
            std::string error = "Get operation failed";
            if (result.error_message) {
                error += ": " + std::string(result.error_message);
            }
            throw std::runtime_error(error);
        }
        
        std::string str_value;
        if (value) {
            str_value = value;
            kv_string_free(value);
        }
        
        return str_value;
    }
    
    void commit() {
        KvFutureHandle future = kv_transaction_commit(handle);
        if (!future) {
            throw std::runtime_error("Failed to create commit future");
        }
        
        if (!wait_for_future(future)) {
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
    
    // Delete copy constructor and assignment operator
    KvTransactionWrapper(const KvTransactionWrapper&) = delete;
    KvTransactionWrapper& operator=(const KvTransactionWrapper&) = delete;
};

// Test C++ RAII wrapper
void test_cpp_wrapper() {
    FFITest test("C++ RAII Wrapper");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        {
            KvTransactionWrapper tx(client);
            
            // Test multiple operations
            tx.set("wrapper_key_1", "wrapper_value_1");
            tx.set("wrapper_key_2", "wrapper_value_2");
            
            std::string value1 = tx.get("wrapper_key_1");
            std::string value2 = tx.get("wrapper_key_2");
            
            test.assert_true(value1 == "wrapper_value_1", "Value1 mismatch");
            test.assert_true(value2 == "wrapper_value_2", "Value2 mismatch");
            
            tx.commit();
        } // Transaction auto-destructs here
        
        // Verify data persisted
        {
            KvTransactionWrapper tx(client);
            
            std::string value1 = tx.get("wrapper_key_1");
            test.assert_true(value1 == "wrapper_value_1", "Persisted value1 mismatch");
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test error handling
void test_error_handling() {
    FFITest test("Error Handling");
    
    kv_init();
    
    // Test with invalid client
    KvFutureHandle future = kv_transaction_begin(nullptr, 30);
    test.assert_true(future == nullptr, "Should fail with NULL client");
    
    // Test with non-existent server
    KvClientHandle bad_client = kv_client_create("localhost:19999");
    test.assert_true(bad_client == nullptr, "Should fail with bad address");
    
    // Test polling NULL future
    int result = kv_future_poll(nullptr);
    test.assert_true(result == -1, "Should return -1 for NULL future");
    
    kv_shutdown();
    test.pass();
}

// Test concurrent operations
void test_concurrent_operations() {
    FFITest test("Concurrent Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Create multiple transactions in parallel
        std::vector<std::thread> threads;
        std::vector<bool> results(4, false);
        
        for (int i = 0; i < 4; i++) {
            threads.emplace_back([&, i]() {
                try {
                    KvTransactionWrapper tx(client);
                    
                    std::string key = "concurrent_key_" + std::to_string(i);
                    std::string value = "concurrent_value_" + std::to_string(i);
                    
                    tx.set(key, value);
                    
                    std::string retrieved = tx.get(key);
                    if (retrieved == value) {
                        tx.commit();
                        results[i] = true;
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Thread " << i << " error: " << e.what() << std::endl;
                }
            });
        }
        
        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
        
        // Check results
        for (int i = 0; i < 4; i++) {
            test.assert_true(results[i], "Thread " + std::to_string(i) + " failed");
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Main test runner
int main() {
    std::cout << "Running KV Store C++ FFI Tests" << std::endl;
    std::cout << "==============================" << std::endl;
    
    std::vector<std::pair<std::string, std::function<void()>>> tests = {
        {"Basic Functionality", test_basic_functionality},
        {"C++ RAII Wrapper", test_cpp_wrapper},
        {"Error Handling", test_error_handling},
        {"Concurrent Operations", test_concurrent_operations}
    };
    
    int passed = 0;
    int failed = 0;
    
    for (size_t i = 0; i < tests.size(); i++) {
        std::cout << "\n[" << (i + 1) << "] Running " << tests[i].first << "..." << std::endl;
        try {
            tests[i].second();
            passed++;
        } catch (const std::exception& e) {
            std::cerr << "FAIL: " << tests[i].first << " - " << e.what() << std::endl;
            failed++;
        }
    }
    
    std::cout << "\n==============================" << std::endl;
    std::cout << "Test Results: " << passed << " passed, " << failed << " failed" << std::endl;
    
    if (failed > 0) {
        std::cout << "\nNote: Some tests may fail if the KV server is not running on localhost:9090" << std::endl;
        std::cout << "Start the server with: ./bin/rocksdbserver-thrift" << std::endl;
        return 1;
    }
    
    return 0;
}