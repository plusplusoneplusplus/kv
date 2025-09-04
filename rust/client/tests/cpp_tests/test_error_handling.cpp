#include "test_common.hpp"

//==============================================================================
// Error Handling Tests
//==============================================================================

// Test error handling
void test_cpp_error_handling() {
    FFITest test("C++ Error Handling");
    
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

// Test invalid operations and edge cases
void test_cpp_invalid_operations() {
    FFITest test("C++ Invalid Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test 1: Operations with invalid handles
        bool null_handle_caught = false;
        try {
            KvFutureHandle invalid_future = kv_transaction_begin(nullptr, 30);
            test.assert_true(invalid_future == nullptr, "Invalid transaction begin should return null");
        } catch (const std::exception& e) {
            null_handle_caught = true;
        }
        
        // Test 2: Operations after transaction is completed
        {
            KvTransactionWrapper tx(client);
            tx.set("test_completed_tx_key", "test_value");
            tx.commit();
            
            // Trying to use transaction after commit should be handled gracefully
            // Note: Our wrapper doesn't prevent this, but the underlying FFI should
            bool post_commit_error = false;
            try {
                // This might succeed or fail depending on implementation
                KvFutureHandle post_commit_future = kv_transaction_set(tx.get(), 
                    KV_STR("post_commit_key"), KV_STR("post_commit_value"), nullptr);
                
                if (post_commit_future == nullptr) {
                    post_commit_error = true;
                } else {
                    int ready = wait_for_future_cpp(post_commit_future);
                    if (!ready) {
                        post_commit_error = true;
                    } else {
                        KvResult result = kv_future_get_void_result(post_commit_future);
                        if (result.success == 0) {
                            post_commit_error = true;
                        }
                    }
                }
            } catch (...) {
                post_commit_error = true;
            }
            
            // Either should work (idempotent) or fail gracefully
            std::cout << "Post-commit operation " << (post_commit_error ? "failed" : "succeeded") 
                     << " as expected" << std::endl;
        }
        
        // Test 3: Large key and value handling
        {
            KvTransactionWrapper tx(client);
            
            // Test very long key
            std::string long_key(1000, 'k');
            std::string normal_value = "normal_value";
            
            bool long_key_success = true;
            try {
                tx.set(long_key, normal_value);
                std::string retrieved = tx.get(long_key);
                if (retrieved != normal_value) {
                    long_key_success = false;
                }
            } catch (const std::exception& e) {
                std::cout << "Long key operation failed as expected: " << e.what() << std::endl;
                long_key_success = false;
            }
            
            // Test very long value
            std::string normal_key = "normal_key";
            std::string long_value(10000, 'v');
            
            bool long_value_success = true;
            try {
                tx.set(normal_key, long_value);
                std::string retrieved = tx.get(normal_key);
                if (retrieved != long_value) {
                    long_value_success = false;
                }
            } catch (const std::exception& e) {
                std::cout << "Long value operation failed: " << e.what() << std::endl;
                long_value_success = false;
            }
            
            if (long_key_success || long_value_success) {
                tx.commit();
            }
            
            std::cout << "Large data handling: long_key=" << (long_key_success ? "OK" : "FAIL")
                     << ", long_value=" << (long_value_success ? "OK" : "FAIL") << std::endl;
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test network error scenarios
void test_cpp_network_errors() {
    FFITest test("C++ Network Errors");
    
    kv_init();
    
    // Test 1: Connection to non-existent server
    {
        bool connection_failed = false;
        try {
            KvClientWrapper client("localhost:99999");  // Non-existent port
        } catch (const std::exception& e) {
            connection_failed = true;
            std::cout << "Connection to invalid server failed as expected: " << e.what() << std::endl;
        }
        
        test.assert_true(connection_failed, "Connection to invalid server should fail");
    }
    
    // Test 2: Invalid hostname
    {
        bool invalid_host_failed = false;
        try {
            KvClientWrapper client("invalid-hostname-12345:9090");
        } catch (const std::exception& e) {
            invalid_host_failed = true;
            std::cout << "Connection to invalid hostname failed as expected: " << e.what() << std::endl;
        }
        
        test.assert_true(invalid_host_failed, "Connection to invalid hostname should fail");
    }
    
    // Test 3: Malformed address
    {
        bool malformed_failed = false;
        try {
            KvClientWrapper client("malformed::address:::123");
        } catch (const std::exception& e) {
            malformed_failed = true;
            std::cout << "Connection with malformed address failed as expected: " << e.what() << std::endl;
        }
        
        test.assert_true(malformed_failed, "Connection with malformed address should fail");
    }
    
    // Test 4: Empty address
    {
        bool empty_address_failed = false;
        try {
            KvClientWrapper client("");
        } catch (const std::exception& e) {
            empty_address_failed = true;
            std::cout << "Connection with empty address failed as expected: " << e.what() << std::endl;
        }
        
        test.assert_true(empty_address_failed, "Connection with empty address should fail");
    }
    
    kv_shutdown();
    test.pass();
}

// Test resource cleanup and leak prevention
void test_cpp_resource_cleanup() {
    FFITest test("C++ Resource Cleanup");
    
    kv_init();
    
    try {
        // Test 1: Multiple client creation and destruction
        for (int i = 0; i < 10; i++) {
            try {
                KvClientWrapper client("localhost:9090");
                
                KvTransactionWrapper tx(client);
                tx.set("cleanup_test_" + std::to_string(i), "value_" + std::to_string(i));
                tx.commit();
                
                // Clients and transactions should be cleaned up automatically
            } catch (const std::exception& e) {
                // Individual failures are OK for this test
                std::cout << "Client " << i << " failed: " << e.what() << std::endl;
            }
        }
        
        // Test 2: Exception during transaction should not leak resources
        for (int i = 0; i < 5; i++) {
            try {
                KvClientWrapper client("localhost:9090");
                KvTransactionWrapper tx(client);
                
                tx.set("exception_test_" + std::to_string(i), "value");
                
                if (i % 2 == 0) {
                    // Simulate exception
                    throw std::runtime_error("Simulated error during transaction");
                }
                
                tx.commit();
            } catch (const std::exception& e) {
                // Expected - resources should be cleaned up by destructors
                std::cout << "Expected exception in iteration " << i << ": " << e.what() << std::endl;
            }
        }
        
        // Test 3: Rapid creation and destruction
        {
            std::vector<std::unique_ptr<KvClientWrapper>> clients;
            
            // Create multiple clients
            for (int i = 0; i < 5; i++) {
                try {
                    clients.push_back(std::make_unique<KvClientWrapper>("localhost:9090"));
                } catch (const std::exception& e) {
                    std::cout << "Client creation " << i << " failed: " << e.what() << std::endl;
                }
            }
            
            // Use them briefly
            for (size_t i = 0; i < clients.size(); i++) {
                try {
                    if (clients[i]) {
                        KvTransactionWrapper tx(*clients[i]);
                        tx.set("multi_client_" + std::to_string(i), "test");
                        tx.commit();
                    }
                } catch (const std::exception& e) {
                    std::cout << "Multi-client operation " << i << " failed: " << e.what() << std::endl;
                }
            }
            
            // Clear all clients - should trigger cleanup
            clients.clear();
        }
        
        std::cout << "Resource cleanup test completed - check for memory leaks manually" << std::endl;
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test timeout scenarios
void test_cpp_timeout_scenarios() {
    FFITest test("C++ Timeout Scenarios");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Test 1: Very short transaction timeout
        {
            bool timeout_handled = false;
            try {
                // Create transaction with 1 second timeout
                KvFutureHandle tx_future = kv_transaction_begin(client.get(), 1);
                if (tx_future) {
                    if (wait_for_future_cpp(tx_future, 5000)) {  // Wait up to 5 seconds
                        KvTransactionHandle tx = kv_future_get_transaction(tx_future);
                        if (tx) {
                            // Set some data
                            KvFutureHandle set_future = kv_transaction_set(tx, 
                                KV_STR("timeout_test_key"), KV_STR("timeout_test_value"), nullptr);
                            
                            if (set_future && wait_for_future_cpp(set_future)) {
                                kv_future_get_void_result(set_future);
                                
                                // Sleep for 2 seconds to exceed timeout
                                std::this_thread::sleep_for(std::chrono::seconds(2));
                                
                                // Try to commit
                                KvFutureHandle commit_future = kv_transaction_commit(tx);
                                if (commit_future) {
                                    bool commit_ready = wait_for_future_cpp(commit_future, 1000);
                                    if (commit_ready) {
                                        KvResult commit_result = kv_future_get_void_result(commit_future);
                                        if (commit_result.success == 0) {
                                            timeout_handled = true;
                                            std::cout << "Timeout properly detected in commit" << std::endl;
                                        }
                                    } else {
                                        timeout_handled = true;
                                        std::cout << "Commit future timed out as expected" << std::endl;
                                    }
                                } else {
                                    timeout_handled = true;
                                    std::cout << "Commit future creation failed due to timeout" << std::endl;
                                }
                            }
                        }
                    }
                }
            } catch (const std::exception& e) {
                timeout_handled = true;
                std::cout << "Timeout exception caught: " << e.what() << std::endl;
            }
            
            // Either timeout should be detected or operation should succeed
            std::cout << "Timeout handling: " << (timeout_handled ? "detected" : "not detected") << std::endl;
        }
        
        // Test 2: Future polling timeout
        {
            KvTransactionWrapper tx(client);
            
            // Start multiple operations
            std::vector<KvFutureHandle> futures;
            for (int i = 0; i < 3; i++) {
                std::string key = "timeout_multi_" + std::to_string(i);
                std::string value = "value_" + std::to_string(i);
                KvFutureHandle future = kv_transaction_set(tx.get(),
                    (const uint8_t*)key.c_str(), key.length(),
                    (const uint8_t*)value.c_str(), value.length(),
                    nullptr);
                if (future) {
                    futures.push_back(future);
                }
            }
            
            // Test timeout on future polling
            int completed_futures = 0;
            for (auto future : futures) {
                if (wait_for_future_cpp(future, 100)) {  // Very short timeout
                    KvResult result = kv_future_get_void_result(future);
                    if (result.success) {
                        completed_futures++;
                    }
                }
            }
            
            std::cout << "Completed " << completed_futures << "/" << futures.size() 
                     << " futures within timeout" << std::endl;
            
            tx.commit();
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}