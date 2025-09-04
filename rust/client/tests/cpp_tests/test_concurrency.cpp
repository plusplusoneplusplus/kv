#include "test_common.hpp"

//==============================================================================
// Concurrency Tests
//==============================================================================

// Test concurrent operations
void test_cpp_concurrent_operations() {
    FFITest test("C++ Concurrent Operations");
    
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

// Test concurrent reads and writes
void test_cpp_concurrent_read_write() {
    FFITest test("C++ Concurrent Read/Write");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Setup initial data
        {
            KvTransactionWrapper setup_tx(client);
            for (int i = 0; i < 10; i++) {
                setup_tx.set("shared_key_" + std::to_string(i), "initial_value_" + std::to_string(i));
            }
            setup_tx.commit();
        }
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_reads(0);
        std::atomic<int> successful_writes(0);
        std::atomic<bool> error_occurred(false);
        
        // Launch reader threads
        for (int i = 0; i < 3; i++) {
            threads.emplace_back([&, i]() {
                try {
                    for (int j = 0; j < 5; j++) {
                        KvTransactionWrapper tx(client);
                        
                        std::string key = "shared_key_" + std::to_string(j % 10);
                        std::string value = tx.get(key);
                        
                        if (!value.empty() && (value.find("initial_value_") == 0 || value.find("updated_value_") == 0)) {
                            successful_reads.fetch_add(1);
                        }
                        
                        tx.commit();
                        std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Reader thread " << i << " error: " << e.what() << std::endl;
                    error_occurred = true;
                }
            });
        }
        
        // Launch writer threads
        for (int i = 0; i < 2; i++) {
            threads.emplace_back([&, i]() {
                try {
                    for (int j = 0; j < 3; j++) {
                        KvTransactionWrapper tx(client);
                        
                        std::string key = "shared_key_" + std::to_string((i * 3 + j) % 10);
                        std::string value = "updated_value_" + std::to_string(i) + "_" + std::to_string(j);
                        
                        tx.set(key, value);
                        tx.commit();
                        
                        successful_writes.fetch_add(1);
                        std::this_thread::sleep_for(std::chrono::milliseconds(15));
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Writer thread " << i << " error: " << e.what() << std::endl;
                    error_occurred = true;
                }
            });
        }
        
        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
        
        test.assert_true(!error_occurred, "No errors should occur during concurrent operations");
        test.assert_true(successful_reads.load() > 0, "Some reads should succeed");
        test.assert_true(successful_writes.load() > 0, "Some writes should succeed");
        
        std::cout << "Concurrent test results: " << successful_reads.load() 
                  << " successful reads, " << successful_writes.load() << " successful writes" << std::endl;
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test concurrent range operations
void test_cpp_concurrent_range_operations() {
    FFITest test("C++ Concurrent Range Operations");
    
    kv_init();
    
    try {
        KvClientWrapper client("localhost:9090");
        
        // Setup test data
        {
            KvTransactionWrapper setup_tx(client);
            for (int i = 0; i < 50; i++) {
                std::string key = "range_concurrent_" + std::string(3 - std::to_string(i).length(), '0') + std::to_string(i);
                std::string value = "value_" + std::to_string(i);
                setup_tx.set(key, value);
            }
            setup_tx.commit();
        }
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_range_ops(0);
        std::atomic<bool> error_occurred(false);
        
        // Launch range query threads
        for (int i = 0; i < 4; i++) {
            threads.emplace_back([&, i]() {
                try {
                    for (int j = 0; j < 10; j++) {
                        KvTransactionWrapper tx(client);
                        
                        // Different range queries
                        std::string prefix = "range_concurrent_";
                        if (i % 2 == 0) {
                            // Get all items
                            auto results = tx.get_range(prefix);
                            if (results.size() > 0) {
                                successful_range_ops.fetch_add(1);
                            }
                        } else {
                            // Get limited items
                            auto results = tx.get_range(prefix, nullptr, 10 + (j % 5));
                            if (results.size() > 0) {
                                successful_range_ops.fetch_add(1);
                            }
                        }
                        
                        tx.commit();
                        std::this_thread::sleep_for(std::chrono::milliseconds(5));
                    }
                } catch (const std::exception& e) {
                    std::cerr << "Range thread " << i << " error: " << e.what() << std::endl;
                    error_occurred = true;
                }
            });
        }
        
        // Launch concurrent modification thread
        threads.emplace_back([&]() {
            try {
                for (int i = 0; i < 5; i++) {
                    KvTransactionWrapper tx(client);
                    
                    // Add new items during range operations
                    std::string key = "range_concurrent_new_" + std::to_string(i);
                    std::string value = "new_value_" + std::to_string(i);
                    tx.set(key, value);
                    
                    tx.commit();
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                }
            } catch (const std::exception& e) {
                std::cerr << "Modification thread error: " << e.what() << std::endl;
                error_occurred = true;
            }
        });
        
        // Wait for all threads
        for (auto& thread : threads) {
            thread.join();
        }
        
        test.assert_true(!error_occurred, "No errors should occur during concurrent range operations");
        test.assert_true(successful_range_ops.load() > 0, "Some range operations should succeed");
        
        std::cout << "Concurrent range test: " << successful_range_ops.load() << " successful range operations" << std::endl;
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}

// Test thread safety with shared client
void test_cpp_thread_safety() {
    FFITest test("C++ Thread Safety");
    
    kv_init();
    
    try {
        // Test that a single client can be used safely across multiple threads
        KvClientWrapper client("localhost:9090");
        
        const int num_threads = 6;
        const int ops_per_thread = 20;
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_operations(0);
        std::atomic<int> total_operations(0);
        std::mutex cout_mutex;
        
        for (int thread_id = 0; thread_id < num_threads; thread_id++) {
            threads.emplace_back([&, thread_id]() {
                try {
                    for (int op = 0; op < ops_per_thread; op++) {
                        KvTransactionWrapper tx(client);
                        
                        std::string key = "thread_" + std::to_string(thread_id) + "_op_" + std::to_string(op);
                        std::string value = "data_" + std::to_string(thread_id) + "_" + std::to_string(op);
                        
                        // Perform set operation
                        tx.set(key, value);
                        
                        // Verify immediately
                        std::string retrieved = tx.get(key);
                        
                        if (retrieved == value) {
                            tx.commit();
                            successful_operations.fetch_add(1);
                        } else {
                            std::lock_guard<std::mutex> lock(cout_mutex);
                            std::cout << "Thread " << thread_id << " op " << op 
                                     << ": value mismatch. Expected: " << value 
                                     << ", Got: " << retrieved << std::endl;
                        }
                        
                        total_operations.fetch_add(1);
                        
                        // Small delay to increase chance of interleaving
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    }
                } catch (const std::exception& e) {
                    std::lock_guard<std::mutex> lock(cout_mutex);
                    std::cerr << "Thread " << thread_id << " encountered error: " << e.what() << std::endl;
                }
            });
        }
        
        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }
        
        int expected_operations = num_threads * ops_per_thread;
        test.assert_true(total_operations.load() == expected_operations, 
                        "All operations should have been attempted");
        
        // Allow for some failures due to concurrency, but most should succeed
        double success_rate = static_cast<double>(successful_operations.load()) / total_operations.load();
        test.assert_true(success_rate > 0.8, "At least 80% of operations should succeed");
        
        std::cout << "Thread safety test: " << successful_operations.load() 
                  << "/" << total_operations.load() << " operations succeeded ("
                  << (success_rate * 100.0) << "%)" << std::endl;
        
        // Verify data integrity after concurrent operations
        {
            KvTransactionWrapper verify_tx(client);
            int verified_keys = 0;
            
            for (int thread_id = 0; thread_id < num_threads; thread_id++) {
                for (int op = 0; op < ops_per_thread; op++) {
                    std::string key = "thread_" + std::to_string(thread_id) + "_op_" + std::to_string(op);
                    std::string expected_value = "data_" + std::to_string(thread_id) + "_" + std::to_string(op);
                    
                    std::string actual_value = verify_tx.get(key);
                    if (actual_value == expected_value) {
                        verified_keys++;
                    }
                }
            }
            
            verify_tx.commit();
            
            std::cout << "Data integrity check: " << verified_keys 
                     << "/" << expected_operations << " keys verified" << std::endl;
            
            test.assert_true(verified_keys == successful_operations.load(), 
                           "All successfully committed operations should be verifiable");
        }
        
    } catch (const std::exception& e) {
        kv_shutdown();
        test.assert_true(false, std::string("Exception: ") + e.what());
    }
    
    kv_shutdown();
    test.pass();
}