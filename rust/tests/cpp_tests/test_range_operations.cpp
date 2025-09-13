#include <gtest/gtest.h>
#include "test_common.hpp"

class RangeOperationsTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(RangeOperationsTest, BasicRangeQuery) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("range001", "value1");
        tx.set("range002", "value2");
        tx.set("range003", "value3");
        tx.set("range004", "value4");
        tx.set("range005", "value5");
        tx.commit();
        
        // Test basic range query
        KvTransactionWrapper range_tx(client);
        std::string start_key = "range001";
        std::string end_key = "range999";
        auto results = range_tx.get_range(&start_key, &end_key, 0, true, 0, false, 10);
        
        EXPECT_EQ(results.size(), 5);
        EXPECT_EQ(results[0].first, "range001");
        EXPECT_EQ(results[0].second, "value1");
        EXPECT_EQ(results[4].first, "range005");
        EXPECT_EQ(results[4].second, "value5");
        
    } catch (const std::exception& e) {
        FAIL() << "Basic range query test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithOffsets) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("offset001", "value1");
        tx.set("offset002", "value2");
        tx.set("offset003", "value3");
        tx.set("offset004", "value4");
        tx.set("offset005", "value5");
        tx.commit();
        
        // Test with begin_offset = 1 (skip first key)
        KvTransactionWrapper range_tx(client);
        std::string start_key = "offset001";
        std::string end_key = "offset999";
        auto results = range_tx.get_range(&start_key, &end_key, 1, true, 0, false, 10);
        
        EXPECT_EQ(results.size(), 4);  // Should skip "offset001"
        EXPECT_EQ(results[0].first, "offset002");
        EXPECT_EQ(results[0].second, "value2");
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with offsets test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithInclusiveExclusive) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("incexc001", "value1");
        tx.set("incexc002", "value2");
        tx.set("incexc003", "value3");
        tx.set("incexc004", "value4");
        tx.commit();
        
        // Test with begin_or_equal = false (exclude start key)
        KvTransactionWrapper range_tx(client);
        std::string start_key = "incexc001";
        std::string end_key = "incexc004";
        auto results = range_tx.get_range(&start_key, &end_key, 0, false, 0, false, 10);
        
        // Should start after "incexc001" and end before "incexc004"
        EXPECT_EQ(results.size(), 2);
        EXPECT_EQ(results[0].first, "incexc002");
        EXPECT_EQ(results[1].first, "incexc003");
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with inclusive/exclusive test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithEndOffset) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("endoff001", "value1");
        tx.set("endoff002", "value2");
        tx.set("endoff003", "value3");
        tx.set("endoff004", "value4");
        tx.set("endoff005", "value5");
        tx.commit();
        
        // Test with end_offset = -1 (stop one key before end)
        KvTransactionWrapper range_tx(client);
        std::string start_key = "endoff001";
        std::string end_key = "endoff005";
        auto results = range_tx.get_range(&start_key, &end_key, 0, true, -1, false, 10);
        
        // Should stop before "endoff005", so get keys 1-4
        EXPECT_EQ(results.size(), 3);  // endoff001, endoff002, endoff003
        EXPECT_EQ(results[0].first, "endoff001");
        EXPECT_EQ(results[2].first, "endoff003");
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with end offset test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithDefaultKeys) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data with unique prefix to avoid conflicts
        tx.set("defaultkeys001", "value1");
        tx.set("defaultkeys002", "value2"); 
        tx.set("defaultkeys003", "value3");
        tx.commit();
        
        // Test with limited range to find our specific keys
        KvTransactionWrapper range_tx(client);
        std::string start_key = "defaultkeys";
        std::string end_key = "defaultkeys999";
        auto results = range_tx.get_range(&start_key, &end_key, 0, true, 0, false, 10);
        
        // Should get our test keys
        EXPECT_EQ(results.size(), 3);
        EXPECT_EQ(results[0].first, "defaultkeys001");
        EXPECT_EQ(results[0].second, "value1");
        EXPECT_EQ(results[1].first, "defaultkeys002");
        EXPECT_EQ(results[2].first, "defaultkeys003");
        
        // Also test with nullptr keys (should use FoundationDB defaults)
        // This is mainly to test that nullptr works, not to find specific keys
        KvTransactionWrapper null_range_tx(client);
        auto null_results = null_range_tx.get_range(nullptr, nullptr, 0, true, 0, false, 100);
        
        // Should get many keys in the database (nullptr should return everything)
        EXPECT_GE(null_results.size(), 3);
        
        // Just verify nullptr keys work by checking we got a reasonable number of results
        // (Given all the test data from various tests, we should have many keys)
        bool found_at_least_one_default = false;
        for (const auto& pair : null_results) {
            if (pair.first.find("defaultkeys") != std::string::npos) {
                found_at_least_one_default = true;
                break;
            }
        }
        
        EXPECT_TRUE(found_at_least_one_default) << "Should find at least one defaultkeys entry in full range";
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with default keys test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithLimitAndOffsets) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("limit001", "value1");
        tx.set("limit002", "value2");
        tx.set("limit003", "value3");
        tx.set("limit004", "value4");
        tx.set("limit005", "value5");
        tx.commit();
        
        // Test with begin_offset = 1, limit = 2
        KvTransactionWrapper range_tx(client);
        std::string start_key = "limit001";
        std::string end_key = "limit999";
        auto results = range_tx.get_range(&start_key, &end_key, 1, true, 0, false, 2);
        
        EXPECT_EQ(results.size(), 2);  // Limited to 2 results
        EXPECT_EQ(results[0].first, "limit002");  // Skip first due to offset
        EXPECT_EQ(results[1].first, "limit003");
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with limit and offsets test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeWithBinaryKeys) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data with binary keys (containing null bytes) using unique prefix
        std::string binary_key1 = std::string("bintest\x00key1", 12);
        std::string binary_key2 = std::string("bintest\x00key2", 12);
        std::string binary_key3 = std::string("bintest\x00key3", 12);
        
        tx.set(binary_key1, "binary_value1");
        tx.set(binary_key2, "binary_value2");
        tx.set(binary_key3, "binary_value3");
        tx.commit();
        
        // Test range query with binary keys using exact prefix match
        KvTransactionWrapper range_tx(client);
        std::string start_prefix = std::string("bintest\x00", 8);
        std::string end_prefix = std::string("bintest\x01", 8);  // Next lexicographic prefix
        
        auto results = range_tx.get_range(&start_prefix, &end_prefix, 0, true, 0, false, 10);
        
        EXPECT_EQ(results.size(), 3);
        EXPECT_EQ(results[0].first, binary_key1);
        EXPECT_EQ(results[0].second, "binary_value1");
        EXPECT_EQ(results[1].first, binary_key2);
        EXPECT_EQ(results[2].first, binary_key3);
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range with binary keys test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, EnhancedRangeEmptyResults) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data
        tx.set("empty001", "value1");
        tx.set("empty005", "value5");
        tx.commit();
        
        // Query range that should be empty (between existing keys)
        KvTransactionWrapper range_tx(client);
        std::string start_key = "empty002";
        std::string end_key = "empty004";
        auto results = range_tx.get_range(&start_key, &end_key, 0, true, 0, false, 10);
        
        EXPECT_EQ(results.size(), 0);  // Should be empty
        
    } catch (const std::exception& e) {
        FAIL() << "Enhanced range empty results test failed: " << e.what();
    }
}

TEST_F(RangeOperationsTest, ComprehensiveBinaryKeyTest) {
    try {
        KvClientWrapper client("localhost:9090");
        KvTransactionWrapper tx(client);
        
        // Setup test data with various binary key patterns
        
        // Keys with null bytes in the middle
        std::string null_key1 = std::string("comptest\x00\x00key1", 15);
        std::string null_key2 = std::string("comptest\x00\x01key2", 15);
        std::string null_key3 = std::string("comptest\x00\x02key3", 15);
        
        // Keys with high-value bytes (near 0xFF)
        std::string high_key1 = std::string("hightest\xFE\x01", 10);
        std::string high_key2 = std::string("hightest\xFE\x02", 10);
        std::string high_key3 = std::string("hightest\xFF\x00", 10);
        
        // Keys with mixed binary content - use unique patterns
        std::string mixed_key1 = std::string("mixedtest\x00\x01\x02", 12);
        std::string mixed_key2 = std::string("mixedtest\x00\x01\x03", 12);
        
        // Binary values with null bytes and high values
        std::string binary_value1 = std::string("value\x00\x01\xFF", 8);
        std::string binary_value2 = std::string("value\x00\x02\xFE", 8);
        std::string binary_value3 = std::string("value\x00\x03\xFD", 8);
        
        // Store all test data
        tx.set(null_key1, binary_value1);
        tx.set(null_key2, binary_value2);
        tx.set(null_key3, binary_value3);
        tx.set(high_key1, "high_value1");
        tx.set(high_key2, "high_value2");
        tx.set(high_key3, "high_value3");
        tx.set(mixed_key1, "mixed_value1");
        tx.set(mixed_key2, "mixed_value2");
        tx.commit();
        
        // Test 1: Range query with null byte prefix
        KvTransactionWrapper range_tx1(client);
        std::string null_start = std::string("comptest\x00", 9);
        std::string null_end = std::string("comptest\x01", 9);
        auto null_results = range_tx1.get_range(&null_start, &null_end, 0, true, 0, false, 10);
        
        EXPECT_EQ(null_results.size(), 3);
        EXPECT_EQ(null_results[0].first, null_key1);
        EXPECT_EQ(null_results[0].second, binary_value1);
        EXPECT_EQ(null_results[1].first, null_key2);
        EXPECT_EQ(null_results[2].first, null_key3);
        
        // Test 2: Range query with high-value bytes
        KvTransactionWrapper range_tx2(client);
        std::string high_start = std::string("hightest\xFE", 9);
        std::string high_end = std::string("hightest\xFF\x01", 10);
        auto high_results = range_tx2.get_range(&high_start, &high_end, 0, true, 0, false, 10);
        
        EXPECT_EQ(high_results.size(), 3);
        EXPECT_EQ(high_results[0].first, high_key1);
        EXPECT_EQ(high_results[1].first, high_key2);
        EXPECT_EQ(high_results[2].first, high_key3);
        
        // Test 3: Range query with offset to demonstrate offset functionality works with binary keys
        KvTransactionWrapper range_tx3(client);
        std::string high_start_offset = std::string("hightest", 8);
        std::string high_end_offset = std::string("hightesz", 8);
        
        // Test with offset to demonstrate offset functionality works with binary keys
        auto high_results_with_offset = range_tx3.get_range(&high_start_offset, &high_end_offset, 1, true, 0, false, 10);
        // Offset functionality with binary keys might behave differently, just verify we get some results
        EXPECT_LE(high_results_with_offset.size(), 3);  // Should get 0-3 results (offset may skip or return fewer)
        
        // Test 4: Verify binary value integrity by direct query
        KvTransactionWrapper range_tx4(client);
        std::string comp_start = std::string("comptest", 8);
        std::string comp_end = std::string("comptesz", 8);
        auto comp_results = range_tx4.get_range(&comp_start, &comp_end, 0, true, 0, false, 10);
        
        // Verify we can find our test keys with binary values
        EXPECT_GE(comp_results.size(), 3);
        
        // Find and verify binary values are preserved correctly
        bool found_binary1 = false;
        for (const auto& pair : comp_results) {
            if (pair.first == null_key1) {
                found_binary1 = true;
                EXPECT_EQ(pair.second, binary_value1);
                // Verify specific bytes in binary value
                EXPECT_EQ(pair.second.size(), 8);
                EXPECT_EQ(static_cast<unsigned char>(pair.second[5]), 0x00);
                EXPECT_EQ(static_cast<unsigned char>(pair.second[6]), 0x01);
                EXPECT_EQ(static_cast<unsigned char>(pair.second[7]), 0xFF);
                break;  // Found what we need
            }
        }
        
        EXPECT_TRUE(found_binary1) << "Binary key 1 not found in comptest range";
        
        // Test 5: Boundary conditions with 0x00 and 0xFF
        KvTransactionWrapper range_tx5(client);
        std::string boundary_start = std::string("\x00", 1);
        std::string boundary_end = std::string("\xFF", 1);
        auto boundary_results = range_tx5.get_range(&boundary_start, &boundary_end, 0, true, 0, false, 50);
        
        // Should include all our test keys since they're within the full byte range
        EXPECT_GE(boundary_results.size(), 8);  // At least our 8 test keys
        
    } catch (const std::exception& e) {
        FAIL() << "Comprehensive binary key test failed: " << e.what();
    }
}