#include <gtest/gtest.h>
#include "test_common.hpp"

int main(int argc, char **argv) {
    std::cout << "Running KV Store C/C++ FFI Tests with Google Test" << std::endl;
    std::cout << "=================================================" << std::endl;
    std::cout << "Note: Some tests may fail if the KV server is not running on localhost:9090" << std::endl;
    std::cout << "Start the server with: ./target/debug/thrift-server --verbose --port 9090" << std::endl;
    std::cout << std::endl;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}