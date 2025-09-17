#include <gtest/gtest.h>
#include "test_common.hpp"

int main(int argc, char **argv) {
    std::cout << "Running KV Store C/C++ FFI Tests with Google Test" << std::endl;
    std::cout << "=================================================" << std::endl;

    // Show current server configuration
    std::string server_addr = get_server_address();
    std::cout << "Server address: " << server_addr << std::endl;

    // Extract port from address for instructions
    size_t colon_pos = server_addr.find(':');
    std::string port = (colon_pos != std::string::npos) ? server_addr.substr(colon_pos + 1) : "9090";

    std::cout << "Note: Tests will connect to the server at " << server_addr << std::endl;
    std::cout << "Environment variables:" << std::endl;
    std::cout << "  KV_TEST_SERVER_ADDRESS: Full server address (e.g., localhost:9097)" << std::endl;
    std::cout << "  KV_TEST_SERVER_PORT: Port only (e.g., 9097)" << std::endl;
    std::cout << "Start the server with: ./target/debug/thrift-server --port " << port << std::endl;
    std::cout << std::endl;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}