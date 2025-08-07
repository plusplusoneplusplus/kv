#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <filesystem>

#include "kvstore_service.h"

int main() {
    
    try {
        // Create data directory if it doesn't exist
        std::string db_path = "./data/rocksdb-cpp";
        std::filesystem::create_directories(db_path);
        
        // Create service
        auto service = std::make_unique<KvStoreService>(db_path);
        
        // Set up server address
        std::string server_address = "0.0.0.0:50051";
        
        // Run the async server
        service->Run(server_address);
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    std::cout << "Server shutdown complete" << std::endl;
    return 0;
}
