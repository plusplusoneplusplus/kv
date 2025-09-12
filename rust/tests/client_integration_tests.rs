// Client integration tests - requires running Thrift server on localhost:9090
// Run with: cargo test client_integration_tests
// Make sure to start thrift server first: cargo run --bin thrift-server

mod common;
mod client;

use rocksdb_server::client::KvStoreClient;

#[tokio::test]
async fn test_client_connectivity() -> Result<(), Box<dyn std::error::Error>> {
    // This test serves as an entry point to verify client connectivity
    println!("Testing client connectivity...");
    println!("Make sure the Thrift server is running on localhost:9090");
    
    // Simple connectivity test (KvStoreClient::connect is synchronous, not async)
    match KvStoreClient::connect("localhost:9090") {
        Ok(_) => {
            println!("✅ Successfully connected to Thrift server");
            Ok(())
        }
        Err(e) => {
            println!("❌ Failed to connect to Thrift server: {}", e);
            println!("💡 Start the server with: cargo run --bin thrift-server");
            Err(e.into())
        }
    }
}

// Include all the individual client tests by reference
// This allows them to be discovered and run by cargo test