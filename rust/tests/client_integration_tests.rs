// Client integration tests - requires running shard server on localhost:9090
// Run with: cargo test client_integration_tests
// Make sure to start shard server first: cargo run --bin shard-server
// Or use the test configuration to get the binary path

mod client;
mod common;

use rocksdb_server::client::KvStoreClient;

#[tokio::test]
async fn test_client_connectivity() -> Result<(), Box<dyn std::error::Error>> {
    // This test serves as an entry point to verify client connectivity
    let server_addr = common::get_server_address();
    println!("Testing client connectivity...");
    println!("Make sure the shard server is running on {}", server_addr);

    // Simple connectivity test (KvStoreClient::connect is synchronous, not async)
    match KvStoreClient::connect(&server_addr) {
        Ok(_) => {
            println!("✅ Successfully connected to shard server");
            Ok(())
        }
        Err(e) => {
            println!("❌ Failed to connect to shard server: {}", e);
            println!("💡 Start the server with: cargo run --bin shard-server");
            println!("💡 Or run the configured binary at: {}",
                common::test_config::shard_server_binary().display());
            Err(e.into())
        }
    }
}

// Include all the individual client tests by reference
// This allows them to be discovered and run by cargo test
