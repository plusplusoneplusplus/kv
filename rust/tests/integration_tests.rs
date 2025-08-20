use std::time::Duration;
use tokio::time::timeout;

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TIoChannel, TTcpChannel};

// Import the generated Thrift types and client from the current crate
use rocksdb_server::lib::kvstore::*;

mod common;

use common::ThriftTestServer;

#[tokio::test]
async fn test_transactional_lifecycle_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    // Give server time to start up
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        // Create client connection
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Test transaction lifecycle
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success, "Begin transaction should succeed");
        let transaction_id = begin_resp.transaction_id;
        
        // Test transactional set
        let set_req = SetRequest::new(transaction_id.clone(), "test_key".to_string(), "test_value".to_string(), None::<String>);
        let set_resp = client.set_key(set_req).expect("Failed to set key");
        assert!(set_resp.success, "Set should succeed: {:?}", set_resp.error);
        
        // Test transactional get
        let get_req = GetRequest::new(transaction_id.clone(), "test_key".to_string(), None::<String>);
        let get_resp = client.get(get_req).expect("Failed to get key");
        assert!(get_resp.found, "Key should be found");
        assert_eq!(get_resp.value, "test_value", "Value should match");
        
        // Test commit
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_transactional_operations_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Begin transaction
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success);
        let transaction_id = begin_resp.transaction_id;
        
        // Test multiple operations
        let keys_values = vec![
            ("key1", "value1"),
            ("key2", "value2"),
            ("key3", "value3"),
        ];
        
        // Set multiple keys
        for (key, value) in &keys_values {
            let set_req = SetRequest::new(transaction_id.clone(), key.to_string(), value.to_string(), None::<String>);
            let set_resp = client.set_key(set_req).expect("Failed to set key");
            assert!(set_resp.success, "Set should succeed for key {}: {:?}", key, set_resp.error);
        }
        
        // Get all keys
        for (key, expected_value) in &keys_values {
            let get_req = GetRequest::new(transaction_id.clone(), key.to_string(), None::<String>);
            let get_resp = client.get(get_req).expect("Failed to get key");
            assert!(get_resp.found, "Key {} should be found", key);
            assert_eq!(get_resp.value, *expected_value, "Value should match for key {}", key);
        }
        
        // Test range query
        let range_req = GetRangeRequest::new(transaction_id.clone(), "key".to_string(), None::<String>, Some(10i32), None::<String>);
        let range_resp = client.get_range(range_req).expect("Failed to get range");
        assert!(range_resp.success, "Range query should succeed: {:?}", range_resp.error);
        assert_eq!(range_resp.key_values.len(), 3, "Should find 3 keys");
        
        // Test delete
        let delete_req = DeleteRequest::new(transaction_id.clone(), "key2".to_string(), None::<String>);
        let delete_resp = client.delete_key(delete_req).expect("Failed to delete key");
        assert!(delete_resp.success, "Delete should succeed: {:?}", delete_resp.error);
        
        // Verify key is deleted
        let get_req = GetRequest::new(transaction_id.clone(), "key2".to_string(), None::<String>);
        let get_resp = client.get(get_req).expect("Failed to get deleted key");
        assert!(!get_resp.found, "Deleted key should not be found");
        
        // Commit transaction
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_fault_injection_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Enable fault injection for timeout on set operations
        let fault_req = FaultInjectionRequest::new(
            "timeout".to_string(),
            Some(1.0.into()), // 100% probability
            Some(50i32),      // 50ms delay
            Some("set".to_string())
        );
        let fault_resp = client.set_fault_injection(fault_req).expect("Failed to set fault injection");
        assert!(fault_resp.success, "Fault injection should succeed: {:?}", fault_resp.error);
        
        // Begin transaction
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success);
        let transaction_id = begin_resp.transaction_id;
        
        // This set operation should fail due to fault injection
        let set_req = SetRequest::new(transaction_id.clone(), "test_key".to_string(), "test_value".to_string(), None::<String>);
        let set_resp = client.set_key(set_req).expect("Failed to call set key (but should be injected fault)");
        assert!(!set_resp.success, "Set should fail due to fault injection");
        assert!(set_resp.error_code == Some("TIMEOUT".to_string()), "Should have timeout error code");
        
        // Disable fault injection
        let disable_fault_req = FaultInjectionRequest::new(
            "timeout".to_string(),
            Some(0.0.into()), // 0% probability
            Some(0i32),
            None::<String>
        );
        let disable_fault_resp = client.set_fault_injection(disable_fault_req).expect("Failed to disable fault injection");
        assert!(disable_fault_resp.success, "Disabling fault injection should succeed");
        
        // Now set should work
        let set_req2 = SetRequest::new(transaction_id.clone(), "test_key".to_string(), "test_value".to_string(), None::<String>);
        let set_resp2 = client.set_key(set_req2).expect("Failed to set key");
        assert!(set_resp2.success, "Set should succeed after disabling fault injection: {:?}", set_resp2.error);
        
        // Commit transaction
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_conflict_detection_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client1 = create_thrift_client(port).expect("Failed to create client 1");
        let mut client2 = create_thrift_client(port).expect("Failed to create client 2");
        
        // Begin two transactions
        let begin_req1 = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp1 = client1.begin_transaction(begin_req1).expect("Failed to begin transaction 1");
        assert!(begin_resp1.success);
        let transaction_id1 = begin_resp1.transaction_id;
        
        let begin_req2 = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp2 = client2.begin_transaction(begin_req2).expect("Failed to begin transaction 2");
        assert!(begin_resp2.success);
        let transaction_id2 = begin_resp2.transaction_id;
        
        // Add read conflicts on the same key for both transactions
        let conflict_req1 = AddReadConflictRequest::new(transaction_id1.clone(), "conflict_key".to_string(), None::<String>);
        let conflict_resp1 = client1.add_read_conflict(conflict_req1).expect("Failed to add read conflict 1");
        assert!(conflict_resp1.success, "Adding read conflict should succeed");
        
        let conflict_req2 = AddReadConflictRequest::new(transaction_id2.clone(), "conflict_key".to_string(), None::<String>);
        let conflict_resp2 = client2.add_read_conflict(conflict_req2).expect("Failed to add read conflict 2");
        assert!(conflict_resp2.success, "Adding read conflict should succeed");
        
        // Set conflicting values in both transactions
        let set_req1 = SetRequest::new(transaction_id1.clone(), "conflict_key".to_string(), "value1".to_string(), None::<String>);
        let set_resp1 = client1.set_key(set_req1).expect("Failed to set key in transaction 1");
        assert!(set_resp1.success, "Set should succeed in transaction 1");
        
        let set_req2 = SetRequest::new(transaction_id2.clone(), "conflict_key".to_string(), "value2".to_string(), None::<String>);
        let set_resp2 = client2.set_key(set_req2).expect("Failed to set key in transaction 2");
        assert!(set_resp2.success, "Set should succeed in transaction 2");
        
        // Try to commit both - one should succeed, the other might retry or fail with conflict
        let commit_req1 = CommitTransactionRequest::new(transaction_id1.clone());
        let commit_resp1 = client1.commit_transaction(commit_req1).expect("Failed to commit transaction 1");
        
        let commit_req2 = CommitTransactionRequest::new(transaction_id2.clone());
        let commit_resp2 = client2.commit_transaction(commit_req2).expect("Failed to commit transaction 2");
        
        // At least one should succeed (retry logic should handle conflicts)
        assert!(commit_resp1.success || commit_resp2.success,
               "At least one transaction should succeed. T1: {:?}, T2: {:?}",
               (commit_resp1.success, &commit_resp1.error), (commit_resp2.success, &commit_resp2.error));
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_version_management_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Begin transaction
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success);
        let transaction_id = begin_resp.transaction_id;
        
        // Test version management
        let set_version_req = SetReadVersionRequest::new(transaction_id.clone(), 12345i64);
        let set_version_resp = client.set_read_version(set_version_req).expect("Failed to set read version");
        assert!(set_version_resp.success, "Set read version should succeed: {:?}", set_version_resp.error);
        
        let get_version_req = GetCommittedVersionRequest::new(transaction_id.clone());
        let get_version_resp = client.get_committed_version(get_version_req).expect("Failed to get committed version");
        assert!(get_version_resp.success, "Get committed version should succeed: {:?}", get_version_resp.error);
        assert!(get_version_resp.version > 0, "Version should be positive");
        
        // Test snapshot operations
        let snapshot_get_req = SnapshotGetRequest::new(transaction_id.clone(), "snapshot_key".to_string(), 123456i64, None::<String>);
        let snapshot_get_resp = client.snapshot_get(snapshot_get_req).expect("Failed to snapshot get");
        // Key doesn't exist, so should not be found but operation should succeed
        assert!(!snapshot_get_resp.found, "Non-existent key should not be found in snapshot");
        
        // Commit transaction
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_versionstamped_operations_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Begin transaction
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success);
        let transaction_id = begin_resp.transaction_id;
        
        // Test versionstamped key
        let vs_key_req = SetVersionstampedKeyRequest::new(transaction_id.clone(), "prefix_".to_string(), "test_value".to_string(), None::<String>);
        let vs_key_resp = client.set_versionstamped_key(vs_key_req).expect("Failed to set versionstamped key");
        assert!(vs_key_resp.success, "Versionstamped key should succeed: {:?}", vs_key_resp.error);
        assert!(vs_key_resp.generated_key.starts_with("prefix_"), "Generated key should start with prefix");
        assert!(vs_key_resp.generated_key.len() > "prefix_".len(), "Generated key should be longer than prefix");
        
        // Test versionstamped value
        let vs_value_req = SetVersionstampedValueRequest::new(transaction_id.clone(), "test_key".to_string(), "value_prefix_".to_string(), None::<String>);
        let vs_value_resp = client.set_versionstamped_value(vs_value_req).expect("Failed to set versionstamped value");
        assert!(vs_value_resp.success, "Versionstamped value should succeed: {:?}", vs_value_resp.error);
        assert!(vs_value_resp.generated_value.starts_with("value_prefix_"), "Generated value should start with prefix");
        assert!(vs_value_resp.generated_value.len() > "value_prefix_".len(), "Generated value should be longer than prefix");
        
        // Verify the versionstamped key was set
        let get_req = GetRequest::new(transaction_id.clone(), vs_key_resp.generated_key, None::<String>);
        let get_resp = client.get(get_req).expect("Failed to get versionstamped key");
        assert!(get_resp.found, "Versionstamped key should exist");
        assert_eq!(get_resp.value, "test_value", "Value should match");
        
        // Verify the versionstamped value was set
        let get_value_req = GetRequest::new(transaction_id.clone(), "test_key".to_string(), None::<String>);
        let get_value_resp = client.get(get_value_req).expect("Failed to get key with versionstamped value");
        assert!(get_value_resp.found, "Key with versionstamped value should exist");
        assert_eq!(get_value_resp.value, vs_value_resp.generated_value, "Value should match generated versionstamped value");
        
        // Commit transaction
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_error_handling_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Test operations with invalid transaction ID
        let invalid_txn_id = "invalid-transaction-id".to_string();
        
        let set_req = SetRequest::new(invalid_txn_id.clone(), "test_key".to_string(), "test_value".to_string(), None::<String>);
        let set_resp = client.set_key(set_req).expect("Failed to call set key");
        assert!(!set_resp.success, "Set with invalid transaction ID should fail");
        assert_eq!(set_resp.error_code, Some("NOT_FOUND".to_string()), "Should have NOT_FOUND error code");
        
        // Begin valid transaction
        let begin_req = BeginTransactionRequest::new(None::<Vec<String>>, Some(60i64));
        let begin_resp = client.begin_transaction(begin_req).expect("Failed to begin transaction");
        assert!(begin_resp.success);
        let transaction_id = begin_resp.transaction_id;
        
        // Test empty key error
        let empty_key_req = SetRequest::new(transaction_id.clone(), "".to_string(), "test_value".to_string(), None::<String>);
        let empty_key_resp = client.set_key(empty_key_req).expect("Failed to call set key with empty key");
        assert!(!empty_key_resp.success, "Set with empty key should fail");
        assert_eq!(empty_key_resp.error_code, Some("INVALID_KEY".to_string()), "Should have INVALID_KEY error code");
        
        // Test invalid column family
        let invalid_cf_req = SetRequest::new(transaction_id.clone(), "test_key".to_string(), "test_value".to_string(), Some("nonexistent_cf".to_string()));
        let invalid_cf_resp = client.set_key(invalid_cf_req).expect("Failed to call set key with invalid CF");
        assert!(!invalid_cf_resp.success, "Set with invalid CF should fail");
        assert_eq!(invalid_cf_resp.error_code, Some("INVALID_CF".to_string()), "Should have INVALID_CF error code");
        
        // Test getting non-existent key (should succeed but not found)
        let get_missing_req = GetRequest::new(transaction_id.clone(), "missing_key".to_string(), None::<String>);
        let get_missing_resp = client.get(get_missing_req).expect("Failed to get missing key");
        assert!(!get_missing_resp.found, "Missing key should not be found");
        assert!(get_missing_resp.error.is_none() || get_missing_resp.error == Some("".to_string()), "Should not have error for missing key");
        
        // Commit transaction
        let commit_req = CommitTransactionRequest::new(transaction_id.clone());
        let commit_resp = client.commit_transaction(commit_req).expect("Failed to commit transaction");
        assert!(commit_resp.success, "Commit should succeed: {:?}", commit_resp.error);
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

#[tokio::test]
async fn test_ping_integration() {
    let mut server = ThriftTestServer::new().await;
    let port = server.start().await.expect("Failed to start server");
    
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    let result = timeout(Duration::from_secs(30), async {
        let mut client = create_thrift_client(port).expect("Failed to create client");
        
        // Test ping
        let ping_req = PingRequest::new(Some("hello".to_string()), None::<i64>);
        let ping_resp = client.ping(ping_req).expect("Failed to ping");
        assert_eq!(ping_resp.message, "hello", "Ping message should echo");
        assert!(ping_resp.server_timestamp > 0, "Server timestamp should be positive");
        assert!(ping_resp.timestamp <= ping_resp.server_timestamp, "Timestamp should be reasonable");
        
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }).await;
    
    assert!(result.is_ok(), "Test timed out or failed: {:?}", result.err());
    server.stop().await;
}

fn create_thrift_client(port: u16) -> thrift::Result<TransactionalKVSyncClient<TBinaryInputProtocol<thrift::transport::ReadHalf<TTcpChannel>>, TBinaryOutputProtocol<thrift::transport::WriteHalf<TTcpChannel>>>> {
    let mut channel = TTcpChannel::new();
    channel.open(&format!("127.0.0.1:{}", port))?;
    
    let (input_channel, output_channel) = channel.split()?;
    let input_protocol = TBinaryInputProtocol::new(input_channel, true);
    let output_protocol = TBinaryOutputProtocol::new(output_channel, true);
    
    Ok(TransactionalKVSyncClient::new(input_protocol, output_protocol))
}