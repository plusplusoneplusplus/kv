use kvstore_client::{KvStoreClient, KvError};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;
    
    // Set and get a value
    let set_future = tx.set("test_key", "test_value", None);
    set_future.await_result().await?;
    
    let get_future = tx.get("test_key", None);
    let value = get_future.await_result().await?;
    
    assert_eq!(value, Some("test_value".to_string()));
    
    // Commit
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_conflict() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Start two transactions
    let tx1_future = client.begin_transaction(None, Some(30));
    let tx1 = tx1_future.await_result().await?;
    
    let tx2_future = client.begin_transaction(None, Some(30));
    let tx2 = tx2_future.await_result().await?;
    
    let conflict_key = "conflict_test";
    
    // Both transactions modify the same key
    let set1_future = tx1.set(conflict_key, "value1", None);
    set1_future.await_result().await?;
    
    let set2_future = tx2.set(conflict_key, "value2", None);
    set2_future.await_result().await?;
    
    // First commit should succeed
    let commit1_future = tx1.commit();
    commit1_future.await_result().await?;
    
    // Second commit should fail with conflict
    let commit2_future = tx2.commit();
    let result = commit2_future.await_result().await;
    
    assert!(result.is_err());
    match result.err().unwrap() {
        KvError::TransactionConflict(_) => {
            // Expected conflict error
        }
        other => panic!("Expected transaction conflict, got: {:?}", other),
    }
    
    Ok(())
}

#[tokio::test]
async fn test_range_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;
    
    // Set multiple keys in a range
    for i in 1..=5 {
        let key = format!("range_test:{:03}", i);
        let value = format!("value_{}", i);
        let set_future = tx.set(&key, &value, None);
        set_future.await_result().await?;
    }
    
    // Get range
    let range_future = tx.get_range("range_test:", Some("range_test:z"), Some(10), None);
    let results = range_future.await_result().await?;
    
    assert_eq!(results.len(), 5);
    
    // Verify ordering and values
    for (i, (key, value)) in results.iter().enumerate() {
        let expected_key = format!("range_test:{:03}", i + 1);
        let expected_value = format!("value_{}", i + 1);
        assert_eq!(key, &expected_key);
        assert_eq!(value, &expected_value);
    }
    
    // Commit
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}

#[tokio::test]
async fn test_read_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // First, set up some data with a regular transaction
    let setup_tx_future = client.begin_transaction(None, Some(30));
    let setup_tx = setup_tx_future.await_result().await?;
    
    let set_future = setup_tx.set("read_test_key", "read_test_value", None);
    set_future.await_result().await?;
    
    let commit_future = setup_tx.commit();
    commit_future.await_result().await?;
    
    // Now test read transaction
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future.await_result().await?;
    
    let get_future = read_tx.snapshot_get("read_test_key", None);
    let value = get_future.await_result().await?;
    
    assert_eq!(value, Some("read_test_value".to_string()));
    
    Ok(())
}

#[tokio::test]
async fn test_read_transaction_binary_keys() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // First, set up binary data with a regular transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Test binary key with null bytes
    let binary_key = b"binary\x00key\x00with\x00nulls";
    let binary_value = b"binary\x00value\x00data\x00test";
    
    tx.set(binary_key, binary_value, None)?;
    
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    // Now test read transaction with binary key
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future.await_result().await?;
    
    let get_future = read_tx.snapshot_get(binary_key, None);
    let value = get_future.await_result().await?;
    
    // Verify binary value retrieval
    assert!(value.is_some());
    let retrieved_value = value.unwrap();
    assert_eq!(retrieved_value, binary_value);
    
    Ok(())
}

#[tokio::test]
async fn test_versionstamped_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;
    
    // Test versionstamped key
    let vs_key_future = tx.set_versionstamped_key("timestamp:", "event_data", None);
    let generated_key = vs_key_future.await_result().await?;
    
    assert!(generated_key.starts_with("timestamp:"));
    assert!(generated_key.len() > "timestamp:".len());
    
    // Commit
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}

#[tokio::test] 
async fn test_connection_timeout() -> Result<(), Box<dyn std::error::Error>> {
    // Test connection to non-existent server
    let result = KvStoreClient::connect("localhost:19999");
    assert!(result.is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_transaction_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction with very short timeout
    let tx_future = client.begin_transaction(None, Some(1));
    let tx = tx_future.await_result().await?;
    
    // Wait longer than timeout
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Try to use transaction after timeout - should fail
    let get_future = tx.get("test_key", None);
    let result = timeout(Duration::from_secs(5), get_future.await_result()).await;
    
    // The operation should either timeout or return a transaction error
    assert!(result.is_err() || result.unwrap().is_err());
    
    Ok(())
}

#[tokio::test]
async fn test_ping() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    let ping_future = client.ping(Some("test message".to_string()));
    let response = ping_future.await_result().await?;
    
    assert!(response.contains("test message") || response == "pong");
    
    Ok(())
}

#[tokio::test]
async fn test_delete_operation() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;
    
    // Set a value
    let set_future = tx.set("delete_test_key", "delete_test_value", None);
    set_future.await_result().await?;
    
    // Verify it exists
    let get_future1 = tx.get("delete_test_key", None);
    let value1 = get_future1.await_result().await?;
    assert_eq!(value1, Some("delete_test_value".to_string()));
    
    // Delete it
    let delete_future = tx.delete("delete_test_key", None);
    delete_future.await_result().await?;
    
    // Verify it's gone
    let get_future2 = tx.get("delete_test_key", None);
    let value2 = get_future2.await_result().await?;
    assert_eq!(value2, None);
    
    // Commit
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}