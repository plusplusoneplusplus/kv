use rocksdb_server::client::{KvStoreClient, KvError, CommitResult};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_versionstamped_key_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Set a versionstamped key
    tx.set_versionstamped_key(b"user_score_", b"100", None)?;
    
    // Commit with results to get generated keys
    let commit_future = tx.commit_with_results();
    let commit_result = commit_future.await_result().await?;
    
    // Verify we got a generated key
    assert_eq!(commit_result.generated_keys.len(), 1, "Should have one generated key");
    assert_eq!(commit_result.generated_values.len(), 0, "Should have no generated values for key operation");
    
    let generated_key = &commit_result.generated_keys[0];
    assert!(generated_key.starts_with(b"user_score_"), "Generated key should start with prefix");
    assert_eq!(generated_key.len(), b"user_score_".len() + 10, "Generated key should be prefix + 10 bytes");
    
    // Verify the key was actually stored by reading it back
    let read_tx_future = client.begin_transaction(None, Some(30));
    let read_tx = read_tx_future.await_result().await?;
    
    let get_future = read_tx.get(generated_key, None);
    let value = get_future.await_result().await?;
    
    assert_eq!(value, Some(b"100".to_vec()), "Should be able to read back the versionstamped key");
    
    Ok(())
}

#[tokio::test]
async fn test_versionstamped_value_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Set a versionstamped value
    tx.set_versionstamped_value(b"user_session", b"session_", None)?;
    
    // Commit with results to get generated values
    let commit_future = tx.commit_with_results();
    let commit_result = commit_future.await_result().await?;
    
    // Verify we got a generated value
    assert_eq!(commit_result.generated_keys.len(), 0, "Should have no generated keys for value operation");
    assert_eq!(commit_result.generated_values.len(), 1, "Should have one generated value");
    
    let generated_value = &commit_result.generated_values[0];
    assert!(generated_value.starts_with(b"session_"), "Generated value should start with prefix");
    assert_eq!(generated_value.len(), b"session_".len() + 10, "Generated value should be prefix + 10 bytes");
    
    // Verify the value was actually stored by reading it back
    let read_tx_future = client.begin_transaction(None, Some(30));
    let read_tx = read_tx_future.await_result().await?;
    
    let get_future = read_tx.get(b"user_session", None);
    let value = get_future.await_result().await?;
    
    assert_eq!(value, Some(generated_value.clone()), "Should be able to read back the versionstamped value");
    
    Ok(())
}

#[tokio::test]
async fn test_mixed_versionstamped_operations() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Set both versionstamped key and value, plus regular operation
    tx.set_versionstamped_key(b"log_entry_", b"event_data", None)?;
    tx.set_versionstamped_value(b"event_value", b"data_", None)?;
    tx.set(b"regular_key", b"regular_value", None)?;
    
    // Commit with results
    let commit_future = tx.commit_with_results();
    let commit_result = commit_future.await_result().await?;
    
    // Verify we got both types of generated data
    assert_eq!(commit_result.generated_keys.len(), 1, "Should have one generated key");
    assert_eq!(commit_result.generated_values.len(), 1, "Should have one generated value");
    
    let generated_key = &commit_result.generated_keys[0];
    let generated_value = &commit_result.generated_values[0];
    
    // Verify formats
    assert!(generated_key.starts_with(b"log_entry_"), "Generated key should start with prefix");
    assert!(generated_value.starts_with(b"data_"), "Generated value should start with prefix");
    
    // Verify all data was stored correctly
    let read_tx_future = client.begin_transaction(None, Some(30));
    let read_tx = read_tx_future.await_result().await?;
    
    // Check versionstamped key
    let get_key_future = read_tx.get(generated_key, None);
    let key_value = get_key_future.await_result().await?;
    assert_eq!(key_value, Some(b"event_data".to_vec()));
    
    // Check versionstamped value
    let get_value_future = read_tx.get(b"event_value", None);
    let stored_value = get_value_future.await_result().await?;
    assert_eq!(stored_value, Some(generated_value.clone()));
    
    // Check regular key
    let get_regular_future = read_tx.get(b"regular_key", None);
    let regular_value = get_regular_future.await_result().await?;
    assert_eq!(regular_value, Some(b"regular_value".to_vec()));
    
    Ok(())
}

#[tokio::test]
async fn test_versionstamp_uniqueness() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // First transaction
    let tx1_future = client.begin_transaction(None, Some(30));
    let mut tx1 = tx1_future.await_result().await?;
    tx1.set_versionstamped_key(b"test_", b"value1", None)?;
    let commit1_future = tx1.commit_with_results();
    let result1 = commit1_future.await_result().await?;
    
    // Second transaction
    let tx2_future = client.begin_transaction(None, Some(30));
    let mut tx2 = tx2_future.await_result().await?;
    tx2.set_versionstamped_key(b"test_", b"value2", None)?;
    let commit2_future = tx2.commit_with_results();
    let result2 = commit2_future.await_result().await?;
    
    // Keys should be different (different versions)
    let key1 = &result1.generated_keys[0];
    let key2 = &result2.generated_keys[0];
    assert_ne!(key1, key2, "Keys from different transactions should be unique");
    
    // Both should have the same prefix
    assert!(key1.starts_with(b"test_"));
    assert!(key2.starts_with(b"test_"));
    
    Ok(())
}

#[tokio::test] 
async fn test_backward_compatibility_commit() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Set a versionstamped key
    tx.set_versionstamped_key(b"compat_test_", b"data", None)?;
    
    // Use the old commit method (should still work)
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    // We can't verify the generated key with the old commit method,
    // but it should not fail
    Ok(())
}