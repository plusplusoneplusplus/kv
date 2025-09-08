use kvstore_client::{KvStoreClient, KvError};

#[tokio::test]
async fn test_read_transaction_binary_keys() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // First, set up binary data with a regular transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;
    
    // Test binary key with diverse byte values (avoiding high bytes that might cause UTF-8 issues)
    let binary_key = b"\x00\x01\x7Fkey\x00\x0A\x0D\x1B\x20\x21\x22";
    let binary_value = b"\x00raw\x7Fbinary\x0A\x0D\x1B\x30\x31\x32\x00data\x40\x41";
    
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
    
    println!("✅ Binary key test passed! Key: {:?}, Value: {:?}", binary_key, binary_value);
    
    Ok(())
}