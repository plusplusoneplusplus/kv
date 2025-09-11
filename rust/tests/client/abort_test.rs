use rocksdb_server::client::KvStoreClient;

#[tokio::test]
async fn test_transaction_abort() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // First, let's check if the key exists before we start
    let pre_tx_future = client.begin_transaction(None, Some(30));
    let pre_tx = pre_tx_future.await_result().await?;
    let pre_get_future = pre_tx.get(b"abort_test_key", None);
    let pre_value = pre_get_future.await_result().await?;
    println!("Value before test: {:?}", pre_value);
    
    // If key exists, delete it first
    if pre_value.is_some() {
        let mut pre_tx = pre_tx; // Make mutable for delete
        pre_tx.delete(b"abort_test_key", None)?; // delete returns KvResult<()>
        let pre_commit_future = pre_tx.commit();
        pre_commit_future.await_result().await?;
        println!("Cleaned up existing key");
    } else {
        let pre_abort_future = pre_tx.abort();
        pre_abort_future.await_result().await?;
        println!("No cleanup needed");
    }
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;
    println!("Started transaction");
    
    // Set a value in the transaction
    let mut tx = tx; // Make mutable for set
    tx.set(b"abort_test_key", b"abort_test_value", None)?; // set returns KvResult<()>
    println!("Set value in transaction");
    
    // Verify we can read it within the transaction
    let in_tx_get_future = tx.get(b"abort_test_key", None);
    let in_tx_value = in_tx_get_future.await_result().await?;
    println!("Value within transaction: {:?}", in_tx_value);
    
    // Abort the transaction
    let abort_future = tx.abort();
    abort_future.await_result().await?;
    println!("Aborted transaction");
    
    // Start a new transaction to verify the key doesn't exist
    let verify_tx_future = client.begin_transaction(None, Some(30));
    let verify_tx = verify_tx_future.await_result().await?;
    
    let get_future = verify_tx.get(b"abort_test_key", None);
    let value = get_future.await_result().await?;
    println!("Value after abort: {:?}", value);
    
    // Commit the verify transaction
    let commit_future = verify_tx.commit();
    commit_future.await_result().await?;
    
    // Key should not exist after abort
    assert_eq!(value, None, "Key should not exist after abort");
    
    println!("Transaction abort test passed!");
    Ok(())
}