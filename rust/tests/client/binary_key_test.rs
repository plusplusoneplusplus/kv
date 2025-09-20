use rocksdb_server::client::KvStoreClient;

use crate::common;

#[tokio::test]
async fn test_read_transaction_binary_keys_snapshot_get() -> Result<(), Box<dyn std::error::Error>>
{
    let client = KvStoreClient::connect(&common::get_server_address())?;

    // First, set up binary data with a regular transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;

    // Test binary key with diverse byte values (avoiding high bytes that might cause UTF-8 issues)
    let binary_key = b"\x00\x01\x7Fkey\x00\x0A\x0D\x1B\x20\x21\x22";
    let binary_value = b"\x00raw\x7Fbinary\x0A\x0D\x1B\x30\x31\x32\x00data\x40\x41";

    tx.set(binary_key, binary_value, None)?;

    let commit_future = tx.commit();
    commit_future.await_result().await?;

    // Test read transaction with snapshot_get
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future.await_result().await?;

    let snapshot_get_future = read_tx.snapshot_get(binary_key, None);
    let snapshot_value = snapshot_get_future.await_result().await?;

    // Verify binary value retrieval via snapshot_get
    assert!(snapshot_value.is_some());
    let retrieved_value = snapshot_value.unwrap();
    assert_eq!(retrieved_value, binary_value);

    println!(
        "✅ Binary key snapshot_get test passed! Key: {:?}, Value: {:?}",
        binary_key, binary_value
    );

    Ok(())
}

#[tokio::test]
async fn test_read_transaction_binary_keys_get() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect(&common::get_server_address())?;

    // First, set up binary data with a regular transaction
    let tx_future = client.begin_transaction(None, Some(30));
    let mut tx = tx_future.await_result().await?;

    // Test binary key with diverse byte values (avoiding high bytes that might cause UTF-8 issues)
    let binary_key = b"\x00\x02\x7Fkey2\x00\x0A\x0D\x1B\x20\x21\x22";
    let binary_value = b"\x00raw2\x7Fbinary\x0A\x0D\x1B\x30\x31\x32\x00data\x40\x41";

    tx.set(binary_key, binary_value, None)?;

    let commit_future = tx.commit();
    commit_future.await_result().await?;

    // Test regular transaction with get (can read committed data)
    let tx_future = client.begin_transaction(None, Some(30));
    let tx = tx_future.await_result().await?;

    let get_future = tx.get(binary_key, None);
    let get_value = get_future.await_result().await?;

    // Verify binary value retrieval via get
    assert!(get_value.is_some());
    let retrieved_value = get_value.unwrap();
    assert_eq!(retrieved_value, binary_value);

    println!(
        "✅ Binary key get test passed! Key: {:?}, Value: {:?}",
        binary_key, binary_value
    );

    Ok(())
}
