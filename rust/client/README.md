# KV Store Client SDK

A high-performance Rust client SDK for the transactional KV store with C FFI bindings, designed similar to FoundationDB's client API.

## Features

- **Async/Await Support**: Full async support using Tokio runtime
- **Transaction Management**: Complete ACID transaction support with conflict detection
- **C FFI Bindings**: C-compatible API for integration with other languages
- **Connection Pooling**: Built-in connection pooling for high-throughput applications
- **Range Operations**: Efficient range queries with key iteration
- **Versionstamped Operations**: Support for versionstamped keys and values
- **Snapshot Reads**: Read-only transactions with consistent snapshots
- **Error Handling**: Comprehensive error types with detailed error codes

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kvstore-client = { path = "path/to/kvstore-client" }
```

## Rust API Usage

### Basic Operations

```rust
use kvstore_client::{KvStoreClient, KvResult};

#[tokio::main]
async fn main() -> KvResult<()> {
    // Connect to server
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(60));
    let tx = tx_future.await_result().await?;
    
    // Set key-value pairs
    let set_future = tx.set("user:1001", "Alice", None);
    set_future.await_result().await?;
    
    // Get values
    let get_future = tx.get("user:1001", None);
    if let Some(value) = get_future.await_result().await? {
        println!("Value: {}", value);
    }
    
    // Commit transaction
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}
```

### Range Operations

```rust
// Get range of keys
let range_future = tx.get_range("user:", Some("user:z"), Some(100), None);
let results = range_future.await_result().await?;

for (key, value) in results {
    println!("{} = {}", key, value);
}
```

### Read Transactions

```rust
// Begin read-only transaction
let read_tx_future = client.begin_read_transaction(None);
let read_tx = read_tx_future.await_result().await?;

let get_future = read_tx.snapshot_get("user:1001", None);
if let Some(value) = get_future.await_result().await? {
    println!("Snapshot value: {}", value);
}
```

### Connection Pooling

```rust
// Create connection pool
let pool = KvStoreClient::connect_with_pool("localhost:9090", 4)?;

// Use pooled connections
let tx_future = pool.begin_transaction(None, Some(60));
let tx = tx_future.await_result().await?;
// ... use transaction
```

## C API Usage

The C API provides a FoundationDB-style interface with opaque handles and future-based operations. See `include/kvstore_client.h` for complete API documentation.

## Building

### Rust Library

```bash
# Build library
cargo build --release

# Run tests (requires running Thrift server)
cargo test
```

### C Shared Library

```bash
# Build C-compatible shared library
cargo build --release --features ffi

# The shared library will be at target/release/libkvstore_client.so (Linux)
# or target/release/libkvstore_client.dylib (macOS)
# or target/release/kvstore_client.dll (Windows)
```


## Error Handling

### Rust Errors

The library uses the `KvError` enum for all error conditions:

- `TransportError`: Network/transport issues
- `ProtocolError`: Protocol-level errors
- `TransactionNotFound`: Transaction doesn't exist or expired
- `TransactionConflict`: Transaction conflict during commit
- `TransactionTimeout`: Transaction timed out
- `InvalidKey`/`InvalidValue`: Invalid input parameters
- `NetworkError`: Network connectivity issues
- `ServerError`: Server-side errors
- `Unknown`: Unexpected errors

### C Error Codes

C API uses numeric error codes defined in `KvErrorCode` enum:

- `KV_SUCCESS` (0): Operation successful
- `KV_ERROR_TRANSPORT` (1000): Transport error
- `KV_ERROR_PROTOCOL` (1001): Protocol error
- `KV_ERROR_TRANSACTION_CONFLICT` (2001): Transaction conflict
- And more...

## Performance Considerations

1. **Connection Pooling**: Use `ClientPool` for high-throughput applications
2. **Batch Operations**: Group multiple operations in a single transaction
3. **Read Transactions**: Use read-only transactions for snapshot consistency
4. **Async Operations**: Use async API in Rust for better concurrency
5. **C API Polling**: In C, poll futures efficiently to avoid blocking

## Thread Safety

- Rust API: All types are `Send + Sync` where appropriate
- C API: Thread-safe when using separate client handles per thread
- Connection pooling automatically distributes load across connections

## Requirements

- Rust 1.70+
- Tokio runtime for async operations
- Running Thrift server on the target address
- For C API: C compiler with C99 support

## Testing

```bash
# Start the Thrift server first
cd ../.. && ./bin/rocksdbserver-thrift

# Run integration tests
cargo test
```