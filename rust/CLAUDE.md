# CLAUDE.md - Rust KV Store Implementation

This file provides guidance to Claude Code when working with the Rust implementation of the high-performance key-value store service.

## Project Overview

This is the Rust implementation of a transactional key-value store service supporting both gRPC and Thrift protocols, using RocksDB as the storage engine. The Rust implementation emphasizes async performance using Tokio and provides a comprehensive client SDK with C FFI bindings.

## Architecture

### Core Components
- **Main Server Library** (`src/lib.rs`): Library exports and common types
- **Service Implementations**:
  - `src/servers/grpc_server.rs`: gRPC server binary using Tokio async runtime
  - `src/servers/thrift_server.rs`: Thrift server binary with async support
- **Core Libraries** (`src/lib/`):
  - `service.rs`: gRPC service implementation with TransactionalKvDatabase
  - `db.rs`: RocksDB wrapper with transaction support
  - `config.rs`: Configuration management with TOML support
  - `proto.rs`: Generated protobuf types and service definitions
  - `kvstore.rs`: Core KV store logic and types
- **Client SDK** (`client/`): High-performance async client with C FFI bindings
- **Testing** (`tests/`): Integration tests with common utilities

### Data Storage
- gRPC server database: `./data/rocksdb-rust/`
- Thrift server database: `./data/rocksdb-thrift/`

## Development Commands

### Building
```bash
# Build all binaries (debug)
cargo build

# Build all binaries (release)
cargo build --release

# Build specific server
cargo build --bin server          # gRPC server
cargo build --bin thrift-server   # Thrift server

# Build client SDK
cd client && cargo build --release

# Build with C FFI support
cd client && cargo build --release --features ffi
```

### Running Servers
```bash
# gRPC server (port 50051)
cargo run --bin server
# or from bin directory:
./bin/rocksdbserver-rust

# Thrift server (port 9090)
cargo run --bin thrift-server
# or from bin directory:
./bin/rocksdbserver-thrift
```

### Protocol Code Generation
```bash
# Generate protobuf files (automatic during build via build.rs)
cargo build

# From project root for all languages:
make proto thrift
```

### Testing
```bash
# Unit tests
cargo test

# Integration tests (requires running server)
cargo test --test integration_tests

# Client SDK tests (requires Thrift server)
cd client && cargo test

# C FFI tests
cd client && ./scripts/build_and_run_ffi_tests.sh
```

## Configuration

### Database Configuration
The server loads RocksDB configuration from TOML files:
- Default location: `bin/db_config.toml` (relative to executable)
- Fallback: Uses default RocksDB settings if config file not found
- Configuration covers cache sizes, bloom filters, compaction settings, etc.

### Server Configuration
Both servers support:
- Configurable database paths
- Async runtime with Tokio
- Structured logging with tracing
- Graceful shutdown handling

## Client SDK

### Features
- **Async/Await Support**: Full Tokio async runtime integration
- **Transaction Management**: ACID transactions with conflict detection
- **C FFI Bindings**: C-compatible API for cross-language integration
- **Connection Pooling**: Built-in pooling for high-throughput applications
- **Range Operations**: Efficient key iteration and range queries
- **Error Handling**: Comprehensive error types with detailed codes
- **Debug Mode**: Comprehensive logging for troubleshooting and performance analysis

### Usage Examples

**Basic Rust Client:**
```rust
use kvstore_client::{KvStoreClient, KvResult};

#[tokio::main]
async fn main() -> KvResult<()> {
    let client = KvStoreClient::connect("localhost:9090")?;
    let tx_future = client.begin_transaction(None, Some(60));
    let tx = tx_future.await_result().await?;
    
    let set_future = tx.set("key", "value", None);
    set_future.await_result().await?;
    
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    Ok(())
}
```

**Debug-Enabled Client:**
```rust
use kvstore_client::{KvStoreClient, ClientConfig, KvResult};

#[tokio::main]
async fn main() -> KvResult<()> {
    let config = ClientConfig::with_debug()
        .with_connection_timeout(30)
        .with_request_timeout(10);
    
    let client = KvStoreClient::connect_with_config("localhost:9090", config)?;
    // All operations will now generate detailed debug logs
    let tx_future = client.begin_transaction(None, Some(60));
    // ... debug output shows connection, transaction, and timing info
    Ok(())
}
```

**C FFI Integration:**
```c
#include "kvstore_client.h"

KvStoreClient* client = kv_client_connect("localhost:9090");
KvTransactionFuture* tx_future = kv_client_begin_transaction(client, NULL, 60);
KvTransaction* tx = kv_future_get_transaction(tx_future);
// ... use transaction
```

## Development Workflow

### Adding New Service Methods
1. Update protocol definitions in project root:
   - `proto/kvstore.proto` for gRPC
   - `thrift/kvstore.thrift` for Thrift
2. Run `make proto thrift` to regenerate all protocol files
3. Implement methods:
   - gRPC: Add to `KvStoreGrpcService` impl in `src/lib/service.rs`
   - Thrift: Add to `ThriftKvStoreService` impl in `src/servers/thrift_server.rs`
4. Update client SDK in `client/src/` if needed
5. Add tests to verify functionality

### Database Operations
- All database operations go through `TransactionalKvDatabase` in `src/lib/db.rs`
- Supports async operations with proper error handling
- Automatic transaction management and rollback on errors
- Configurable RocksDB options via TOML files

### Error Handling
The codebase uses structured error handling:
- Database errors are wrapped and propagated appropriately
- gRPC/Thrift protocol errors are handled at service boundaries
- Client SDK provides comprehensive error types (`KvError` enum)
- C FFI uses numeric error codes for cross-language compatibility

## Performance Characteristics

### Async Runtime
- Built on Tokio async runtime for high concurrency
- Non-blocking I/O throughout the stack
- Efficient connection pooling in client SDK
- Configurable connection limits and timeouts

### Database Performance
- RocksDB storage engine with configurable options
- Support for different cache and compaction strategies via TOML config
- Efficient prefix-based iteration for range operations
- Automatic database creation and directory management

### Memory Management
- Zero-copy operations where possible
- Efficient string handling with minimal allocations
- Connection pooling to reuse network resources
- Proper cleanup and resource management

## Testing Strategy

### Unit Tests
- Individual component testing in `tests/` directory
- Mock implementations for isolated testing
- Common test utilities in `tests/common/mod.rs`

### Integration Tests
- Full server lifecycle testing
- Multi-protocol compatibility verification
- Client SDK functionality validation
- C FFI binding correctness

### Performance Testing
- Use the benchmark tool from `../benchmark-rust/`
- Supports both gRPC and Thrift protocol benchmarking
- Configurable workload patterns and thread counts
- Database configuration testing with different TOML files

## Dependencies

### Core Dependencies
- `tokio`: Async runtime and utilities
- `tonic`: gRPC server and client implementation  
- `thrift`: Thrift protocol support
- `rocksdb`: RocksDB database bindings
- `prost`: Protocol Buffers implementation
- `tracing`: Structured logging and diagnostics

### Client SDK Dependencies
- `tokio`: Async runtime for client operations
- `thrift`: Thrift client protocol implementation
- `uuid`: Transaction ID generation
- `futures`: Future composition and utilities

### Build Dependencies
- `tonic-build`: Protocol buffer code generation
- Standard Rust toolchain (1.70+)

## Cross-Language Compatibility

The Rust implementation maintains full compatibility with:
- Go gRPC/Thrift clients via shared protocol definitions
- C++ gRPC servers via standard gRPC protocol
- C applications via FFI bindings in client SDK
- Benchmark tools via consistent protocol implementation

All protocol changes must maintain backward compatibility across language implementations.