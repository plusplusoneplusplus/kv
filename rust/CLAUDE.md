# CLAUDE.md - Rust KV Store Implementation

This file provides guidance to Claude Code when working with the Rust implementation of the high-performance key-value store service.

## Project Overview

This is a unified Rust implementation of a transactional key-value store service supporting both gRPC and Thrift protocols, using RocksDB as the storage engine. The implementation combines server and client components in a single workspace, emphasizing async performance using Tokio and providing a comprehensive client SDK with C FFI bindings.

## Architecture

### Core Components
- **Main Library** (`src/lib.rs`): Unified library exports and re-exports for both server and client
- **Server Implementations**:
  - `src/servers/grpc_server.rs`: gRPC server binary using Tokio async runtime
  - `src/servers/thrift_server.rs`: Thrift server binary with async support
- **Core Server Libraries** (`src/lib/`):
  - `service.rs`: gRPC service implementation with TransactionalKvDatabase
  - `db.rs`: RocksDB wrapper with transaction support
  - `config.rs`: Configuration management with TOML support
  - `proto.rs`: Generated protobuf types and service definitions
- **Client SDK Module** (`src/client/`): High-performance async client with C FFI bindings
  - `mod.rs`: Client module exports and re-exports
  - `client.rs`: High-level client API for connecting and managing transactions
  - `transaction.rs`: Transaction and ReadTransaction implementations
  - `ffi.rs`: C FFI bindings for cross-language integration
  - `config.rs`: Client configuration with debug logging support
  - `error.rs`: Comprehensive error types and result handling
  - `future.rs`: Async future utilities for FFI compatibility
- **Generated Code** (`src/generated/`): Shared protocol definitions
- **Unified Testing** (`tests/`): All tests in organized subdirectories
  - `integration_tests.rs`: Server integration tests
  - `client/`: Client SDK tests
  - `cpp_tests/`: C++ FFI tests
- **Sub-Crates** (`crates/`): Additional workspace members
  - `benchmark/`: Benchmarking tools

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

# Build unified library with client SDK
cargo build --release

# Build with C FFI support
cargo build --release --features ffi

# Build with RSML consensus support (requires private RSML submodule)
cargo build --features rsml

# Or via CMake with environment variable
WITH_RSML=true cmake --build build
```

### Feature Flags
- **`ffi`** (default): Enables C FFI bindings for client SDK
- **`rsml`**: Enables RSML consensus implementation (manual local development only)
  - **Commented out by default** to prevent CI failures with private dependencies
  - To enable: Uncomment `consensus-rsml` dependency and `rsml` feature in `rust/Cargo.toml`
  - Requires access to private RSML submodule: `git submodule update --init --recursive`
  - After uncommenting: Use `cargo build --features rsml` to build with RSML support
  - Use `../scripts/run_test.sh` for testing (RSML enabled by default when available)
  - When enabled, provides access to `RsmlFactoryBuilder`, `RsmlError`, and `RsmlConfig` types
  - CI builds work without any RSML references for public buildability

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

# All tests (unified)
cargo test

# Integration tests (requires running server)
cargo test --test integration_tests

# Client SDK tests (requires Thrift server)
cargo test client

# C FFI tests (via CMake)
cd .. && cmake --build build --target test_ffi

# Test with RSML feature (manual local development only)
cd ../scripts && ./run_test.sh
# To disable RSML:
cd ../scripts && ./run_test.sh --no-rsml
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

## Unified Library Structure

The crate now combines server and client components in a single workspace:

### Library Exports
- **Server Types**: `Config`, `TransactionalKvDatabase`, protocol types
- **Client Types**: `KvStoreClient`, `ClientConfig`, `KvError`, `Transaction`, etc.
- **Shared Code**: Protocol definitions, error types, utilities

### Client SDK Features
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
use rocksdb_server::client::{KvStoreClient, KvResult};

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
use rocksdb_server::client::{KvStoreClient, ClientConfig, KvResult};

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

KvClientHandle client = kv_client_create("localhost:9090");
KvFutureHandle tx_future = kv_transaction_begin(client, 60);
while (!kv_future_poll(tx_future)) { usleep(1000); }
KvTransactionHandle tx = kv_future_get_transaction(tx_future);
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
4. Update client SDK in `src/client/` if needed
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

### Unified Test Structure
- **Server Tests**: `tests/integration_tests.rs` - Full server lifecycle testing
- **Client Tests**: `tests/client/` - Client SDK functionality validation  
- **FFI Tests**: `tests/cpp_tests/` - C++ FFI binding correctness
- **Common Utilities**: `tests/common/mod.rs` - Shared test helpers

### Test Organization
- All tests run with single `cargo test` command
- Integration tests require running server on localhost:9090
- FFI tests can be run via CMake: `cmake --build build --target test_ffi`
- Client and server tests share common test utilities

### Performance Testing
- Use the benchmark tool from `crates/benchmark/`
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