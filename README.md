# RocksDB Key-Value Service

A high-performance key-value service supporting both gRPC and Thrift protocols, implemented in Rust with RocksDB as the storage engine. Features comprehensive benchmarking capabilities and configurable database settings.

## Project Structure

```
.
├── rust/                  # Unified Rust implementation  
│   ├── src/
│   │   ├── lib.rs        # Library exports and re-exports
│   │   ├── servers/      # Server implementations
│   │   │   ├── grpc_server.rs  # Rust gRPC server binary
│   │   │   └── shard_server.rs  # Rust shard server binary
│   │   ├── lib/          # Core library modules
│   │   │   ├── service.rs     # gRPC service implementation
│   │   │   ├── db.rs          # Database operations
│   │   │   ├── config.rs      # Configuration management
│   │   │   └── proto.rs       # Protocol buffer types
│   │   ├── client/       # Client SDK module
│   │   │   ├── mod.rs         # Module exports
│   │   │   ├── client.rs      # High-level client API
│   │   │   ├── transaction.rs # Transaction support
│   │   │   ├── ffi.rs         # C FFI bindings
│   │   │   ├── config.rs      # Client configuration
│   │   │   ├── error.rs       # Error types
│   │   │   └── future.rs      # Async future utilities
│   │   └── generated/    # Generated protocol code
│   │       └── kvstore.rs     # Thrift definitions (auto-generated)
│   ├── tests/            # All tests (unified)
│   │   ├── integration_tests.rs # Server integration tests
│   │   ├── client/            # Client SDK tests
│   │   ├── cpp_tests/         # C++ FFI tests
│   │   └── kvstore_client.h   # FFI header file
│   ├── crates/           # Sub-crates
│   │   └── benchmark/    # Benchmarking tools
│   ├── Cargo.toml       # Workspace configuration
│   └── build.rs         # Build script
├── proto/                 # Protocol buffer definitions
│   └── kvstore.proto     # gRPC service and message definitions
├── thrift/               # Thrift definitions
│   └── kvstore.thrift    # Thrift service and message definitions
├── nodejs/               # Node.js web viewer for the shard server
├── data/                  # Database storage directory (auto-created)
├── bin/                   # Compiled binaries (auto-created)
├── configs/               # Database configuration files
│   └── db/                # RocksDB configuration presets
│       ├── default.toml   # Default database settings
│       ├── cold_block_cache.toml # Cold cache configuration
│       └── warm_large_cache.toml # Large cache configuration
├── benchmark_results/     # Benchmark output and reports
├── scripts/               # Development and testing scripts
├── CMakeLists.txt        # CMake build configuration
└── README.md            # This file
```

## Features

- **Multiple Protocols**: Support for both gRPC and Thrift protocols
- **Rust Implementation**: High-performance async server implementation
- **C FFI Bindings**: Client library with C/C++ FFI support
- **Key Operations**:
  - **Get**: Retrieve a value by key
  - **Put**: Store a key-value pair
  - **Delete**: Remove a key-value pair
  - **ListKeys**: List all keys with optional prefix filtering
  - **Ping**: Health check and latency testing
- **Benchmarking**: Multi-threaded performance testing with detailed metrics and HTML reports
- **Configuration Management**: Configurable RocksDB settings for different workloads
- **High Performance**: Built on RocksDB for efficient storage and retrieval

## Prerequisites

### System Requirements
- **Rust**: 1.70 or later (with Cargo)
- **Node.js**: 18 or later (for optional web viewer)
- **Ubuntu/Debian**: 20.04 or later (for package installations below)

### Required System Packages

#### Ubuntu/Debian - Complete Installation
```bash
# Install all required packages in one command:
sudo apt update && sudo apt install -y \
    build-essential \
    pkg-config \
    librocksdb-dev \
    protobuf-compiler \
    libprotobuf-dev \
    thrift-compiler \
    git
```

#### Alternative Package Managers

**macOS (Homebrew):**
```bash
brew install rocksdb protobuf thrift rust
```

**CentOS/RHEL/Fedora:**
```bash
# Enable EPEL repository first (CentOS/RHEL)
sudo yum install epel-release  # or sudo dnf install epel-release

# Install packages
sudo yum install rocksdb-devel protobuf-compiler protobuf-devel thrift
# or for newer systems:
sudo dnf install rocksdb-devel protobuf-compiler protobuf-devel thrift
```

### Rust Installation (if not already installed)
```bash
# Install Rust via rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

## Build Instructions

### Quick Start (Recommended)

```bash
# Clone the repository
git clone <repository-url>
cd kv

# Build all servers and tools using CMake
cmake -B build -S .
cmake --build build

# For release builds:
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Verify build
ls bin/             # Should show: rocksdbserver-rust, shard-node, benchmark-rust, etc.
```

### Manual Rust Build

#### Generate Protocol Code
```bash
# Generate Thrift definitions
thrift --gen rs -out rust/src thrift/kvstore.thrift
```

#### Build Individual Components

**Rust gRPC Server:**
```bash
cd rust && cargo build --bin server
cp target/debug/server ../bin/rocksdbserver-rust
# For release: cargo build --release --bin server
```

**Rust Shard Node:**
```bash
cd rust && cargo build --bin shard-node
cp target/debug/shard-node ../bin/shard-node
# For release: cargo build --release --bin shard-node
```

**Rust Benchmark Tool:**
```bash
cd benchmark-rust && cargo build --release
cp target/release/benchmark ../bin/benchmark-rust
```

**Unified Rust Library with Client SDK and FFI:**
```bash
cd rust && cargo build --release --features ffi
```

### CMake Build Targets

```bash
cmake --build build --target build_help    # Show available targets
cmake --build build --target rust_grpc_server     # Build Rust gRPC server
cmake --build build --target rust_shard_node       # Build Rust shard node
cmake --build build --target rust_benchmark       # Build benchmark tool
cmake --build build --target rust_client_lib      # Build client library
cmake --build build --target unified_ffi_test     # Build FFI tests
cmake --build build --target test_ffi             # Run FFI tests
```

### Troubleshooting Build Issues

**Common Issues:**

1. **"protoc: command not found"**
   ```bash
   sudo apt install protobuf-compiler
   ```

2. **"thrift: command not found"**
   ```bash
   sudo apt install thrift-compiler
   ```

3. **RocksDB linking errors**
   ```bash
   sudo apt install librocksdb-dev pkg-config
   ```

4. **Rust compilation errors**
   ```bash
   cd rust && cargo clean && cargo build
   ```

**Verify Installation:**
```bash
# Check tools are available
protoc --version
thrift --version
rustc --version

# Check libraries
pkg-config --libs rocksdb
ldconfig -p | grep rocksdb
```

## Usage

### Starting the Servers

All servers use port 50051 by default. Start one server at a time:

**Rust gRPC Server:**
```bash
./bin/rocksdbserver-rust
# Database: ./data/rocksdb-rust/
```

**Rust Shard Node:**
```bash
./bin/shard-node
# Database: ./data/rocksdb-shard-node/
```

### Using the Rust Client Library

The client library can be used from Rust, C, or C++:

**Rust Usage:**
```rust
use rocksdb_server::client::{KvStoreClient, ClientConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = KvStoreClient::connect("localhost:9090")?;
    
    // Begin transaction
    let tx_future = client.begin_transaction(None, Some(60));
    let tx = tx_future.await_result().await?;
    
    // Set and get values
    tx.set("key1", "value1", None)?;
    let get_future = tx.get("key1", None);
    let value = get_future.await_result().await?;
    println!("Retrieved: {:?}", value);
    
    // Commit transaction
    let commit_future = tx.commit();
    commit_future.await_result().await?;
    
    Ok(())
}
```

**C/C++ Usage (via FFI):**
```cpp
#include "kvstore_client.h"

int main() {
    KvClientHandle client = kv_client_create("localhost:9090");
    
    // Begin transaction
    KvFutureHandle tx_future = kv_transaction_begin(client, 30);
    while (!kv_future_poll(tx_future)) {
        usleep(1000);  // Wait for completion
    }
    KvTransactionHandle tx = kv_future_get_transaction(tx_future);
    
    // Set and get values
    KvFutureHandle set_future = kv_transaction_set(tx, 
        KV_STR("key1"), KV_STR("value1"), NULL);
    // ... poll and handle set_future
    
    KvFutureHandle get_future = kv_transaction_get(tx, 
        KV_STR("key1"), NULL);
    // ... poll and get result
    
    // Commit
    KvFutureHandle commit_future = kv_transaction_commit(tx);
    // ... poll and handle commit
    
    kv_client_destroy(client);
    return 0;
}
```

### Node.js Web Viewer

```bash
cd nodejs
npm install
THRIFT_HOST=localhost THRIFT_PORT=9090 npm start
```

The web UI runs on http://localhost:3000 and communicates with the shard node.

### Protocol-Specific Usage

**gRPC with grpcurl:**
```bash
# Install grpcurl
go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest

# Put operation
grpcurl -plaintext -d '{"key":"test","value":"data"}' localhost:50051 kvstore.KVStore/Put

# Get operation
grpcurl -plaintext -d '{"key":"test"}' localhost:50051 kvstore.KVStore/Get

# List keys
grpcurl -plaintext -d '{"prefix":"", "limit":10}' localhost:50051 kvstore.KVStore/ListKeys
```

## Benchmarking

The Rust benchmark tool supports multiple protocols (gRPC, Thrift, and raw RocksDB) and can test various workload patterns with configurable database settings.

### Quick Start

```bash
# Build everything first (release mode for accurate benchmarks)
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Start a server (choose one)
./bin/rocksdbserver-rust &     # Rust gRPC server  
# OR
./bin/shard-node &               # Rust shard node

# Run benchmark
./bin/benchmark-rust           # gRPC protocol
```

### Configuration Options

**Connection:**
- `-a`: Server address (default: localhost:50051)

**Workload Configuration:**
- `-t`: Concurrent threads (default: based on CPU cores)
- `-n`: Total requests (default: 100,000)
- `-w`: Write percentage 0-100 (default: 30)

**Data Configuration:**
- `-key-size`: Key size in bytes (default: 16)
- `-value-size`: Value size in bytes (default: 100)

**Database Configuration:**
- `-c`: Path to TOML configuration file for RocksDB settings
- Available presets: `configs/db/default.toml`, `configs/db/cold_block_cache.toml`, `configs/db/warm_large_cache.toml`

### Example Benchmark Commands

```bash
# Default benchmark
./bin/benchmark-rust

# Read-heavy workload
./bin/benchmark-rust -w 10 -n 1000000

# High-concurrency test
./bin/benchmark-rust -t 64 -n 500000

# Large-scale test
./bin/benchmark-rust -t 32 -n 2000000 -w 20

# Test with cold cache configuration
./bin/benchmark-rust -c configs/db/cold_block_cache.toml

# Raw RocksDB performance test
./bin/benchmark-rust --raw -t 16 -n 500000
```

### Benchmark Output

```
Starting benchmark...
Protocol: gRPC
Threads: 32, Requests: 100000, Write %: 30

=== MIXED BENCHMARK STATISTICS ===
Total Operations: 100000
Successful: 99998 (99.998%)
Failed: 2 (0.002%)

Operation Breakdown:
- Reads: 70000 operations
- Writes: 30000 operations  
- Pings: 0 operations

Throughput: 15234.56 ops/sec
Duration: 6.567s

Latency Statistics:
  Average: 2.1ms
  P50: 1.8ms   P90: 3.2ms   P95: 4.1ms   P99: 8.7ms   P99.9: 15.2ms

Per-Operation Stats:
  Reads  - Avg: 1.9ms, P99: 7.2ms
  Writes - Avg: 2.4ms, P99: 11.1ms
```

### Performance Characteristics

**Typical Results (example hardware):**
- **Thrift Protocol**: ~20,000-25,000 ops/sec, lower latency
- **gRPC Protocol**: ~15,000-20,000 ops/sec, better tooling ecosystem
- **Read Performance**: Generally 2-3x faster than writes
- **Raw RocksDB**: Highest throughput for comparison baseline

## API Reference

### gRPC Service Definition

**Service:** `kvstore.KVStore`

**Operations:**
- `Get(GetRequest) returns (GetResponse)` - Retrieve value by key
- `Put(PutRequest) returns (PutResponse)` - Store key-value pair  
- `Delete(DeleteRequest) returns (DeleteResponse)` - Remove key-value pair
- `ListKeys(ListKeysRequest) returns (ListKeysResponse)` - List keys with optional prefix
- `Ping(PingRequest) returns (PingResponse)` - Health check and latency test

### Thrift Service Definition

**Service:** `KVStore`

**Operations:**
- `GetResponse get(GetRequest request)` - Retrieve value by key
- `PutResponse put(PutRequest request)` - Store key-value pair
- `DeleteResponse delete_key(DeleteRequest request)` - Remove key-value pair
- `ListKeysResponse list_keys(ListKeysRequest request)` - List keys with optional prefix
- `PingResponse ping(PingRequest request)` - Health check and latency test

### Message Formats

**GetRequest:** `{key: string}`  
**GetResponse:** `{value: string, found: bool}`

**PutRequest:** `{key: string, value: string}`  
**PutResponse:** `{success: bool, error?: string}`

**DeleteRequest:** `{key: string}`  
**DeleteResponse:** `{success: bool, error?: string}`

**ListKeysRequest:** `{prefix?: string, limit?: int32}`  
**ListKeysResponse:** `{keys: string[]}`

**PingRequest:** `{message?: string, timestamp?: int64}`  
**PingResponse:** `{message: string, timestamp: int64, server_timestamp: int64}`

## Development

### Project Structure Notes

- **Generated Files**: Files like `rust/src/kvstore.rs` are auto-generated and should not be edited directly
- **Protocol Definitions**: Edit `proto/kvstore.proto` for gRPC or `thrift/kvstore.thrift` for Thrift changes
- **Database Storage**: Each server uses its own RocksDB database directory in `./data/`

### Making Changes

**To modify the service:**
1. Edit `proto/kvstore.proto` (for gRPC) or `thrift/kvstore.thrift` (for Thrift)
2. Regenerate code: `thrift --gen rs -out rust/src thrift/kvstore.thrift`
3. Update server implementation in Rust
4. Rebuild: `cmake --build build`

**To add new operations:**
1. Add to protocol definition files
2. Regenerate protocol code
3. Implement in server implementation
4. Update client library and benchmark tools
5. Test with both protocols

### Code Generation

```bash
# Regenerate Thrift code
thrift --gen rs -out rust/src thrift/kvstore.thrift
```

### Testing

```bash
# Quick functionality test
cmake --build build
./bin/rocksdbserver-rust &
# Use Rust client library or grpcurl to test operations
killall rocksdbserver-rust

# Performance test
./bin/rocksdbserver-rust &
./bin/benchmark-rust -n 1000
killall rocksdbserver-rust

# FFI tests
cmake --build build --target test_ffi
```

### Performance Notes

- **Database Persistence**: Data persists between server restarts in `./data/` directories
- **Graceful Shutdown**: All servers handle SIGTERM/SIGINT gracefully
- **Concurrency**: All servers are designed for high-concurrency workloads
- **Memory Usage**: RocksDB manages its own memory and disk caching

### Troubleshooting

**Port Already in Use:**
```bash
lsof -i :50051          # Check what's using the port
killall rocksdbserver-rust shard-node
```

**Database Issues:**
```bash
rm -rf data/            # Clear all databases (will lose data!)
```

**Build Cache Issues:**
```bash
cd rust && cargo clean  # Clear Rust build cache
rm -rf build/           # Clear CMake build cache
```