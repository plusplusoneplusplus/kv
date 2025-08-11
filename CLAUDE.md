# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance key-value store service supporting both gRPC and Thrift protocols, using RocksDB as the storage engine. The project demonstrates multi-language implementation patterns with identical service interfaces across Go, Rust, and C++ implementations, featuring comprehensive benchmarking and configurable database settings.

## Architecture

### Multi-Language Service Implementation
The codebase implements the same KVStore service across multiple protocols in three languages:
- **Go** (`go/main.go`, `go/server.go`): gRPC server with transaction database support
- **Rust** (`rust/src/main.rs`, `rust/src/thrift_main.rs`): Both gRPC and Thrift servers using Tokio async runtime
- **C++** (`cpp/src/`): High-performance gRPC server with TransactionDB and custom semaphore implementation

### Core Components
- **Protocol Definitions**: 
  - `proto/kvstore.proto`: gRPC service contract
  - `thrift/kvstore.thrift`: Thrift service contract
- **Database Storage**: Each implementation uses RocksDB with configurable settings
- **Benchmarking Suite** (`benchmark-rust/`): Multi-protocol performance testing with support for gRPC, Thrift, and raw RocksDB access
- **Client Tools** (`go/client.go`, `go/ping_client.go`): Language-agnostic clients
- **Configuration Management** (`configs/db/`): RocksDB tuning presets for different workloads

### Data Storage
- Go gRPC server: `./data/rocksdb/`
- Rust gRPC server: `./data/rocksdb-rust/`
- Rust Thrift server: `./data/rocksdb-thrift/`
- C++ gRPC server: `./data/rocksdb-cpp/`
- Benchmark raw databases: `./data/cold-*`, `./data/test-*`, etc.

## Common Development Commands

### Protocol Code Generation
```bash
./generate.sh              # Generate protobuf files for all languages
make proto                 # Same as above
make thrift                # Generate Thrift files for Go and Rust
```

### Building
```bash
# Build all implementations (debug)
make build

# Build all implementations (release)
make build-release

# Individual language builds
make go-deps && cd go && go build -o ../bin/rocksdbserver main.go
make rust-deps && cd rust && cargo build --bin server
cd cpp && make debug       # or make release
```

### Running Servers
All servers listen on port 50051 - run one at a time:
```bash
./bin/rocksdbserver        # Go gRPC server
./bin/rocksdbserver-rust   # Rust gRPC server
./bin/rocksdbserver-thrift # Rust Thrift server
./bin/rocksdbserver-cpp    # C++ gRPC server
```

### Client Operations
The Go client works with all server implementations:
```bash
./bin/client -op put -key "test" -value "data"
./bin/client -op get -key "test"
./bin/client -op delete -key "test"
./bin/client -op list -prefix "user:" -limit 20
```

### Benchmarking
Use the Rust benchmark tool for performance testing across protocols:
```bash
./bin/benchmark-rust                               # Default gRPC mixed workload
./bin/benchmark-rust --thrift -w 10 -n 1000000   # Thrift protocol, read-heavy
./bin/benchmark-rust --raw -t 32 -n 500000       # Raw RocksDB access
./bin/benchmark-rust -c configs/db/cold_block_cache.toml  # Custom config
```

## Development Workflow

### Adding New Service Methods
1. Edit protocol definition files:
   - `proto/kvstore.proto` for gRPC methods and messages
   - `thrift/kvstore.thrift` for Thrift methods and messages
2. Run `make proto thrift` to regenerate code for all languages
3. Implement the new method in each server implementation:
   - Go: Add method to Server struct in `go/server.go`
   - Rust gRPC: Add method to KvStoreService impl in `rust/src/service.rs`
   - Rust Thrift: Add method to ThriftKvStoreService impl in `rust/src/thrift_main.rs`
   - C++: Add method to KvStoreService class in `cpp/src/kvstore_service.cpp`
4. Update client if needed in `go/client.go`

### Testing Changes
- Use the Rust benchmark tool to verify performance across implementations and protocols
- Test with the client to verify functionality (supports both gRPC and Thrift)
- Each server creates separate database directories for isolation
- Use different configuration files to test various database tunings

### Language-Specific Build Systems
- **Go**: Standard Go modules with `go.mod` and `go.sum`
- **Rust**: Cargo with `build.rs` for protobuf generation and separate binaries for gRPC and Thrift servers
- **C++**: CMake with automatic protobuf generation in build process
- **Benchmark**: Separate Rust project with comprehensive client implementations

## Performance Characteristics

### Concurrency Model
Different implementations have varying concurrency approaches:
- **Go**: Configurable connection limits and goroutine-based concurrency
- **Rust**: Tokio async runtime with configurable connection pools
- **C++**: Thread-based concurrency with semaphore controls
- **Benchmarking**: Multi-threaded client with configurable thread counts

### Database Features
- RocksDB storage engine with configurable options via TOML files
- Support for different cache configurations (cold, warm, large cache)
- Efficient prefix-based key iteration for ListKeys operation
- Automatic database creation and directory setup
- Separate database instances per server implementation

## Cross-Language and Cross-Protocol Compatibility

The client is implemented in Go and the benchmark tool in Rust, both work seamlessly with all server implementations due to shared protocol contracts. This design allows for:
- Performance comparison between language implementations and protocols
- Mixed-language deployments with protocol flexibility
- Consistent API behavior across all implementations and protocols
- Protocol-specific optimizations (gRPC vs Thrift vs raw RocksDB)
- Comprehensive benchmarking across all combinations