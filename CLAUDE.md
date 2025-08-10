# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance gRPC key-value store service using RocksDB as the storage engine. The project demonstrates multi-language implementation patterns with identical gRPC service interfaces across Go, Rust, and C++ implementations.

## Architecture

### Multi-Language Service Implementation
The codebase implements the same gRPC KVStore service in three languages:
- **Go** (`go/main.go`): Server with transaction database support and semaphore-based concurrency control
- **Rust** (`rust/src/main.rs`): Async server using Tokio with similar concurrency patterns  
- **C++** (`cpp/src/`): High-performance server with TransactionDB and custom semaphore implementation

### Core Components
- **Proto Definition** (`proto/kvstore.proto`): Single source of truth for gRPC service contract
- **Database Storage**: Each implementation uses RocksDB TransactionDB for ACID compliance
- **Concurrency Control**: All implementations use semaphore-based limits (32 read, 16 write operations)
- **Client Tools** (`go/client.go`): Language-agnostic client that works with all server implementations

### Data Storage
- Go server: `./data/rocksdb/`
- Rust server: `./data/rocksdb-rust/`
- C++ server: `./data/rocksdb-cpp/`

## Common Development Commands

### Protocol Buffer Generation
```bash
./generate.sh              # Generate protobuf files for all languages
make proto                 # Same as above
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
./bin/rocksdbserver        # Go server
./bin/rocksdbserver-rust   # Rust server
./bin/rocksdbserver-cpp    # C++ server
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
Use the Rust benchmark tool for performance testing:
```bash
./bin/benchmark-rust                               # Default mixed workload
./bin/benchmark-rust -w 10 -n 1000000            # Read-heavy workload
```

## Development Workflow

### Adding New gRPC Methods
1. Edit `proto/kvstore.proto` to add new service methods and messages
2. Run `./generate.sh` to regenerate protobuf code for all languages
3. Implement the new method in each server implementation:
   - Go: Add method to Server struct in `go/main.go`
   - Rust: Add method to KvStoreService impl in `rust/src/main.rs`
   - C++: Add method to KvStoreService class in `cpp/src/kvstore_service.cpp`
4. Update client if needed in `go/client.go`

### Testing Changes
- Use the Rust benchmark tool to verify performance across implementations
- Test with the client to verify functionality
- Each server creates separate database directories for isolation

### Language-Specific Build Systems
- **Go**: Standard Go modules with `go.mod`
- **Rust**: Cargo with `build.rs` for protobuf generation
- **C++**: CMake with automatic protobuf generation in build process

## Performance Characteristics

### Concurrency Model
All implementations use identical concurrency limits:
- 32 concurrent read operations
- 16 concurrent write operations
- Semaphore-based throttling to prevent resource exhaustion

### Database Features
- ACID transactions via RocksDB TransactionDB
- Pessimistic locking for write operations
- Efficient prefix-based key iteration for ListKeys operation
- Automatic database creation and directory setup

## Cross-Language Compatibility

The client is implemented in Go and the benchmark tool in Rust, both work seamlessly with all server implementations due to the shared gRPC contract. This design allows for:
- Performance comparison between language implementations
- Mixed-language deployments
- Consistent API behavior across all implementations