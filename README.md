# RocksDB Key-Value Service

A high-performance key-value service supporting both gRPC and Thrift protocols, with RocksDB as the storage engine. Features implementations in Go and Rust with comprehensive benchmarking capabilities.

## Project Structure

```
.
├── go/                     # Go implementation
│   ├── main.go            # Go gRPC server implementation
│   ├── server.go          # Server logic
│   ├── client.go          # Go client (works with both protocols)
│   ├── ping_client.go     # Ping client for testing
│   ├── go.mod            # Go module definition
│   ├── proto/            # Generated gRPC/protobuf files for Go
│   └── thrift/           # Generated Thrift files for Go
├── rust/                  # Rust implementation  
│   ├── src/
│   │   ├── main.rs       # Rust gRPC server implementation
│   │   ├── thrift_main.rs # Rust Thrift server implementation
│   │   ├── service.rs    # gRPC service implementation
│   │   ├── db.rs         # Database operations
│   │   └── kvstore.rs    # Generated Thrift definitions (auto-generated)
│   ├── Cargo.toml       # Rust dependencies
│   └── build.rs         # Protobuf build script
├── cpp/                   # C++ implementation (optional)
│   ├── src/
│   │   ├── main.cpp      # C++ server implementation
│   │   ├── kvstore_service.cpp # Service implementation
│   │   └── kvstore_service.h   # Service header
│   ├── CMakeLists.txt    # CMake build configuration
│   └── README.md         # C++ specific instructions
├── benchmark-rust/        # Rust benchmarking tools
│   └── src/              # Rust benchmark implementation
├── proto/                 # Protocol buffer definitions
│   └── kvstore.proto     # gRPC service and message definitions
├── thrift/               # Thrift definitions
│   └── kvstore.thrift    # Thrift service and message definitions
├── data/                  # Database storage directory (auto-created)
├── bin/                   # Compiled binaries (auto-created)
├── generate.sh           # Protobuf code generation script
├── Makefile             # Build automation for all targets
└── README.md            # This file
```

## Features

- **Multiple Protocols**: Support for both gRPC and Thrift protocols
- **Multi-language**: Go and Rust server implementations
- **Key Operations**:
  - **Get**: Retrieve a value by key
  - **Put**: Store a key-value pair
  - **Delete**: Remove a key-value pair
  - **ListKeys**: List all keys with optional prefix filtering
  - **Ping**: Health check and latency testing
- **Benchmarking**: Multi-threaded performance testing with detailed metrics (Rust implementation)
- **High Performance**: Built on RocksDB for efficient storage and retrieval

## Prerequisites

### System Requirements
- **Go**: 1.18 or later
- **Rust**: 1.70 or later (with Cargo)
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

# Optional: gRPC C++ libraries (only needed if building C++ implementation)
sudo apt install -y libgrpc++-dev libgrpc-dev protobuf-compiler-grpc
```

#### Alternative Package Managers

**macOS (Homebrew):**
```bash
brew install rocksdb protobuf thrift go rust
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

### Language-Specific Setup

#### Go Installation (if not already installed)
```bash
# Download and install Go 1.21+
wget https://go.dev/dl/go1.21.5.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.21.5.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
```

#### Rust Installation (if not already installed)
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

# Generate protocol definitions (gRPC + Thrift)
make proto
make thrift

# Build all servers and tools
make build          # Debug builds
# OR
make build-release  # Optimized release builds

# Verify build
ls bin/             # Should show: benchmark-rust, client, rocksdbserver, rocksdbserver-rust, rocksdbserver-thrift
```

### Step-by-Step Build Process

#### 1. Generate Protocol Code
```bash
# Generate gRPC protobuf files
make proto
# Equivalent to: ./generate.sh

# Generate Thrift definitions
make thrift
# Equivalent to: thrift --gen rs -out rust/src thrift/kvstore.thrift
```

#### 2. Install Dependencies
```bash
# Go dependencies
make go-deps
# Equivalent to: go mod tidy && go mod download

# Rust dependencies
make rust-deps  
# Equivalent to: cd rust && cargo fetch

# Rust benchmark dependencies handled by cargo
```

#### 3. Build Individual Components

**Go gRPC Server:**
```bash
go build -o bin/rocksdbserver go/main.go go/server.go
```

**Rust gRPC Server:**
```bash
cd rust && cargo build --bin server
cp target/debug/server ../bin/rocksdbserver-rust
# For release: cargo build --release --bin server
```

**Rust Thrift Server:**
```bash
cd rust && cargo build --bin thrift-server
cp target/debug/thrift-server ../bin/rocksdbserver-thrift
# For release: cargo build --release --bin thrift-server
```

**Client Tool:**
```bash
go build -o bin/client go/client.go
```

**Rust Benchmark Tool:**
```bash
cd benchmark-rust && cargo build --release
cp target/release/benchmark ../bin/benchmark-rust
```

### Makefile Targets

```bash
make build          # Build all (debug mode)
make build-release  # Build all (release mode) 
make proto          # Generate gRPC/protobuf files
make thrift         # Generate Thrift files
make clean          # Clean all build artifacts
make go-deps        # Install Go dependencies
make rust-deps      # Install Rust dependencies
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

4. **Go module issues**
   ```bash
   go clean -modcache
   go mod download
   ```

5. **Rust compilation errors**
   ```bash
   cd rust && cargo clean && cargo build
   ```

**Verify Installation:**
```bash
# Check tools are available
protoc --version
thrift --version
go version
rustc --version

# Check libraries
pkg-config --libs rocksdb
ldconfig -p | grep rocksdb
```

## Usage

### Starting the Servers

All servers use port 50051 by default. Start one server at a time:

**Go gRPC Server:**
```bash
./bin/rocksdbserver
# Database: ./data/rocksdb/
```

**Rust gRPC Server:**
```bash
./bin/rocksdbserver-rust
# Database: ./data/rocksdb-rust/
```

**Rust Thrift Server:**
```bash
./bin/rocksdbserver-thrift
# Database: ./data/rocksdb-thrift/
```

### Using the Client

The client automatically detects and works with both gRPC and Thrift protocols:

**Basic Operations:**
```bash
# Put a key-value pair
./bin/client -op put -key "hello" -value "world"

# Get a value by key
./bin/client -op get -key "hello"

# Delete a key
./bin/client -op delete -key "hello"

# List all keys
./bin/client -op list

# List keys with prefix
./bin/client -op list -prefix "user:" -limit 20

# Health check / ping
./bin/client -op ping
```

**Client Configuration:**
- `-addr`: Server address (default: localhost:50051)
- `-op`: Operation (get, put, delete, list, ping)
- `-key`: Key for operation
- `-value`: Value for put operation
- `-prefix`: Prefix for list operation
- `-limit`: Limit for list operation (default: 10)

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

The Rust benchmark tool supports gRPC protocol and can test various workload patterns.

### Quick Start

```bash
# Build everything first
make build-release

# Start a server (choose one)
./bin/rocksdbserver &          # Go gRPC server
# OR
./bin/rocksdbserver-rust &     # Rust gRPC server  
# OR
./bin/rocksdbserver-thrift &   # Rust Thrift server

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

### Performance Comparison

**Typical Results (example hardware):**
- **Thrift Protocol**: ~20,000-25,000 ops/sec, lower latency
- **gRPC Protocol**: ~15,000-20,000 ops/sec, better tooling ecosystem
- **Read Performance**: Generally 2-3x faster than writes
- **Rust vs Go**: Rust typically 10-20% higher throughput

### Advanced Benchmarking

**Server Comparison:**
```bash
# Test server implementations
./bin/rocksdbserver & PID1=$!
./bin/benchmark-rust > go_grpc.txt
kill $PID1

./bin/rocksdbserver-rust & PID2=$!  
./bin/benchmark-rust > rust_grpc.txt
kill $PID2
```

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

- **Generated Files**: Files like `rust/src/kvstore.rs` and `go/proto/*.pb.go` are auto-generated and should not be edited directly
- **Protocol Definitions**: Edit `proto/kvstore.proto` for gRPC or `thrift/kvstore.thrift` for Thrift changes
- **Database Storage**: Each server uses its own RocksDB database directory in `./data/`

### Making Changes

**To modify the service:**
1. Edit `proto/kvstore.proto` (for gRPC) or `thrift/kvstore.thrift` (for Thrift)
2. Regenerate code: `make proto` and/or `make thrift`
3. Update server implementations in Go and Rust
4. Rebuild: `make build-release`

**To add new operations:**
1. Add to protocol definition files
2. Regenerate protocol code
3. Implement in both server implementations
4. Update client and benchmark tools
5. Test with both protocols

### Code Generation

```bash
# Regenerate all protocol code
make proto thrift

# Individual generation
./generate.sh                                          # gRPC only
thrift --gen rs -out rust/src thrift/kvstore.thrift   # Thrift only
```

### Testing

```bash
# Quick functionality test
make build-release
./bin/rocksdbserver &
./bin/client -op put -key "test" -value "hello"
./bin/client -op get -key "test"
./bin/client -op delete -key "test"
killall rocksdbserver

# Performance test
./bin/rocksdbserver-rust &
./bin/benchmark-rust -n 1000
killall rocksdbserver-rust
```

### Performance Notes

- **Database Persistence**: Data persists between server restarts in `./data/` directories
- **Graceful Shutdown**: All servers handle SIGTERM/SIGINT gracefully
- **Connection Timeout**: Client timeout is set to 10 seconds
- **Concurrency**: All servers are designed for high-concurrency workloads
- **Memory Usage**: RocksDB manages its own memory and disk caching

### Troubleshooting

**Port Already in Use:**
```bash
lsof -i :50051          # Check what's using the port
killall rocksdbserver rocksdbserver-rust rocksdbserver-thrift
```

**Database Issues:**
```bash
rm -rf data/            # Clear all databases (will lose data!)
```

**Build Cache Issues:**
```bash
make clean              # Clean all build artifacts
go clean -modcache      # Clear Go module cache
cd rust && cargo clean  # Clear Rust build cache
```
