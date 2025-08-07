# RocksDB gRPC Service

A high-performance gRPC service for key-value operations using RocksDB as the storage engine, with implementations in multiple languages and comprehensive benchmarking capabilities.

## Project Structure

```
.
├── go/                     # Go implementation
│   ├── main.go            # Go server implementation
│   ├── client.go          # Go client (works with both servers)
│   ├── benchmark.go       # Go benchmarking tool (works with both servers)
│   ├── go.mod            # Go module definition
│   └── kvstore/          # Generated protobuf files for Go
├── rust/                  # Rust implementation  
│   ├── src/
│   │   └── main.rs       # Rust server implementation
│   ├── Cargo.toml       # Rust dependencies
│   └── build.rs         # Protobuf build script
├── cpp/                   # C++ implementation
│   ├── src/
│   │   ├── main.cpp      # C++ server implementation
│   │   ├── kvstore_service.cpp # Service implementation
│   │   └── kvstore_service.h   # Service header
│   ├── CMakeLists.txt    # CMake build configuration
│   └── Makefile         # C++ build automation
├── proto/                 # Protocol buffer definitions
│   └── kvstore.proto     # Service and message definitions
├── data/                  # Database storage directory
├── generate.sh           # Protobuf code generation script
├── Makefile             # Build automation
└── README.md            # This file
```

## Features

- **Get**: Retrieve a value by key
- **Put**: Store a key-value pair
- **Delete**: Remove a key-value pair
- **ListKeys**: List all keys with optional prefix filtering
- **Benchmark**: Multi-threaded performance testing with detailed metrics

## Prerequisites

- Go 1.21 or later
- Rust (for Rust implementation)
- C++ compiler with C++17 support (for C++ implementation)
- CMake 3.16+ (for C++ implementation)
- Protocol Buffers compiler (`protoc`)
- RocksDB development libraries
- gRPC development libraries (for C++ implementation)

### Installing RocksDB

#### Ubuntu/Debian
```bash
sudo apt-get update
sudo apt-get install librocksdb-dev
```

#### macOS
```bash
brew install rocksdb
```

#### CentOS/RHEL
```bash
sudo yum install epel-release
sudo yum install rocksdb-devel
```

### Installing gRPC and protoc

#### Ubuntu/Debian
```bash
sudo apt-get install protobuf-compiler libgrpc++-dev libprotobuf-dev protobuf-compiler-grpc
```

#### macOS
```bash
brew install protobuf grpc
```

#### CentOS/RHEL
```bash
sudo yum install protobuf-compiler grpc-devel protobuf-devel
```

## Setup

### Quick Start with Makefile

```bash
# Generate protobuf files
make proto

# Build all servers and clients (Go, Rust, C++)
make build          # Debug builds
make build-release  # Release builds

# Or build individually:
make go-deps        # Install Go dependencies
make rust-deps      # Install Rust dependencies  
make cpp-deps       # Install C++ dependencies (Ubuntu/Debian)
```

### Manual Setup

1. **Generate gRPC code:**
   ```bash
   chmod +x generate.sh
   ./generate.sh
   ```

2. **Download Go dependencies:**
   ```bash
   make go-deps
   # or manually:
   cd go && go mod tidy
   ```

3. **Download Rust dependencies:**
   ```bash
   make rust-deps
   # or manually:
   cd rust && cargo fetch
   ```

4. **Build servers:**
   ```bash
   # Go server
   cd go && go build -o ../bin/rocksdbserver main.go
   
   # Rust server
   cd rust && cargo build --bin server && cp target/debug/server ../bin/rocksdbserver-rust
   
   # C++ server
   cd cpp && make debug && cp build/server ../bin/rocksdbserver-cpp
   ```

5. **Build Go client and benchmark (work with all servers):**
   ```bash
   cd go && go build -o ../bin/client client.go
   cd go && go build -o ../bin/benchmark benchmark.go
   ```

## Usage

### Starting the servers

**Go Server (port 50051):**
```bash
./bin/rocksdbserver
```

**Rust Server (port 50051):**
```bash
./bin/rocksdbserver-rust
```

**C++ Server (port 50051):**
```bash
./bin/rocksdbserver-cpp
```

All servers will create their own RocksDB databases:
- Go server: `./data/rocksdb` 
- Rust server: `./data/rocksdb-rust`
- C++ server: `./data/rocksdb-cpp`

### Using the client

The client works with all three server implementations. Since all use port 50051, start one server at a time:

**Basic operations:**
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
```

**Client options:**
- `-addr`: Server address (default: localhost:50051)
- `-op`: Operation (get, put, delete, list)
- `-key`: Key for operation
- `-value`: Value for put operation
- `-prefix`: Prefix for list operation
- `-limit`: Limit for list operation (default: 10)

## Benchmarking

The benchmark tool tests performance with mixed read/write workloads on both server implementations.

### Quick Start

```bash
# Build and run benchmark on Go server
make all
./bin/benchmark

# Test Rust server (stop Go server first, start Rust server)
./bin/benchmark
```

### Configuration Options

- `-mode`: "mixed" (read/write/ping) or "ping" (latency only)
- `-threads`: Concurrent threads (default: 32)
- `-requests`: Total requests (default: 100,000)
- `-write-pct`: Write percentage 0-100 (default: 30)
- `-key-size`: Key size in bytes (default: 16)
- `-value-size`: Value size in bytes (default: 100)

### Example Commands

```bash
# Read-heavy workload
./bin/benchmark -write-pct 10 -requests 1000000

# Write-heavy with large values
./bin/benchmark -write-pct 70 -value-size 1024

# Latency test
./bin/benchmark -mode ping -threads 1

# Compare servers
./bin/benchmark > go_results.txt    # Go server
./bin/benchmark > rust_results.txt  # Rust server (restart)
```

### Benchmark Output

The benchmark provides detailed performance statistics:

```
=== MIXED BENCHMARK STATISTICS ===
Total Operations: 100000
Successful: 99998 (99.998%)
Throughput: 15234.56 ops/sec

Latency Statistics:
  Average: 2.1ms
  P50: 1.8ms  P90: 3.2ms  P95: 4.1ms  P99: 8.7ms
```

## API Reference

**gRPC Service Operations:**
- `Get(GetRequest) returns (GetResponse)`
- `Put(PutRequest) returns (PutResponse)` 
- `Delete(DeleteRequest) returns (DeleteResponse)`
- `ListKeys(ListKeysRequest) returns (ListKeysResponse)`

**Using grpcurl:**
```bash
# Put
grpcurl -plaintext -d '{"key":"test","value":"data"}' localhost:50051 kvstore.KVStore/Put

# Get  
grpcurl -plaintext -d '{"key":"test"}' localhost:50051 kvstore.KVStore/Get
```

## Development

**To modify the service:**
1. Edit `proto/kvstore.proto`
2. Run `./generate.sh` to regenerate gRPC code
3. Update server implementation and rebuild

**Notes:**
- Both servers use graceful shutdown (SIGTERM/SIGINT)
- Database files stored in `./data/rocksdb/` (Go) and `./data/rocksdb-rust/` (Rust)
- Client timeout: 10 seconds
