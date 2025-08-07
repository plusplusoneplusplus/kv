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
- Protocol Buffers compiler (`protoc`)
- RocksDB development libraries

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

### Installing protoc

#### Ubuntu/Debian
```bash
sudo apt-get install protobuf-compiler
```

#### macOS
```bash
brew install protobuf
```

## Setup

### Quick Start with Makefile

```bash
# Generate protobuf files
make proto

# Build all Go binaries
make all

# Build Rust server
make rust

# Or build individually:
make go-server      # Builds Go rocksdbserver
make go-client      # Builds Go client (works with both servers)
make go-benchmark   # Builds Go benchmark tool (works with both servers)
make rust-server    # Builds Rust rocksdbserver-rust
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
   cd go && go build -o ../rocksdbserver main.go
   
   # Rust server
   cd rust && cargo build --bin server && cp target/debug/server ../rocksdbserver-rust
   ```

5. **Build Go client and benchmark (work with both servers):**
   ```bash
   cd go && go build -o ../client client.go
   cd go && go build -o ../benchmark benchmark.go
   ```

## Usage

### Starting the servers

**Go Server (port 50051):**
```bash
./rocksdbserver
```

**Rust Server (port 50052):**
```bash
./rocksdbserver-rust
```

Both servers will create their own RocksDB databases:
- Go server: `./data/rocksdb` 
- Rust server: `./data/rocksdb-rust`

### Using the client

The Go client works with both Go and Rust servers. Use the `--addr` flag to specify which server to connect to:

#### Connect to Go server (default):
```bash
./client -op put -key "hello" -value "world"
./client -op get -key "hello"
```

#### Connect to Rust server:
```bash
./client --addr localhost:50052 -op put -key "hello" -value "world"
./client --addr localhost:50052 -op get -key "hello"
```

#### Available operations:

**Put a key-value pair:**
```bash
./client -op put -key "hello" -value "world"
```

**Get a value by key:**
```bash
./client -op get -key "hello"
```

**Delete a key:**
```bash
./client -op delete -key "hello"
```

**List all keys:**
```bash
./client -op list
```

#### List keys with prefix
```bash
./client -op list -prefix "user:" -limit 20
```

### Client Options

- `-addr`: Server address (default: localhost:50051)
- `-op`: Operation (get, put, delete, list)
- `-key`: Key for operation
- `-value`: Value for put operation
- `-prefix`: Prefix for list operation
- `-limit`: Limit for list operation (default: 10)

## API Reference

### Proto Definition

The service is defined in `proto/kvstore.proto` with the following operations:

- `Get(GetRequest) returns (GetResponse)`
- `Put(PutRequest) returns (PutResponse)`
- `Delete(DeleteRequest) returns (DeleteResponse)`
- `ListKeys(ListKeysRequest) returns (ListKeysResponse)`

### Example gRPC calls

Using grpcurl (install with `go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest`):

```bash
# Put
grpcurl -plaintext -d '{"key":"test","value":"data"}' localhost:50051 kvstore.KVStore/Put

# Get
grpcurl -plaintext -d '{"key":"test"}' localhost:50051 kvstore.KVStore/Get

# List
grpcurl -plaintext -d '{"prefix":"","limit":10}' localhost:50051 kvstore.KVStore/ListKeys

# Delete
grpcurl -plaintext -d '{"key":"test"}' localhost:50051 kvstore.KVStore/Delete
```

## Project Structure

```
rocksdb_svc/
├── proto/
│   └── kvstore.proto      # Protocol buffer definition
├── kvstore/               # Generated gRPC code (created by generate.sh)
├── data/                  # RocksDB data directory (created at runtime)
├── main.go                # gRPC server implementation
├── client.go              # CLI client
├── generate.sh            # Script to generate gRPC code
├── go.mod                 # Go module file
└── README.md             # This file
```

## Development

To modify the service:

1. Edit the protobuf definition in `proto/kvstore.proto`
2. Run `./generate.sh` to regenerate the gRPC code
3. Update the server implementation in `main.go`
4. Rebuild and test

## Notes

- The server uses graceful shutdown on SIGTERM/SIGINT
- RocksDB options are set to reasonable defaults
- The client has a 10-second timeout for operations
- Database files are stored in `./data/rocksdb/`
