# RocksDB gRPC Service

A simple gRPC service for key-value operations using RocksDB as the storage engine.

## Features

- **Get**: Retrieve a value by key
- **Put**: Store a key-value pair
- **Delete**: Remove a key-value pair
- **ListKeys**: List all keys with optional prefix filtering

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

1. **Generate gRPC code:**
   ```bash
   chmod +x generate.sh
   ./generate.sh
   ```

2. **Download dependencies:**
   ```bash
   go mod tidy
   ```

3. **Build the server:**
   ```bash
   go build -o server main.go
   ```

4. **Build the client:**
   ```bash
   go build -o client client.go
   ```

## Usage

### Starting the server

```bash
./server
```

The server will start on port 50051 and create a RocksDB database in the `./data/rocksdb` directory.

### Using the client

#### Put a key-value pair
```bash
./client -op put -key "hello" -value "world"
```

#### Get a value by key
```bash
./client -op get -key "hello"
```

#### Delete a key
```bash
./client -op delete -key "hello"
```

#### List all keys
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
