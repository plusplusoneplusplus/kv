# C++ RocksDB gRPC Server

This is a C++ implementation of the KVStore gRPC service using RocksDB as the storage backend.

## Features

- **High Performance**: Built with C++ for optimal performance
- **Transaction Support**: Uses RocksDB TransactionDB for ACID compliance
- **Concurrency Control**: Implements semaphore-based concurrency limiting (32 concurrent reads, 16 concurrent writes)
- **gRPC Interface**: Compatible with the same protobuf service definition as Go and Rust implementations
- **Signal Handling**: Graceful shutdown on SIGINT/SIGTERM

## Dependencies

### Ubuntu/Debian
```bash
sudo apt-get update && sudo apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libgrpc++-dev \
    libprotobuf-dev \
    protobuf-compiler-grpc \
    librocksdb-dev
```

### Fedora/RHEL
```bash
sudo dnf install -y grpc-devel protobuf-devel rocksdb-devel cmake gcc-c++
```

### Arch Linux
```bash
sudo pacman -S grpc protobuf rocksdb cmake base-devel
```

## Building

### From main directory
```bash
make build        # Debug build
make build-release # Release build
```

### From cpp directory
```bash
make debug        # Debug build
make release      # Release build
make install-deps # Install dependencies (Ubuntu/Debian)
```

## Running

After building, the executable will be available as:
- Debug: `bin/rocksdbserver-cpp` (from main directory) or `cpp/build/server`
- Release: Same locations

The server listens on `0.0.0.0:50051` by default and stores data in `./data/rocksdb-cpp/`.

## Implementation Details

- **Storage**: RocksDB TransactionDB for ACID transactions
- **Concurrency**: Custom semaphore implementation to limit concurrent operations
- **Error Handling**: Comprehensive error handling with appropriate gRPC status codes
- **Memory Management**: Modern C++ with RAII and smart pointers
- **Build System**: CMake with automatic protobuf/gRPC code generation

## Performance Characteristics

- Optimized for high throughput with configurable concurrency limits
- Efficient iterator-based key listing with prefix support
- Low-latency ping endpoint for health checks
- Minimal memory allocations in hot paths
