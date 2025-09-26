# Test Configuration System

This directory contains shared test utilities and configuration for the KV store test suite.

## Binary Path Management

The `test_config.rs` module provides centralized management of server binary paths used in tests. This eliminates hardcoded paths scattered throughout test files.

### Features

- **Auto-detection**: Automatically finds binaries based on build system used
- **Multiple fallback strategies**: Tries CMake build, Cargo debug, Cargo release locations
- **Environment variable overrides**: Support custom binary paths via env vars
- **Validation**: Checks binary existence before running tests
- **Error messaging**: Clear instructions when binaries are missing

### Usage

```rust
use common::test_config;

// Get binary paths
let thrift_server = test_config::thrift_server_binary();
let grpc_server = test_config::grpc_server_binary();

// Validate binaries exist
test_config::TestConfig::global().validate_thrift_server()?;
```

### Environment Variables

Override binary locations using environment variables:

```bash
# Override Thrift server binary
export THRIFT_SERVER_BINARY=/path/to/custom/thrift-server

# Override gRPC server binary
export GRPC_SERVER_BINARY=/path/to/custom/grpc-server

# Run tests
cargo test
```

### Binary Detection Priority

The system searches for binaries in this order:

1. **Environment variable** (THRIFT_SERVER_BINARY / GRPC_SERVER_BINARY)
2. **CMake build directory** (build/bin/rocksdbserver-thrift, build/bin/rocksdbserver-rust)
3. **Cargo debug build** (target/debug/thrift-server, target/debug/server)
4. **Cargo release build** (target/release/thrift-server, target/release/server)
5. **Relative paths** (for cross-directory compatibility)

### Testing the Configuration

```bash
# Test binary detection
cargo test test_config_detection -- --nocapture

# This will show detected paths and validation status
```

## Migration from Hardcoded Paths

Old approach (scattered throughout tests):
```rust
Command::new("./target/debug/thrift-server")  // ❌ Hardcoded
```

New approach (centralized):
```rust
use common::test_config;

Command::new(test_config::thrift_server_binary())  // ✅ Configurable
```

This provides:
- Better maintainability
- Support for different build environments
- Clear error messages when binaries are missing
- Flexibility for CI/CD systems