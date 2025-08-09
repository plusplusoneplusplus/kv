# RocksDB Service Benchmark Tool (Rust)

This is a Rust port of the Go benchmark tool for testing the performance of RocksDB service implementations across different protocols (gRPC, Thrift, and raw RocksDB).

## Features

- **Multi-protocol support**: gRPC, Thrift, and raw RocksDB benchmarking
- **Multiple benchmark modes**: ping, read, write, and mixed workloads
- **Configurable concurrency**: Adjustable number of worker threads
- **Comprehensive statistics**: Latency percentiles, throughput, and success rates
- **JSON output**: Machine-readable results for further analysis
- **Pre-population**: Automatic key pre-population for read benchmarks

## Building

```bash
cargo build --release
```

## Usage Examples

Basic mixed workload:
```bash
./target/release/benchmark --mode=mixed --requests=100000
```

gRPC ping benchmark:
```bash
./target/release/benchmark --protocol=grpc --mode=ping --requests=10000 --threads=16
```

Write-only benchmark with custom parameters:
```bash
./target/release/benchmark --mode=write --requests=25000 --threads=16 --value-size=1024
```

JSON output for analysis:
```bash
./target/release/benchmark --mode=mixed --requests=50000 --json=results.json
```

Raw RocksDB benchmarking (no network overhead):
```bash
./target/release/benchmark --protocol=raw --mode=write --requests=100000 --threads=16
```

## Command Line Options

- `--addr`: Server address (default: localhost:50051 for gRPC, localhost:9090 for Thrift)
- `--mode`: Benchmark mode (ping, read, write, mixed)
- `--threads`: Number of concurrent threads
- `--requests`: Total number of requests
- `--write-pct`: Write percentage for mixed mode
- `--ping-pct`: Ping percentage for mixed mode  
- `--key-size`: Key size in bytes
- `--value-size`: Value size in bytes
- `--timeout`: Timeout for operations in seconds
- `--protocol`: Protocol to use (grpc, thrift, raw)
- `--prepopulate`: Number of keys to pre-populate
- `--json`: Output results to JSON file
- `--no-cache-read`: Don't fill block cache on reads
- `--config`: Path to config file for raw mode

## Protocol Support

- **gRPC**: Full implementation using tonic
- **Thrift**: Placeholder implementation (requires Thrift code generation)
- **Raw**: Direct RocksDB access with transaction support

## Implementation Notes

This Rust version maintains feature parity with the original Go benchmark tool while leveraging Rust's async/await capabilities and zero-cost abstractions for optimal performance.

The Thrift client is currently a placeholder and would need proper Thrift code generation to be fully functional.