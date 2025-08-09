# RocksDB Service Benchmark Tool

This directory contains a comprehensive benchmark tool for testing the RocksDB service implementation.

## Building

### From the root directory
```bash
# Build all components including benchmark
make build
```

### From the benchmark directory
```bash
# Build just the benchmark tool
cd benchmark
go build -o ../bin/benchmark benchmark.go
```

## Running

The benchmark tool supports several modes of operation:

### Basic Usage
```bash
# From the root directory
./bin/benchmark [options]

# Or run directly from benchmark directory
cd benchmark
go run benchmark.go [options]
```

### Benchmark Modes

1. **Ping-only benchmark** - Tests basic connectivity and latency:
   ```bash
   ./bin/benchmark -mode=ping -requests=10000 -threads=16
   ```

2. **Read-only benchmark** - Tests read performance:
   ```bash
   ./bin/benchmark -mode=read -requests=50000 -threads=32 -key-size=20
   ```

3. **Write-only benchmark** - Tests write performance:
   ```bash
   ./bin/benchmark -mode=write -requests=25000 -threads=16 -value-size=1024
   ```

4. **Mixed workload** (default) - Tests realistic read/write patterns:
   ```bash
   ./bin/benchmark -mode=mixed -write-pct=30 -requests=100000
   # Creates: 30% writes, 70% reads (ping-pct defaults to 0)
   ```

5. **Mixed workload with pings**:
   ```bash
   ./bin/benchmark -mode=mixed -write-pct=30 -ping-pct=10 -requests=100000
   # Creates: 30% writes, 10% pings, 60% reads
   ```

6. **Custom server address**:
   ```bash
   ./bin/benchmark -addr=192.168.1.100:50051 -mode=ping
   ```

### Available Options

- `-addr`: Server address (default: "localhost:50051")
- `-mode`: Benchmark mode: ping, read, write, mixed (default: "mixed")
- `-threads`: Number of concurrent threads (default: 32)
- `-requests`: Total number of requests (default: 100000)
- `-write-pct`: Percentage of write operations (0-100, mixed mode only, default: 30)
- `-ping-pct`: Percentage of ping operations (0-100, mixed mode only, default: 0)
- `-key-size`: Size of keys in bytes (default: 16)
- `-value-size`: Size of values in bytes (default: 100)
- `-timeout`: Timeout for individual operations (default: 30s)
- `-no-cache-read`: Do not fill RocksDB block cache on reads (raw mode); useful for colder read testing

## Output

The benchmark tool provides detailed statistics including:

- **Operations Summary**: Total, successful, and failed operations with percentages
- **Throughput**: Operations per second
- **Latency Statistics**: Min, average, and percentile latencies (P50, P90, P95, P99, Max)
- **Per-operation Breakdown**: Separate statistics for read, write, and ping operations

## Dependencies

The benchmark tool requires:
- The main RocksDB service protobuf definitions (`rocksdb_svc/go/proto`)
- gRPC client libraries
- Go 1.18+

## Notes

- The benchmark automatically validates ping responses to ensure proper round-trip communication
- Progress is reported every 10,000 completed requests
- Results are collected and analyzed after all workers complete
- The tool uses persistent gRPC connections per worker for realistic testing

## DB Profiles

Predefined RocksDB configs live in `configs/db/`:

- `default.toml` – balanced defaults
- `cold_block_cache.toml` – tiny cache, small blocks, index/filter not cached (simulate cold)
- `warm_large_cache.toml` – large cache, larger blocks (simulate warm)

Use a specific profile with the benchmark (raw mode):

```bash
./bin/benchmark -protocol=raw -config=./configs/db/default.toml ...
./bin/benchmark -protocol=raw -config=./configs/db/cold_block_cache.toml ...
./bin/benchmark -protocol=raw -config=./configs/db/warm_large_cache.toml ...
```

Tip: For colder reads, combine with the flag:

- `-no-cache-read` – do not fill RocksDB block cache on reads (raw mode)
