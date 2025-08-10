# Benchmark Scripts

Quick reference for running performance benchmarks on the KV store.

## Scripts

### `run_cold_raw.sh`
Comprehensive cold page benchmark suite with multiple configurations.
```bash
./scripts/run_cold_raw.sh        # Full suite: 3 dataset sizes × 5 thread configs × 3 workload ratios
./scripts/run_cold_raw.sh --min  # Quick test: 2GB dataset, 4 threads, 90:10 mixed
```
**Features:** Dataset reuse, OS cache clearing, iteration averaging, HTML reports  
**Latency:** 50-1000μs (true disk I/O)

## Manual Commands

### Hot Page Reads (cached)
```bash
./bin/benchmark-rust --protocol=raw --mode=read --requests=10000 --prepopulate=10000
```

### Cold Page Reads (uncached)  
```bash
./bin/benchmark-rust --protocol=raw --mode=read --requests=5000 --prepopulate=262144 --no-cache-read --config=./configs/db/cold_block_cache.toml
```

### Mixed Workloads
```bash
./bin/benchmark-rust --mode=mixed --write-pct=10 --requests=50000
```

## Key Parameters

- `--no-cache-read`: Bypass RocksDB block cache
- `--config=./configs/db/cold_block_cache.toml`: Tiny 1MB cache
- `--prepopulate=N`: Create N keys for reads (larger = colder)
- `--value-size=8192`: Use 8KB values for large datasets

## Expected Latency

| Access Pattern | Latency | Description |
|----------------|---------|-------------|
| Hot cache | <1μs | All data in cache |
| RocksDB cache miss | 1-50μs | OS page cache hit |  
| True cold (SSD) | 100-1000μs | Actual disk I/O |
| True cold (HDD) | 5-20ms | Mechanical disk |