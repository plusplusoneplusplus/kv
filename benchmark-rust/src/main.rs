use anyhow::Result;
use clap::Parser;
use futures::future::join_all;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

mod clients;
mod config;

use clients::{ClientFactory, KvOperations};

#[derive(Parser, Debug)]
#[command(name = "benchmark")]
#[command(about = "RocksDB Service Benchmark Tool", long_about = None)]
struct Args {
    #[arg(long, help = "server address (defaults: grpc=localhost:50051, thrift=localhost:9090)")]
    addr: Option<String>,

    #[arg(long, default_value = "mixed", help = "benchmark mode: ping, read, write, mixed")]
    mode: String,

    #[arg(long, default_value = "32", help = "number of concurrent threads")]
    threads: usize,

    #[arg(long, default_value = "100000", help = "total number of requests")]
    requests: i64,

    #[arg(long, default_value = "30", help = "percentage of write operations (0-100) - only used in mixed mode", name = "write-pct")]
    write_pct: i32,

    #[arg(long, default_value = "0", help = "percentage of ping operations (0-100) - only used in mixed mode", name = "ping-pct")]
    ping_pct: i32,

    #[arg(long, default_value = "16", help = "size of keys in bytes", name = "key-size")]
    key_size: usize,

    #[arg(long, default_value = "100", help = "size of values in bytes", name = "value-size")]
    value_size: usize,

    #[arg(long, default_value = "30", help = "timeout for individual operations (seconds)")]
    timeout: u64,

    #[arg(long, default_value = "grpc", help = "protocol to use: grpc, thrift, raw")]
    protocol: String,

    #[arg(long, default_value = "10000", help = "number of keys to pre-populate for read operations")]
    prepopulate: i32,

    #[arg(long, help = "output results to JSON file (e.g., results.json)")]
    json: Option<String>,

    #[arg(long, help = "do not fill block cache on reads (raw mode)", name = "no-cache-read")]
    no_cache_read: bool,

    #[arg(long, help = "path to config file (for raw protocol)")]
    config: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub server_addr: String,
    pub mode: String,
    pub num_threads: usize,
    pub total_requests: i64,
    pub write_percentage: i32,
    pub ping_percentage: i32,
    pub key_size: usize,
    pub value_size: usize,
    pub timeout: Duration,
    pub protocol: String,
    pub prepopulate_keys: i32,
    #[serde(skip)]
    pub prepopulated_keys: Vec<String>,
    #[serde(skip)]
    pub json_output_file: Option<String>,
    pub no_cache_read: bool,
    #[serde(skip)]
    pub config_file: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub operation: String,
    pub latency: Duration,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Statistics {
    pub operation: String,
    pub count: i64,
    pub success: i64,
    pub failed: i64,
    pub min_latency: Duration,
    pub max_latency: Duration,
    pub avg_latency: Duration,
    pub p50_latency: Duration,
    pub p90_latency: Duration,
    pub p95_latency: Duration,
    pub p99_latency: Duration,
    pub throughput: f64,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkReport {
    pub config: BenchmarkConfig,
    pub start_time: SystemTime,
    pub end_time: SystemTime,
    pub total_duration: Duration,
    pub statistics: Vec<Statistics>,
}

struct Worker {
    id: usize,
    kv_ops: Arc<dyn KvOperations>,
    config: Arc<BenchmarkConfig>,
    results_tx: mpsc::UnboundedSender<BenchmarkResult>,
    counter: Arc<AtomicI64>,
}

impl Worker {
    async fn run(&self) -> Result<()> {
        loop {
            let current = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
            if current > self.config.total_requests {
                break;
            }

            let result = match self.config.mode.as_str() {
                "ping" => self.kv_ops.ping(self.id).await,
                "read" => {
                    let key = self.get_read_key();
                    self.kv_ops.get(&key).await
                }
                "write" => {
                    let key = generate_random_string(self.config.key_size);
                    let value = generate_random_string(self.config.value_size);
                    self.kv_ops.put(&key, &value).await
                }
                "mixed" => {
                    let remainder = current % 100;
                    
                    if remainder < self.config.write_percentage as i64 {
                        let key = generate_random_string(self.config.key_size);
                        let value = generate_random_string(self.config.value_size);
                        self.kv_ops.put(&key, &value).await
                    } else if remainder < (self.config.write_percentage + self.config.ping_percentage) as i64 {
                        self.kv_ops.ping(self.id).await
                    } else {
                        let key = self.get_read_key();
                        self.kv_ops.get(&key).await
                    }
                }
                _ => unreachable!(),
            };

            if let Err(_) = self.results_tx.send(result) {
                break;
            }

            if current % 10000 == 0 {
                println!("Completed {} requests...", current);
            }
        }

        Ok(())
    }

    fn get_read_key(&self) -> String {
        if !self.config.prepopulated_keys.is_empty() {
            let index = thread_rng().gen_range(0..self.config.prepopulated_keys.len());
            self.config.prepopulated_keys[index].clone()
        } else {
            generate_random_string(self.config.key_size)
        }
    }
}

fn generate_random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

async fn pre_populate_database(
    client_factory: &dyn ClientFactory,
    config: &mut BenchmarkConfig,
) -> Result<()> {
    println!("Pre-populating database with {} keys...", config.prepopulate_keys);
    
    let kv_client = client_factory.create_client(&config.server_addr, config).await?;
    
    for i in 0..config.prepopulate_keys {
        let key_prefix = format!("benchmark_key_{:08}", i);
        let remaining_key_size = config.key_size.saturating_sub(key_prefix.len());
        
        let key = if remaining_key_size > 0 {
            let random_suffix = generate_random_string(remaining_key_size);
            format!("{}{}", key_prefix, random_suffix)
        } else {
            key_prefix.chars().take(config.key_size).collect()
        };
        
        let value = generate_random_string(config.value_size);
        
        let result = kv_client.put(&key, &value).await;
        if !result.success {
            return Err(anyhow::anyhow!("Failed to pre-populate key {}: {:?}", key, result.error));
        }
        
        config.prepopulated_keys.push(key);
        
        if (i + 1) % 1000 == 0 {
            println!("Pre-populated {}/{} keys...", i + 1, config.prepopulate_keys);
        }
    }
    
    println!("Pre-population completed.\n");
    Ok(())
}

async fn run_benchmark(config: BenchmarkConfig) -> Result<()> {
    let start_time = SystemTime::now();
    
    let client_factory: Box<dyn ClientFactory> = match config.protocol.as_str() {
        "grpc" => Box::new(clients::grpc::GrpcClientFactory),
        "thrift" => Box::new(clients::thrift::ThriftClientFactory),
        "raw" => Box::new(clients::raw::RawClientFactory::new()),
        _ => return Err(anyhow::anyhow!("Unsupported protocol: {}", config.protocol)),
    };

    let mut config = config;
    
    let needs_pre_population = config.mode == "read" || 
        (config.mode == "mixed" && 100 - config.write_percentage - config.ping_percentage > 0);
    
    if needs_pre_population && config.prepopulate_keys > 0 {
        pre_populate_database(client_factory.as_ref(), &mut config).await?;
    }

    let (results_tx, mut results_rx) = mpsc::unbounded_channel();
    let counter = Arc::new(AtomicI64::new(0));
    let config = Arc::new(config);

    println!("Starting {} workers...", config.num_threads);
    let benchmark_start_time = Instant::now();

    let mut tasks = Vec::new();
    for worker_id in 0..config.num_threads {
        let kv_client = client_factory.create_client(&config.server_addr, &config).await?;
        let worker = Worker {
            id: worker_id,
            kv_ops: kv_client,
            config: config.clone(),
            results_tx: results_tx.clone(),
            counter: counter.clone(),
        };

        let task = tokio::spawn(async move {
            worker.run().await
        });
        tasks.push(task);
    }

    drop(results_tx);

    let mut all_results = Vec::new();
    let results_task = tokio::spawn(async move {
        while let Some(result) = results_rx.recv().await {
            all_results.push(result);
        }
        all_results
    });

    join_all(tasks).await;
    let all_results = results_task.await?;

    let benchmark_end_time = Instant::now();
    let total_duration = benchmark_end_time.duration_since(benchmark_start_time);
    let end_time = SystemTime::now();

    println!("Benchmark completed in {:?}\n", total_duration);

    analyze_results(all_results, total_duration, &config, start_time, end_time);

    Ok(())
}

fn analyze_results(
    results: Vec<BenchmarkResult>,
    total_duration: Duration,
    config: &BenchmarkConfig,
    start_time: SystemTime,
    end_time: SystemTime,
) {
    let mut read_results = Vec::new();
    let mut write_results = Vec::new();
    let mut ping_results = Vec::new();

    for result in &results {
        match result.operation.as_str() {
            "read" => read_results.push(result),
            "write" => write_results.push(result),
            "ping" => ping_results.push(result),
            _ => {}
        }
    }

    let mut all_stats = Vec::new();

    if !read_results.is_empty() {
        let read_stats = calculate_statistics("READ", &read_results, total_duration);
        all_stats.push(read_stats);
    }

    if !write_results.is_empty() {
        let write_stats = calculate_statistics("WRITE", &write_results, total_duration);
        all_stats.push(write_stats);
    }

    if !ping_results.is_empty() {
        let ping_stats = calculate_statistics("PING", &ping_results, total_duration);
        all_stats.push(ping_stats);
    }

    let overall_stats = if !results.is_empty() {
        Some(calculate_statistics("OVERALL", &results.iter().collect::<Vec<_>>(), total_duration))
    } else {
        None
    };

    if let Some(ref overall) = overall_stats {
        all_stats.push(overall.clone());
    }

    if let Some(json_file) = &config.json_output_file {
        let report = BenchmarkReport {
            config: config.clone(),
            start_time,
            end_time,
            total_duration,
            statistics: all_stats,
        };

        if let Err(e) = write_json_results(&report, json_file) {
            eprintln!("Failed to write JSON results: {}", e);
        } else {
            println!("Results written to {}", json_file);
        }

        println!("=== BENCHMARK SUMMARY ===");
        println!("Total Duration: {:?}", total_duration);
        println!("Total Requests: {}", results.len());
        if let Some(overall) = &overall_stats {
            println!("Overall Throughput: {:.2} ops/sec", overall.throughput);
            println!("Success Rate: {:.2}%", (overall.success as f64 / overall.count as f64) * 100.0);
        }
        println!("Detailed results saved to: {}", json_file);
    } else {
        println!("=== BENCHMARK RESULTS ===");
        println!("Total Duration: {:?}", total_duration);
        println!("Total Requests: {}", results.len());
        println!();

        for stats in &all_stats {
            print_statistics(stats);
        }
    }
}

fn write_json_results(report: &BenchmarkReport, filename: &str) -> Result<()> {
    let json = serde_json::to_string_pretty(report)?;
    std::fs::write(filename, json)?;
    Ok(())
}

fn calculate_statistics(operation: &str, results: &[&BenchmarkResult], total_duration: Duration) -> Statistics {
    if results.is_empty() {
        return Statistics {
            operation: operation.to_string(),
            count: 0,
            success: 0,
            failed: 0,
            min_latency: Duration::ZERO,
            max_latency: Duration::ZERO,
            avg_latency: Duration::ZERO,
            p50_latency: Duration::ZERO,
            p90_latency: Duration::ZERO,
            p95_latency: Duration::ZERO,
            p99_latency: Duration::ZERO,
            throughput: 0.0,
        };
    }

    let mut latencies: Vec<Duration> = results.iter().map(|r| r.latency).collect();
    latencies.sort();

    let total_latency: Duration = latencies.iter().sum();
    let success_count = results.iter().filter(|r| r.success).count() as i64;
    let failed_count = results.len() as i64 - success_count;

    Statistics {
        operation: operation.to_string(),
        count: results.len() as i64,
        success: success_count,
        failed: failed_count,
        min_latency: *latencies.first().unwrap(),
        max_latency: *latencies.last().unwrap(),
        avg_latency: total_latency / latencies.len() as u32,
        p50_latency: percentile(&latencies, 50),
        p90_latency: percentile(&latencies, 90),
        p95_latency: percentile(&latencies, 95),
        p99_latency: percentile(&latencies, 99),
        throughput: results.len() as f64 / total_duration.as_secs_f64(),
    }
}

fn percentile(latencies: &[Duration], p: u8) -> Duration {
    if latencies.is_empty() {
        return Duration::ZERO;
    }

    let index = ((p as f64 / 100.0 * latencies.len() as f64).ceil() as usize).saturating_sub(1);
    let index = index.min(latencies.len() - 1);
    latencies[index]
}

fn print_statistics(stats: &Statistics) {
    println!("=== {} STATISTICS ===", stats.operation);
    
    println!("┌─────────────────────┬─────────────┬──────────────┬──────────────┐");
    println!("│ {:19} │ {:11} │ {:12} │ {:12} │", "Metric", "Total", "Successful", "Failed");
    println!("├─────────────────────┼─────────────┼──────────────┼──────────────┤");
    println!("│ {:19} │ {:11} │ {:12} │ {:12} │", "Operations", stats.count, stats.success, stats.failed);
    println!("│ {:19} │ {:11} │ {:11.2}% │ {:11.2}% │", "Percentage", "-", 
             (stats.success as f64 / stats.count as f64) * 100.0, 
             (stats.failed as f64 / stats.count as f64) * 100.0);
    println!("│ {:19} │ {:11.2} │ {:12} │ {:12} │", "Throughput (op/s)", stats.throughput, "-", "-");
    println!("└─────────────────────┴─────────────┴──────────────┴──────────────┘");
    println!();

    println!("┌─────────────────────┬─────────────────┐");
    println!("│ {:19} │ {:15} │", "Latency Metric", "Value");
    println!("├─────────────────────┼─────────────────┤");
    println!("│ {:19} │ {:15?} │", "Minimum", stats.min_latency);
    println!("│ {:19} │ {:15?} │", "Average", stats.avg_latency);
    println!("│ {:19} │ {:15?} │", "50th Percentile", stats.p50_latency);
    println!("│ {:19} │ {:15?} │", "90th Percentile", stats.p90_latency);
    println!("│ {:19} │ {:15?} │", "95th Percentile", stats.p95_latency);
    println!("│ {:19} │ {:15?} │", "99th Percentile", stats.p99_latency);
    println!("│ {:19} │ {:15?} │", "Maximum", stats.max_latency);
    println!("└─────────────────────┴─────────────────┘");
    println!();
}

fn show_usage_examples() {
    println!("RocksDB Service Benchmark Tool");
    println!();
    println!("Usage Examples:");
    println!();
    println!("1. Ping-only benchmark with gRPC:");
    println!("   ./benchmark --protocol=grpc --mode=ping --requests=10000 --threads=16");
    println!();
    println!("2. Read-only benchmark with Thrift:");
    println!("   ./benchmark --protocol=thrift --mode=read --requests=50000 --threads=32 --key-size=20");
    println!();
    println!("3. Write-only benchmark (default gRPC):");
    println!("   ./benchmark --mode=write --requests=25000 --threads=16 --value-size=1024");
    println!();
    println!("4. Mixed workload (default):");
    println!("   ./benchmark --mode=mixed --write-pct=30 --requests=100000");
    println!("   # This creates: 30% writes, 70% reads (ping-pct defaults to 0)");
    println!();
    println!("5. Mixed workload with pings:");
    println!("   ./benchmark --mode=mixed --write-pct=30 --ping-pct=10 --requests=100000");
    println!("   # This creates: 30% writes, 10% pings, 60% reads");
    println!();
    println!("6. Custom server address with protocol:");
    println!("   ./benchmark --protocol=grpc --addr=192.168.1.100:50051 --mode=ping");
    println!("   ./benchmark --protocol=thrift --addr=192.168.1.100:9090 --mode=ping");
    println!();
    println!("7. Using default addresses:");
    println!("   ./benchmark --protocol=grpc --mode=ping    # uses localhost:50051");
    println!("   ./benchmark --protocol=thrift --mode=ping  # uses localhost:9090");
    println!("   ./benchmark --protocol=raw --mode=ping     # uses ./data/rocksdb-benchmark");
    println!();
    println!("8. Raw RocksDB benchmarking (no network overhead):");
    println!("   ./benchmark --protocol=raw --mode=write --requests=100000 --threads=16");
    println!("   ./benchmark --protocol=raw --mode=mixed --write-pct=30 --requests=50000");
    println!("   ./benchmark --protocol=raw --addr=/tmp/my-benchmark-db --mode=read");
    println!();
    println!("9. JSON output for further processing:");
    println!("   ./benchmark --mode=mixed --requests=50000 --json=results.json");
    println!("   ./benchmark --protocol=thrift --mode=ping --json=/tmp/ping-results.json");
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if std::env::args().any(|arg| arg == "-h" || arg == "--help") {
        show_usage_examples();
        return Ok(());
    }

    let valid_modes = ["ping", "read", "write", "mixed"];
    if !valid_modes.contains(&args.mode.as_str()) {
        return Err(anyhow::anyhow!("Mode must be one of: ping, read, write, mixed"));
    }

    let valid_protocols = ["grpc", "thrift", "raw"];
    if !valid_protocols.contains(&args.protocol.as_str()) {
        return Err(anyhow::anyhow!("Protocol must be one of: grpc, thrift, raw"));
    }

    if args.mode == "mixed" {
        if args.write_pct < 0 || args.write_pct > 100 {
            return Err(anyhow::anyhow!("Write percentage must be between 0 and 100"));
        }
        if args.ping_pct < 0 || args.ping_pct > 100 {
            return Err(anyhow::anyhow!("Ping percentage must be between 0 and 100"));
        }
        if args.write_pct + args.ping_pct > 100 {
            return Err(anyhow::anyhow!("Write percentage + Ping percentage cannot exceed 100"));
        }
    }

    let server_addr = args.addr.unwrap_or_else(|| {
        match args.protocol.as_str() {
            "grpc" => "localhost:50051".to_string(),
            "thrift" => "localhost:9090".to_string(),
            "raw" => "./data/rocksdb-benchmark".to_string(),
            _ => "localhost:50051".to_string(),
        }
    });

    let config = BenchmarkConfig {
        server_addr: server_addr.clone(),
        mode: args.mode.clone(),
        num_threads: args.threads,
        total_requests: args.requests,
        write_percentage: args.write_pct,
        ping_percentage: args.ping_pct,
        key_size: args.key_size,
        value_size: args.value_size,
        timeout: Duration::from_secs(args.timeout),
        protocol: args.protocol.clone(),
        prepopulate_keys: args.prepopulate,
        prepopulated_keys: Vec::new(),
        json_output_file: args.json.clone(),
        no_cache_read: args.no_cache_read,
        config_file: args.config.clone(),
    };

    println!("=== RocksDB Service Benchmark ===");
    println!("Server: {}", config.server_addr);
    println!("Protocol: {}", config.protocol);
    println!("Mode: {}", config.mode);
    println!("Threads: {}", config.num_threads);
    println!("Total Requests: {}", config.total_requests);

    if config.mode == "mixed" {
        println!("Write Percentage: {}%", config.write_percentage);
        println!("Ping Percentage: {}%", config.ping_percentage);
        println!("Read Percentage: {}%", 100 - config.write_percentage - config.ping_percentage);
    }

    if config.mode == "read" || config.mode == "write" || config.mode == "mixed" {
        println!("Key Size: {} bytes", config.key_size);
        println!("Value Size: {} bytes", config.value_size);
        if config.mode == "read" || (config.mode == "mixed" && 100 - config.write_percentage - config.ping_percentage > 0) {
            println!("Pre-populate Keys: {}", config.prepopulate_keys);
        }
    }

    println!("Timeout: {:?}", config.timeout);
    println!();

    run_benchmark(config).await?;

    Ok(())
}