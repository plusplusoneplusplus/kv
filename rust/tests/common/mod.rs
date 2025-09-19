pub mod test_cluster;

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

#[allow(dead_code)]
pub struct ThriftTestServer {
    child: Option<Child>,
    pub port: Option<u16>,
    config_dir: tempfile::TempDir,
}

impl ThriftTestServer {
    #[allow(dead_code)]
    pub async fn new() -> Self {
        // Create temporary directory for test configuration
        let config_dir = tempfile::tempdir().expect("Failed to create temp dir");
        
        // Create test configuration file
        let config_content = r#"
[database]
db_path = "./data/rocksdb-test"
max_read_concurrency = 32
max_write_concurrency = 16

[cache]
lru_cache_size = "256MB"

[compaction]
level_zero_file_num_compaction_trigger = 4
level_zero_slowdown_writes_trigger = 20
level_zero_stop_writes_trigger = 36
target_file_size_base = "64MB"
max_bytes_for_level_base = "256MB"
max_bytes_for_level_multiplier = 10
compression_type = "LZ4"
bottommost_compression_type = "ZSTD"
compaction_style = "Level"
compaction_priority = "ByCompensatedSize"

[write_buffer]
write_buffer_size = "64MB"
max_write_buffer_number = 3
min_write_buffer_number_to_merge = 1

[block_cache]
cache_size = "128MB"
num_shard_bits = 6
strict_capacity_limit = false
high_pri_pool_ratio = 0.5

[read_options]
verify_checksums = true
fill_cache = true

[write_options]
sync = false
disable_wal = false

[bloom_filter]
bits_per_key = 10
block_based = true
"#;
        
        let config_path = config_dir.path().join("db_config.toml");
        std::fs::write(&config_path, config_content).expect("Failed to write config file");
        
        Self {
            child: None,
            port: None,
            config_dir,
        }
    }
    
    #[allow(dead_code)]
    pub async fn start(&mut self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        // Find available port
        let port = find_available_port()?;
        self.port = Some(port);
        
        // Get the absolute path to the debug thrift server binary
        let current_dir = std::env::current_dir()?;
        let server_path = current_dir.join("target").join("debug").join("thrift-server");
        let _config_path = self.config_dir.path().join("db_config.toml");
        
        // Start server process from the config directory so it finds the config file
        let mut cmd = Command::new(server_path);
        cmd.env("THRIFT_PORT", port.to_string())
           .env("RUST_LOG", "info")
           .current_dir(self.config_dir.path());
        
        let child = cmd.spawn()?;
        self.child = Some(child);
        
        // Wait for server to start up
        let mut attempts = 0;
        let max_attempts = 50; // 5 seconds with 100ms intervals
        
        while attempts < max_attempts {
            if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(100));
            attempts += 1;
        }
        
        if attempts >= max_attempts {
            return Err("Server failed to start within timeout".into());
        }
        
        Ok(port)
    }
    
    #[allow(dead_code)]
    pub async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ThriftTestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[allow(dead_code)]
fn find_available_port() -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
    // Try to bind to port 0 to get an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener); // Release the port
    Ok(port)
}

#[allow(dead_code)]
pub async fn wait_for_server_ready(port: u16, timeout_secs: u64) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let timeout = Duration::from_secs(timeout_secs);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err("Server did not become ready within timeout".into())
}

/// Get server address from environment or default to localhost:9090
#[allow(dead_code)]
pub fn get_server_address() -> String {
    // Check for full server address first
    if let Ok(addr) = std::env::var("KV_TEST_SERVER_ADDRESS") {
        return addr;
    }

    // Check for port only and build address
    if let Ok(port) = std::env::var("KV_TEST_SERVER_PORT") {
        return format!("localhost:{}", port);
    }

    // Default fallback
    "localhost:9090".to_string()
}