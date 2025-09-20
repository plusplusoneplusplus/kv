use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};
use std::{env, fs};

fn wait_for_port(addr: &str, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if TcpStream::connect(addr).is_ok() {
            return true;
        }
        sleep(Duration::from_millis(50));
    }
    false
}

fn find_free_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn spawn_thrift_server(port: u16) -> Option<Child> {
    // Prefer an explicit path from env for CI, fallback to relative debug path
    let server_bin = env::var("THRIFT_SERVER_BIN")
        .unwrap_or_else(|_| "../rust/target/debug/thrift-server".to_string());
    if !std::path::Path::new(&server_bin).exists() {
        eprintln!(
            "thrift-server binary not found at {} — skipping smoke test",
            server_bin
        );
        return None;
    }

    let mut cmd = Command::new(server_bin);
    cmd.env("THRIFT_PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    match cmd.spawn() {
        Ok(child) => Some(child),
        Err(e) => {
            eprintln!("failed to spawn thrift-server: {}", e);
            None
        }
    }
}

#[test]
fn thrift_ping_benchmark_smoke() {
    // Pick a free port and start the server
    let port = find_free_port();
    let addr = format!("127.0.0.1:{}", port);
    let maybe_child = spawn_thrift_server(port);
    if maybe_child.is_none() {
        // Skip test gracefully if we cannot start the server (e.g. local dev)
        eprintln!("Skipping thrift ping benchmark smoke test");
        return;
    }
    let mut child = maybe_child.unwrap();

    // Wait for server to be ready
    assert!(
        wait_for_port(&addr, Duration::from_secs(10)),
        "Thrift server did not become ready on {}",
        addr
    );

    // Resolve benchmark binary path via Cargo’s test-provided env var
    let bench_bin = env::var("CARGO_BIN_EXE_benchmark").expect("CARGO_BIN_EXE_benchmark not set");

    // Run a tiny ping benchmark and write JSON report to a temp file
    let json_path = env::temp_dir().join(format!("thrift_ping_report_{}.json", port));
    let status = Command::new(bench_bin)
        .arg("--protocol=thrift")
        .arg(format!("--addr={}", addr))
        .arg("--mode=ping")
        .arg("--threads=4")
        .arg("--requests=100")
        .arg("--timeout=10")
        .arg(format!("--json={}", json_path.display()))
        .status()
        .expect("failed to run benchmark binary");
    assert!(status.success(), "benchmark process failed");

    // Basic sanity: JSON file exists and is non-empty
    let data = fs::read_to_string(&json_path).expect("failed to read json output");
    assert!(!data.is_empty(), "json output is empty");

    // Cleanup server
    let _ = child.kill();
    let _ = child.wait();
}
