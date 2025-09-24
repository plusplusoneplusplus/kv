use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../../thrift/kvstore.thrift");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let thrift_file = PathBuf::from("../../../thrift/kvstore.thrift");

    // Generate Rust thrift files
    let output = Command::new("thrift")
        .args(&["--gen", "rs", "-out", out_dir.to_str().unwrap()])
        .arg(&thrift_file)
        .output()
        .expect("Failed to execute thrift compiler. Make sure 'thrift' is installed and in PATH.");

    if !output.status.success() {
        panic!(
            "thrift compilation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    // Tell cargo where to find the generated file
    println!("cargo:rustc-env=THRIFT_OUT_DIR={}", out_dir.display());
}