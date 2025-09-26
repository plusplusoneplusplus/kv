use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::io::{BufRead, BufReader, Write};

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

    // Post-process the generated file to remove inner attributes and imports
    let generated_file = out_dir.join("kvstore.rs");
    let processed_file = out_dir.join("kvstore_processed.rs");

    let input = fs::File::open(&generated_file).expect("Failed to open generated file");
    let mut output_file = fs::File::create(&processed_file).expect("Failed to create processed file");

    let reader = BufReader::new(input);
    let mut skip_count = 0;

    for line in reader.lines() {
        let line = line.expect("Failed to read line");

        // Skip the first 30 lines which contain the problematic imports and attributes
        if skip_count < 30 {
            skip_count += 1;
            continue;
        }

        writeln!(output_file, "{}", line).expect("Failed to write line");
    }

    // Tell cargo where to find the processed file
    println!("cargo:rustc-env=THRIFT_PROCESSED_FILE={}", processed_file.display());
}