use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Inform rustc that `cfg(disabled_test)` is an expected conditional.
    // This silences the `unexpected_cfgs` lint while keeping tests disabled.
    println!("cargo:rustc-check-cfg=cfg(disabled_test)");

    let proto_file = "../proto/kvstore.proto";
    let out_dir = std::env::var("OUT_DIR")?;
    let generated_file = format!("{}/kvstore.rs", out_dir);

    // Only compile protos if source is newer than generated file or if generated file doesn't exist
    if should_regenerate_proto(proto_file, &generated_file) {
        tonic_build::compile_protos(proto_file)?;
    }

    // Watch for changes to source files only
    println!("cargo:rerun-if-changed={}", proto_file);
    println!("cargo:rerun-if-changed=../thrift/kvstore.thrift");

    Ok(())
}

fn should_regenerate_proto(proto_file: &str, generated_file: &str) -> bool {
    let proto_path = Path::new(proto_file);
    let generated_path = Path::new(generated_file);

    // Regenerate if proto file doesn't exist (error case) or generated file doesn't exist
    if !proto_path.exists() || !generated_path.exists() {
        return true;
    }

    // Regenerate if proto file is newer than generated file
    if let (Ok(proto_meta), Ok(gen_meta)) = (proto_path.metadata(), generated_path.metadata()) {
        if let (Ok(proto_time), Ok(gen_time)) = (proto_meta.modified(), gen_meta.modified()) {
            return proto_time > gen_time;
        }
    }

    // Default to regenerating if we can't determine timestamps
    true
}
