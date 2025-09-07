fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/kvstore.proto")?;
    
    // Watch for changes to Thrift files - kvstore.rs is copied by CMake
    println!("cargo:rerun-if-changed=src/kvstore.rs");
    println!("cargo:rerun-if-changed=../thrift/kvstore.thrift");
    
    Ok(())
}
