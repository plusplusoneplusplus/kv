fn main() {
    // Generated kvstore.rs is copied by CMake from build/generated/
    println!("cargo:rerun-if-changed=src/kvstore.rs");
    println!("cargo:rerun-if-changed=../thrift/kvstore.thrift");
}