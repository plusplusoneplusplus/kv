fn main() {
    // No build steps needed - we use the pre-generated kvstore.rs file
    println!("cargo:rerun-if-changed=src/kvstore.rs");
}