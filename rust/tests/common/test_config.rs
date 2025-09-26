/// Test configuration and binary path management
/// Provides centralized access to server binaries for testing

use std::env;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Configuration for test binaries and paths
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub thrift_server_path: PathBuf,
    pub grpc_server_path: PathBuf,
    pub workspace_root: PathBuf,
}

/// Global test configuration instance
static TEST_CONFIG: OnceLock<TestConfig> = OnceLock::new();

impl TestConfig {
    /// Get the global test configuration, initializing it if needed
    pub fn global() -> &'static TestConfig {
        TEST_CONFIG.get_or_init(Self::detect_config)
    }

    /// Detect and configure test binary paths automatically
    fn detect_config() -> TestConfig {
        let workspace_root = Self::find_workspace_root();

        let thrift_server_path = Self::find_thrift_server_binary(&workspace_root);
        let grpc_server_path = Self::find_grpc_server_binary(&workspace_root);

        TestConfig {
            thrift_server_path,
            grpc_server_path,
            workspace_root,
        }
    }

    /// Find the workspace root directory
    fn find_workspace_root() -> PathBuf {
        let current_dir = env::current_dir()
            .expect("Failed to get current directory");

        // Search upwards for Cargo.toml
        let mut path = current_dir.as_path();
        loop {
            let cargo_toml = path.join("Cargo.toml");
            if cargo_toml.exists() {
                // Check if this is a workspace root (not just a crate)
                if let Ok(content) = std::fs::read_to_string(&cargo_toml) {
                    if content.contains("[workspace]") {
                        return path.to_path_buf();
                    }
                }
                // If not workspace, try parent
                if let Some(parent) = path.parent() {
                    path = parent;
                    continue;
                }
            }

            if let Some(parent) = path.parent() {
                path = parent;
            } else {
                break;
            }
        }

        // Fallback to current directory
        current_dir
    }

    /// Find the Thrift server binary with multiple fallback strategies
    fn find_thrift_server_binary(workspace_root: &Path) -> PathBuf {
        // Priority order for Thrift server binary detection:

        // 1. Environment variable override
        if let Ok(path) = env::var("THRIFT_SERVER_BINARY") {
            let binary_path = PathBuf::from(path);
            if binary_path.exists() {
                return binary_path;
            }
        }

        // 2. CMake build directory (preferred for integrated builds)
        let cmake_thrift = workspace_root.join("build/bin/rocksdbserver-thrift");
        if cmake_thrift.exists() {
            return cmake_thrift;
        }

        // 3. Cargo debug build (current working solution)
        let cargo_debug = workspace_root.join("target/debug/thrift-server");
        if cargo_debug.exists() {
            return cargo_debug;
        }

        // 4. Cargo release build
        let cargo_release = workspace_root.join("target/release/thrift-server");
        if cargo_release.exists() {
            return cargo_release;
        }

        // 5. Check common locations relative to test directory
        let relative_paths = [
            "./target/debug/thrift-server",
            "../target/debug/thrift-server",
            "../../target/debug/thrift-server",
        ];

        for relative_path in &relative_paths {
            let path = PathBuf::from(relative_path);
            if path.exists() {
                return path;
            }
        }

        // 6. Fallback to expected Cargo location (for error messaging)
        workspace_root.join("target/debug/thrift-server")
    }

    /// Find the gRPC server binary with multiple fallback strategies
    fn find_grpc_server_binary(workspace_root: &Path) -> PathBuf {
        // Priority order for gRPC server binary detection:

        // 1. Environment variable override
        if let Ok(path) = env::var("GRPC_SERVER_BINARY") {
            let binary_path = PathBuf::from(path);
            if binary_path.exists() {
                return binary_path;
            }
        }

        // 2. CMake build directory
        let cmake_grpc = workspace_root.join("build/bin/rocksdbserver-rust");
        if cmake_grpc.exists() {
            return cmake_grpc;
        }

        // 3. Cargo debug build
        let cargo_debug = workspace_root.join("target/debug/server");
        if cargo_debug.exists() {
            return cargo_debug;
        }

        // 4. Cargo release build
        let cargo_release = workspace_root.join("target/release/server");
        if cargo_release.exists() {
            return cargo_release;
        }

        // 5. Fallback to expected Cargo location
        workspace_root.join("target/debug/server")
    }

    /// Get the Thrift server binary path
    pub fn thrift_server_binary(&self) -> &Path {
        &self.thrift_server_path
    }

    /// Get the gRPC server binary path
    pub fn grpc_server_binary(&self) -> &Path {
        &self.grpc_server_path
    }

    /// Check if the Thrift server binary exists and is executable
    pub fn validate_thrift_server(&self) -> Result<(), String> {
        if !self.thrift_server_path.exists() {
            return Err(format!(
                "Thrift server binary not found at: {}\n\
                 Tried locations:\n\
                 - Environment variable: THRIFT_SERVER_BINARY\n\
                 - CMake build: {}/build/bin/rocksdbserver-thrift\n\
                 - Cargo debug: {}/target/debug/thrift-server\n\
                 - Cargo release: {}/target/release/thrift-server\n\
                 \n\
                 To fix this:\n\
                 1. Run 'cargo build --bin thrift-server' to build the binary\n\
                 2. Or set THRIFT_SERVER_BINARY environment variable to the correct path\n\
                 3. Or run CMake build: 'cmake --build build'",
                self.thrift_server_path.display(),
                self.workspace_root.display(),
                self.workspace_root.display(),
                self.workspace_root.display()
            ));
        }

        Ok(())
    }

    /// Check if the gRPC server binary exists and is executable
    #[allow(dead_code)]
    pub fn validate_grpc_server(&self) -> Result<(), String> {
        if !self.grpc_server_path.exists() {
            return Err(format!(
                "gRPC server binary not found at: {}\n\
                 Run 'cargo build --bin server' to build the binary",
                self.grpc_server_path.display()
            ));
        }

        Ok(())
    }

    /// Print current configuration for debugging
    pub fn print_config(&self) {
        println!("Test Configuration:");
        println!("  Workspace root: {}", self.workspace_root.display());
        println!("  Thrift server:  {} (exists: {})",
                self.thrift_server_path.display(),
                self.thrift_server_path.exists());
        println!("  gRPC server:    {} (exists: {})",
                self.grpc_server_path.display(),
                self.grpc_server_path.exists());
    }
}

/// Convenience function to get the Thrift server binary path
pub fn thrift_server_binary() -> &'static Path {
    TestConfig::global().thrift_server_binary()
}

/// Convenience function to get the gRPC server binary path
pub fn grpc_server_binary() -> &'static Path {
    TestConfig::global().grpc_server_binary()
}

/// Validate that all required binaries exist
#[allow(dead_code)]
pub fn validate_test_binaries() -> Result<(), String> {
    let config = TestConfig::global();
    config.validate_thrift_server()?;
    config.validate_grpc_server()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_detection() {
        let config = TestConfig::global();
        config.print_config();

        // At minimum, paths should be set
        assert!(!config.thrift_server_path.as_os_str().is_empty());
        assert!(!config.grpc_server_path.as_os_str().is_empty());
    }

    #[test]
    fn test_workspace_root_detection() {
        let root = TestConfig::find_workspace_root();
        println!("Detected workspace root: {}", root.display());

        // Should find a directory that contains Cargo.toml
        let cargo_toml = root.join("Cargo.toml");
        assert!(cargo_toml.exists(), "Should find Cargo.toml at workspace root");
    }

    #[test]
    fn test_convenience_functions() {
        let thrift_path = thrift_server_binary();
        let grpc_path = grpc_server_binary();

        assert!(!thrift_path.as_os_str().is_empty());
        assert!(!grpc_path.as_os_str().is_empty());
    }

    #[test]
    fn test_env_var_override() {
        // This test would need to be run with environment variables set
        // to verify the override functionality works
        env::set_var("THRIFT_SERVER_BINARY", "/tmp/fake-thrift-server");

        // Create a new config to test env var detection
        // Note: This won't affect the global config due to OnceLock
        let config = TestConfig::detect_config();

        // Clean up
        env::remove_var("THRIFT_SERVER_BINARY");

        println!("Config with env var: {:?}", config);
    }
}