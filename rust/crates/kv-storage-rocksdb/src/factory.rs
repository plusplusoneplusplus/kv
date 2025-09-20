use crate::engine::TransactionalKvDatabase;
use kv_storage_api::KvDatabase;
use std::sync::Arc;

/// Simple database factory that creates the right database type based on deployment mode.
/// This is all we actually need - the "routing" is handled by the consensus layer (RSML).
pub enum DatabaseFactory {
    /// Standalone mode - single database instance
    Standalone { config: crate::config::Config },
    /// Replicated mode - consensus-backed database (Phase 1)
    Replicated {
        config: crate::config::Config,
        instance_id: u32,
    },
}

impl DatabaseFactory {
    /// Create database instance
    pub fn create_database(&self) -> Result<Arc<dyn KvDatabase>, String> {
        match self {
            DatabaseFactory::Standalone { config } => {
                let db_path = &config.database.base_path;
                TransactionalKvDatabase::new(db_path, config, &[])
                    .map(|db| Arc::new(db) as Arc<dyn KvDatabase>)
                    .map_err(|e| format!("Failed to create standalone database: {}", e))
            }
            DatabaseFactory::Replicated {
                config,
                instance_id,
            } => {
                // In Phase 1, this will create a database wrapped with RSML consensus
                let db_path = format!("{}/replica_{}", config.database.base_path, instance_id);
                TransactionalKvDatabase::new(&db_path, config, &[])
                    .map(|db| Arc::new(db) as Arc<dyn KvDatabase>)
                    .map_err(|e| format!("Failed to create replicated database: {}", e))
            }
        }
    }

    /// Create standalone factory
    pub fn standalone(config: crate::config::Config) -> Self {
        DatabaseFactory::Standalone { config }
    }

    /// Create replicated factory
    pub fn replicated(config: crate::config::Config, instance_id: u32) -> Self {
        DatabaseFactory::Replicated {
            config,
            instance_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DeploymentMode};
    use tempfile::TempDir;

    fn create_test_config(base_path: &str) -> Config {
        let mut config = Config::default();
        config.database.base_path = base_path.to_string();
        config
    }

    #[test]
    fn test_standalone_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let factory = DatabaseFactory::standalone(config);

        match factory {
            DatabaseFactory::Standalone { .. } => {
                // Success - factory created correctly
            }
            _ => panic!("Expected Standalone factory"),
        }
    }

    #[test]
    fn test_replicated_factory_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let instance_id = 42;

        let factory = DatabaseFactory::replicated(config, instance_id);

        match factory {
            DatabaseFactory::Replicated {
                instance_id: id, ..
            } => {
                assert_eq!(id, 42);
            }
            _ => panic!("Expected Replicated factory"),
        }
    }

    #[test]
    fn test_standalone_database_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let factory = DatabaseFactory::standalone(config);
        let result = factory.create_database();

        assert!(
            result.is_ok(),
            "Failed to create standalone database: {:?}",
            result.err()
        );

        let _db = result.unwrap();
        // Database created successfully
    }

    #[test]
    fn test_replicated_database_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(temp_dir.path().to_str().unwrap());

        let factory = DatabaseFactory::replicated(config, 1);
        let result = factory.create_database();

        assert!(
            result.is_ok(),
            "Failed to create replicated database: {:?}",
            result.err()
        );

        let _db = result.unwrap();
        // Database created successfully
    }

    #[test]
    fn test_replicated_database_path_separation() {
        let temp_dir = TempDir::new().unwrap();
        let base_path = temp_dir.path().to_str().unwrap();
        let config = create_test_config(base_path);

        // Create two replicated databases with different instance IDs
        let factory1 = DatabaseFactory::replicated(config.clone(), 1);
        let factory2 = DatabaseFactory::replicated(config, 2);

        let _db1 = factory1.create_database().unwrap();
        let _db2 = factory2.create_database().unwrap();

        // Both should succeed - they use different paths

        // Verify different instances created different directories
        let replica1_path = format!("{}/replica_1", base_path);
        let replica2_path = format!("{}/replica_2", base_path);

        assert!(std::path::Path::new(&replica1_path).exists());
        assert!(std::path::Path::new(&replica2_path).exists());
    }

    #[test]
    fn test_config_factory_integration() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = create_test_config(temp_dir.path().to_str().unwrap());

        // Test standalone mode
        config.deployment.mode = DeploymentMode::Standalone;
        let factory = config.create_database_factory();

        match factory {
            DatabaseFactory::Standalone { .. } => {
                // Success
            }
            _ => panic!("Expected Standalone factory from config"),
        }

        // Test replicated mode
        config.deployment.mode = DeploymentMode::Replicated;
        config.deployment.instance_id = Some(5);
        let factory = config.create_database_factory();

        match factory {
            DatabaseFactory::Replicated { instance_id: 5, .. } => {
                // Success
            }
            _ => panic!("Expected Replicated factory with instance_id 5"),
        }
    }

    #[test]
    fn test_invalid_path_error_handling() {
        let config = create_test_config("/invalid/nonexistent/path");
        let factory = DatabaseFactory::standalone(config);

        let result = factory.create_database();
        assert!(result.is_err(), "Expected error for invalid path");

        let error_msg = result.err().unwrap();
        assert!(error_msg.contains("Failed to create standalone database"));
    }
}
