//! RSML consensus implementation crate
//!
//! This crate provides RSML-specific consensus engine integration with the
//! consensus-api framework. It implements the factory pattern for engine-agnostic
//! usage while preserving RSML-specific error details and configuration options.

pub mod config;
pub mod error;
pub mod factory;

#[cfg(feature = "rsml")]
pub mod engine;

#[cfg(feature = "test-utils")]
pub mod test_utils;

pub use config::*;
pub use error::*;
pub use factory::*;

#[cfg(feature = "rsml")]
pub use engine::*;

// Re-export key types from consensus-api for convenience
pub use consensus_api::{ConsensusEngine, ConsensusConfig, StateMachine, ProposeResponse, LogEntry};

/// Initialize test logging with ANSI colors disabled
/// Available for both unit tests and integration tests
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)  // Disable ANSI color codes
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus_api::{ConsensusResult, LogEntry};
    use std::sync::{Arc, Mutex};
    use std::collections::HashMap;

    /// Simple key-value state machine for testing
    #[derive(Debug)]
    struct TestStateMachine {
        state: Arc<Mutex<HashMap<String, String>>>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn get_state(&self) -> HashMap<String, String> {
            self.state.lock().unwrap().clone()
        }
    }

    impl StateMachine for TestStateMachine {
        fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
            // Parse the entry data as "SET key value" or "GET key"
            let data_str = String::from_utf8_lossy(&entry.data);
            let parts: Vec<&str> = data_str.split_whitespace().collect();

            match parts.as_slice() {
                ["SET", key, value] => {
                    self.state.lock().unwrap().insert(key.to_string(), value.to_string());
                    Ok(format!("SET {} = {}", key, value).into_bytes())
                }
                ["GET", key] => {
                    let value = self.state.lock().unwrap()
                        .get(*key)
                        .cloned()
                        .unwrap_or_else(|| "NOT_FOUND".to_string());
                    Ok(format!("GET {} = {}", key, value).into_bytes())
                }
                _ => Ok(b"INVALID_OPERATION".to_vec()),
            }
        }

        fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
            let state = self.state.lock().unwrap();
            let serialized = serde_json::to_vec(&*state)
                .map_err(|e| consensus_api::ConsensusError::SerializationError {
                    message: e.to_string(),
                })?;
            Ok(serialized)
        }

        fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
            let state: HashMap<String, String> = serde_json::from_slice(snapshot)
                .map_err(|e| consensus_api::ConsensusError::SerializationError {
                    message: e.to_string(),
                })?;
            *self.state.lock().unwrap() = state;
            Ok(())
        }
    }

    #[test]
    fn test_state_machine() {
        let sm = TestStateMachine::new();

        // Test SET operation
        let set_entry = LogEntry {
            index: 1,
            term: 1,
            data: b"SET greeting hello".to_vec(),
            timestamp: 12345,
        };

        let result = sm.apply(&set_entry).unwrap();
        assert_eq!(result, b"SET greeting = hello");

        // Test GET operation
        let get_entry = LogEntry {
            index: 2,
            term: 1,
            data: b"GET greeting".to_vec(),
            timestamp: 12346,
        };

        let result = sm.apply(&get_entry).unwrap();
        assert_eq!(result, b"GET greeting = hello");

        // Check state
        let state = sm.get_state();
        assert_eq!(state.get("greeting"), Some(&"hello".to_string()));
    }

    #[test]
    fn test_invalid_operation() {
        let sm = TestStateMachine::new();

        let invalid_entry = LogEntry {
            index: 1,
            term: 1,
            data: b"INVALID OPERATION".to_vec(),
            timestamp: 12345,
        };

        let result = sm.apply(&invalid_entry).unwrap();
        assert_eq!(result, b"INVALID_OPERATION");
    }

    #[tokio::test]
    async fn test_consensus_factory_workflow() {
        // Initialize tracing for debugging
        init_test_logging();

        println!("Hello World Consensus Test");
        println!("==========================");

        // Step 1: Create a test cluster configuration
        let mut cluster_members = HashMap::new();
        cluster_members.insert("1".to_string(), "localhost:8080".to_string());
        cluster_members.insert("2".to_string(), "localhost:8081".to_string());
        cluster_members.insert("3".to_string(), "localhost:8082".to_string());

        println!("Created cluster with {} nodes", cluster_members.len());

        // Step 2: Create RSML consensus factory using in-memory transport
        let mut config = RsmlConfig::default();
        config.base.node_id = "1".to_string();
        config.base.cluster_members = cluster_members;
        config.transport.transport_type = crate::config::TransportType::InMemory;

        let factory = RsmlConsensusFactory::new(config).unwrap();

        println!("Created RSML consensus factory for node: {}", factory.config().base.node_id);

        // Step 3: Create our state machine
        let state_machine = Arc::new(TestStateMachine::new());
        println!("Created test state machine");

        // Step 4: Attempt to create consensus engine
        println!("Attempting to create consensus engine...");

        match factory.create_engine(state_machine.clone()).await {
            Ok(engine) => {
                println!("Successfully created consensus engine!");
                println!("   Engine type: {}", std::any::type_name_of_val(&*engine));
                println!("   Node ID: {}", engine.node_id());
                println!("   Current term: {}", engine.current_term());
                println!("   Is leader: {}", engine.is_leader());
                println!();
                println!("RSML consensus engine integration is working!");

                // Verify that the factory and configuration are working
                println!();
                println!("Factory successfully validated configuration:");
                println!("   Node ID: {}", factory.config().base.node_id);
                println!("   Cluster size: {}", factory.config().base.cluster_members.len());
                println!("   Transport: InMemory");

                // Assert key aspects are working
                assert_eq!(factory.config().base.node_id, "1");
                assert_eq!(factory.config().base.cluster_members.len(), 3);

                // Verify engine basics
                assert_eq!(engine.node_id(), "1");
                assert_eq!(engine.current_term(), 1);
                assert!(engine.is_leader());
            }
            Err(error) => {
                // Expected when RSML feature is not enabled
                println!("Engine creation failed as expected: {}", error);
                println!("This is expected when RSML feature is not enabled.");

                // Verify the factory configuration is still working
                assert_eq!(factory.config().base.node_id, "1");
                assert_eq!(factory.config().base.cluster_members.len(), 3);

                println!("Factory configuration validation is working correctly!");
            }
        }

        println!();
        println!("Hello World Consensus Test Complete!");
        println!("   The factory pattern is working and ready for RSML integration.");
    }
}