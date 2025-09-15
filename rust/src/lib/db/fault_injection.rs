use std::sync::{Arc, RwLock};
use super::types::{FaultInjectionConfig, OpResult};

/// Fault injection operations for testing
pub struct FaultInjection;

impl FaultInjection {
    /// Set fault injection configuration for testing
    pub fn set_fault_injection(
        fault_injection: &Arc<RwLock<Option<FaultInjectionConfig>>>,
        config: Option<FaultInjectionConfig>
    ) -> OpResult {
        let mut fault_injection_guard = fault_injection.write().unwrap();
        *fault_injection_guard = config;
        OpResult::success()
    }

    /// Clear fault injection configuration
    pub fn clear_fault_injection(fault_injection: &Arc<RwLock<Option<FaultInjectionConfig>>>) {
        let mut fault_injection_guard = fault_injection.write().unwrap();
        *fault_injection_guard = None;
    }

    /// Get the current fault injection configuration (for debugging/testing)
    pub fn get_fault_injection(
        fault_injection: &Arc<RwLock<Option<FaultInjectionConfig>>>
    ) -> Option<FaultInjectionConfig> {
        let fault_injection_guard = fault_injection.read().unwrap();
        fault_injection_guard.clone()
    }
}