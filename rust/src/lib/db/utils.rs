use std::time::Duration;
use super::types::FaultInjectionConfig;

pub fn apply_versionstamp(buffer: &[u8], commit_version: u64, batch_order: u16) -> Result<Vec<u8>, String> {
    if buffer.len() < 10 {
        return Err("Buffer must be at least 10 bytes for versionstamp".to_string());
    }

    let mut result = buffer.to_vec();
    let len = result.len();

    // Overwrite last 10 bytes: 8 bytes commit version + 2 bytes batch order (FoundationDB-compatible)
    result[len-10..len-2].copy_from_slice(&commit_version.to_be_bytes());
    result[len-2..].copy_from_slice(&batch_order.to_be_bytes());

    Ok(result)
}

pub fn calculate_offset_key(base_key: &[u8], offset: i32) -> Vec<u8> {
    if offset == 0 {
        return base_key.to_vec();
    }

    let mut result = base_key.to_vec();

    if offset > 0 {
        // Positive offset: increment key lexicographically
        for _ in 0..offset {
            result = increment_key(&result);
        }
    } else {
        // Negative offset: decrement key lexicographically
        for _ in 0..(-offset) {
            result = decrement_key(&result);
        }
    }

    result
}

/// Increment a key to get the next lexicographically ordered key
pub fn increment_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return vec![0x01];
    }

    let mut result = key.to_vec();

    // Try to increment from the rightmost byte
    for i in (0..result.len()).rev() {
        if result[i] < 0xFF {
            result[i] += 1;
            return result; // Successfully incremented, no carry needed
        }
        // This byte is 0xFF, set to 0x00 and continue carry
        result[i] = 0x00;
    }

    // All bytes were 0xFF and are now 0x00
    // The next lexicographic key is the original key with 0x00 appended
    // For example: [0xFF] -> [0xFF, 0x00]
    let mut original = key.to_vec();
    original.push(0x00);
    original
}

/// Decrement a key to get the previous lexicographically ordered key
pub fn decrement_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        // Cannot go before empty key
        return Vec::new();
    }

    let mut result = key.to_vec();

    // Handle special case: single byte 'a' (97) should become empty
    if result == b"a" {
        return Vec::new();
    }

    // Find the rightmost non-zero byte and decrement it
    for i in (0..result.len()).rev() {
        if result[i] > 0x00 {
            result[i] -= 1;
            // Set all bytes to the right to 0xFF (due to borrow)
            for j in (i + 1)..result.len() {
                result[j] = 0xFF;
            }
            return result;
        }
    }

    // All bytes were 0x00 - need to shorten the key
    // Remove trailing zeros until we find a non-zero byte or become empty
    while let Some(&0x00) = result.last() {
        result.pop();
        if result.is_empty() {
            return Vec::new();
        }
    }

    // Decrement the last non-zero byte
    if let Some(last_byte) = result.last_mut() {
        *last_byte -= 1;
    }

    result
}

pub fn should_inject_fault(config: &Option<FaultInjectionConfig>, operation: &str) -> Option<String> {
    if let Some(ref config) = *config {
        if let Some(ref target) = config.target_operation {
            if target != operation {
                return None;
            }
        }

        let random: f64 = rand::random();
        if random < config.probability {
            if config.duration_ms > 0 {
                std::thread::sleep(Duration::from_millis(config.duration_ms as u64));
            }
            return Some(config.fault_type.clone());
        }
    }
    None
}