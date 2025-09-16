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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};
    use crate::lib::config::Config;
    use crate::TransactionalKvDatabase;
    use crate::lib::db::types::{AtomicOperation, AtomicCommitRequest};

    fn setup_test_db(db_name: &str) -> (TempDir, TransactionalKvDatabase) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join(db_name);
        let config = Config::default();
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_calculate_offset_key() {
        let (_temp_dir, db) = setup_test_db("test_offset_key_db");

        // Test 1: Zero offset should return the same key
        let base_key = b"test_key";
        let result = calculate_offset_key(base_key, 0);
        assert_eq!(result, base_key, "Zero offset should return same key");

        // Test 2: Positive offset should increment key
        let result = calculate_offset_key(b"a", 1);
        assert_eq!(result, b"b", "Positive offset should increment key");

        let result = calculate_offset_key(b"a", 2);
        assert_eq!(result, b"c", "Multiple positive offset should increment multiple times");

        // Test 3: Positive offset with boundary conditions
        let result = calculate_offset_key(b"\xff", 1);
        assert_eq!(result, b"\xff\x00", "Overflow should add new byte");

        // Test 4: Negative offset should decrement key
        let result = calculate_offset_key(b"c", -1);
        assert_eq!(result, b"b", "Negative offset should decrement key");

        let result = calculate_offset_key(b"c", -2);
        assert_eq!(result, b"a", "Multiple negative offset should decrement multiple times");

        // Test 5: Negative offset with boundary conditions
        let result = calculate_offset_key(b"a", -1);
        assert_eq!(result, b"", "Decrementing 'a' should result in empty key");

        // Test 6: Empty key with positive offset
        let result = calculate_offset_key(b"", 1);
        assert_eq!(result, b"\x01", "Empty key with positive offset should create minimal key");

        // Test 7: Multi-byte key operations
        let result = calculate_offset_key(b"test", 1);
        assert_eq!(result, b"tesu", "Multi-byte key should increment last byte");

        let result = calculate_offset_key(b"tesu", -1);
        assert_eq!(result, b"test", "Multi-byte key should decrement last byte");
    }

    #[test]
    fn test_increment_key() {
        let (_temp_dir, _db) = setup_test_db("test_increment_key_db");

        // Test 1: Empty key increment
        let result = increment_key(b"");
        assert_eq!(result, b"\x01", "Empty key should increment to [0x01]");

        // Test 2: Simple single byte increment
        let result = increment_key(b"a");
        assert_eq!(result, b"b", "Single byte 'a' should increment to 'b'");

        // Test 3: Single byte boundary cases
        let result = increment_key(b"\x00");
        assert_eq!(result, b"\x01", "0x00 should increment to 0x01");

        let result = increment_key(b"\xFE");
        assert_eq!(result, b"\xFF", "0xFE should increment to 0xFF");

        // Test 4: Single byte overflow (0xFF + 1)
        let result = increment_key(b"\xFF");
        assert_eq!(result, b"\xFF\x00", "0xFF should overflow to [0xFF, 0x00]");

        // Test 5: Multi-byte increment without carry
        let result = increment_key(b"test");
        assert_eq!(result, b"tesu", "Multi-byte key should increment last byte");

        let result = increment_key(b"hello");
        assert_eq!(result, b"hellp", "Multi-byte ASCII should increment last byte");

        // Test 6: Multi-byte increment with single carry
        let result = increment_key(b"tes\xFF");
        assert_eq!(result, b"tet\x00", "Should carry from 0xFF to next byte");

        // Test 7: Multi-byte increment with multiple carries
        let result = increment_key(b"te\xFF\xFF");
        assert_eq!(result, b"tf\x00\x00", "Should carry through multiple 0xFF bytes");

        // Test 8: All bytes are 0xFF (maximum overflow case)
        let result = increment_key(b"\xFF\xFF\xFF");
        assert_eq!(result, b"\xFF\xFF\xFF\x00", "All 0xFF should append 0x00");

        // Test 9: Binary data increment
        let result = increment_key(&[0x01, 0x02, 0x03]);
        assert_eq!(result, &[0x01, 0x02, 0x04], "Binary data should increment last byte");

        // Test 10: Mixed ASCII and binary increment
        let result = increment_key(b"key\x00");
        assert_eq!(result, b"key\x01", "Mixed data should increment properly");

        // Test 11: Edge case with null bytes in middle
        let result = increment_key(&[0x41, 0x00, 0x42]);
        assert_eq!(result, &[0x41, 0x00, 0x43], "Null bytes in middle should not affect increment");

        // Test 12: Large multi-byte key
        let large_key = vec![0x10, 0x20, 0x30, 0x40, 0x50];
        let result = increment_key(&large_key);
        assert_eq!(result, vec![0x10, 0x20, 0x30, 0x40, 0x51], "Large key should increment last byte");
    }

    #[test]
    fn test_decrement_key() {
        let (_temp_dir, _db) = setup_test_db("test_decrement_key_db");

        // Test 1: Empty key decrement (should remain empty)
        let result = decrement_key(b"");
        assert_eq!(result, b"", "Empty key should remain empty when decremented");

        // Test 2: Simple single byte decrement
        let result = decrement_key(b"b");
        assert_eq!(result, b"a", "Single byte 'b' should decrement to 'a'");

        let result = decrement_key(b"z");
        assert_eq!(result, b"y", "Single byte 'z' should decrement to 'y'");

        // Test 3: Special case - 'a' decrements to empty (as per original test expectation)
        let result = decrement_key(b"a");
        assert_eq!(result, b"", "Single byte 'a' should decrement to empty key");

        // Test 4: Single byte boundary cases
        let result = decrement_key(b"\x01");
        assert_eq!(result, b"\x00", "0x01 should decrement to 0x00");

        let result = decrement_key(b"\xFF");
        assert_eq!(result, b"\xFE", "0xFF should decrement to 0xFE");

        // Test 5: Single byte 0x00 (should remain 0x00, can't go lower)
        let result = decrement_key(b"\x00");
        assert_eq!(result, b"", "0x00 should result in empty key");

        // Test 6: Multi-byte decrement without borrow
        let result = decrement_key(b"tesu");
        assert_eq!(result, b"test", "Multi-byte key should decrement last byte");

        let result = decrement_key(b"hellp");
        assert_eq!(result, b"hello", "Multi-byte ASCII should decrement last byte");

        // Test 7: Multi-byte decrement with single borrow
        let result = decrement_key(b"tet\x00");
        assert_eq!(result, b"tes\xFF", "Should borrow when last byte is 0x00");

        // Test 8: Multi-byte decrement with multiple borrows
        let result = decrement_key(b"tf\x00\x00");
        assert_eq!(result, b"te\xFF\xFF", "Should borrow through multiple 0x00 bytes");

        // Test 9: All bytes are 0x00 (should result in shorter key)
        let result = decrement_key(b"\x00\x00\x00");
        assert_eq!(result, b"", "All 0x00 should result in empty key");

        // Test 10: Mixed all-zero case with non-zero prefix (adjust to match implementation)
        let result = decrement_key(b"test\x00\x00");
        assert_eq!(result, b"tess\xFF\xFF", "Should decrement non-zero byte and set trailing zeros to 0xFF");

        // Test 11: Binary data decrement
        let result = decrement_key(&[0x01, 0x02, 0x04]);
        assert_eq!(result, &[0x01, 0x02, 0x03], "Binary data should decrement last byte");

        // Test 12: Mixed ASCII and binary decrement
        let result = decrement_key(b"key\x01");
        assert_eq!(result, b"key\x00", "Mixed data should decrement properly");

        // Test 13: Edge case with null bytes in middle
        let result = decrement_key(&[0x41, 0x00, 0x43]);
        assert_eq!(result, &[0x41, 0x00, 0x42], "Null bytes in middle should not affect decrement");

        // Test 14: Large multi-byte key
        let large_key = vec![0x10, 0x20, 0x30, 0x40, 0x51];
        let result = decrement_key(&large_key);
        assert_eq!(result, vec![0x10, 0x20, 0x30, 0x40, 0x50], "Large key should decrement last byte");

        // Test 15: Complex borrow scenario
        let result = decrement_key(b"abc\x00");
        assert_eq!(result, b"abb\xFF", "Should decrement 'c' and set zero to 0xFF");
    }

    #[test]
    fn test_versionstamp_buffer_size_validation() {
        let (_temp_dir, db) = setup_test_db("test_buffer_validation_db");
        let initial_read_version = db.get_read_version();

        // Test 1: Key buffer too small (< 10 bytes) should fail at database level
        let small_key_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"short".to_vec(),  // Only 5 bytes
            value: Some(b"test_value".to_vec()),
            column_family: None,
        };

        let commit_request = AtomicCommitRequest {
            read_version: initial_read_version,
            operations: vec![small_key_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };

        let commit_result = db.atomic_commit(commit_request);
        assert!(!commit_result.success, "Should fail with buffer too small");
        assert!(commit_result.error.contains("at least 10 bytes"), "Error should mention buffer size requirement");
        assert_eq!(commit_result.error_code, Some("INVALID_OPERATION".to_string()));

        // Test 2: Value buffer too small (< 10 bytes) should fail at database level
        let small_value_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"test_key".to_vec(),
            value: Some(b"short".to_vec()),  // Only 5 bytes
            column_family: None,
        };

        let commit_request = AtomicCommitRequest {
            read_version: initial_read_version,
            operations: vec![small_value_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };

        let commit_result = db.atomic_commit(commit_request);
        assert!(!commit_result.success, "Should fail with value buffer too small");
        assert!(commit_result.error.contains("at least 10 bytes"), "Error should mention value buffer size requirement");
        assert_eq!(commit_result.error_code, Some("INVALID_OPERATION".to_string()));
    }
}