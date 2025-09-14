//! Integration tests for Memory Comparable Encoding (MCE) with database operations
//!
//! These tests verify that MCE works correctly in realistic database scenarios,
//! including version iteration, prefix matching, and MVCC operations.

use rocksdb_server::{VersionedKey, encode_mce, decode_mce};
use std::collections::HashMap;

/// Simulate a simple versioned key-value store using MCE
struct MockVersionedDB {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl MockVersionedDB {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Store a versioned key-value pair
    fn put_versioned(&mut self, key: &[u8], value: &[u8], version: u64) {
        let versioned_key = VersionedKey::new(key.to_vec(), version);
        let encoded_key = versioned_key.encode();
        self.data.insert(encoded_key, value.to_vec());
    }

    /// Find all versions of a key that are <= read_version
    fn find_versions_for_key(&self, key: &[u8], read_version: u64) -> Vec<(u64, Vec<u8>)> {
        let mce_prefix = encode_mce(key);
        let mut results = Vec::new();

        for (encoded_key, value) in &self.data {
            // Check if this key starts with our MCE prefix
            if encoded_key.starts_with(&mce_prefix) {
                if let Ok(versioned_key) = VersionedKey::decode(encoded_key) {
                    // Verify exact key match (MCE prefix matching should guarantee this)
                    if versioned_key.original_key == key && versioned_key.version <= read_version {
                        results.push((versioned_key.version, value.clone()));
                    }
                }
            }
        }

        // Sort by version (newest first due to inverted encoding)
        results.sort_by(|a, b| b.0.cmp(&a.0));
        results
    }

    /// Get the latest version of a key that is <= read_version
    fn get_at_version(&self, key: &[u8], read_version: u64) -> Option<Vec<u8>> {
        self.find_versions_for_key(key, read_version)
            .into_iter()
            .next()
            .map(|(_, value)| value)
    }

    /// Get all keys with their latest versions at a specific read timestamp
    fn snapshot_scan(&self, read_version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut key_latest_version: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();

        for (encoded_key, value) in &self.data {
            if let Ok(versioned_key) = VersionedKey::decode(encoded_key) {
                if versioned_key.version <= read_version {
                    let original_key = versioned_key.original_key;
                    let version = versioned_key.version;

                    // Keep only the latest version for each key
                    match key_latest_version.get(&original_key) {
                        Some((existing_version, _)) if *existing_version >= version => {
                            // Keep existing newer version
                        }
                        _ => {
                            // This is newer or first version for this key
                            key_latest_version.insert(original_key, (version, value.clone()));
                        }
                    }
                }
            }
        }

        key_latest_version
            .into_iter()
            .map(|(key, (_, value))| (key, value))
            .collect()
    }
}

#[test]
fn test_mvcc_snapshot_isolation() {
    let mut db = MockVersionedDB::new();

    // Version 100: Initial data
    db.put_versioned(b"user:alice", b"alice_v1", 100);
    db.put_versioned(b"user:bob", b"bob_v1", 100);
    db.put_versioned(b"product:1", b"laptop", 100);

    // Version 200: Update alice
    db.put_versioned(b"user:alice", b"alice_v2", 200);

    // Version 300: Add new user, delete product
    db.put_versioned(b"user:charlie", b"charlie_v1", 300);
    db.put_versioned(b"product:1", b"DELETED", 300);

    // Test snapshot reads at different versions

    // At version 100: should see initial state
    assert_eq!(db.get_at_version(b"user:alice", 100), Some(b"alice_v1".to_vec()));
    assert_eq!(db.get_at_version(b"user:bob", 100), Some(b"bob_v1".to_vec()));
    assert_eq!(db.get_at_version(b"product:1", 100), Some(b"laptop".to_vec()));
    assert_eq!(db.get_at_version(b"user:charlie", 100), None);

    // At version 200: should see alice updated
    assert_eq!(db.get_at_version(b"user:alice", 200), Some(b"alice_v2".to_vec()));
    assert_eq!(db.get_at_version(b"user:bob", 200), Some(b"bob_v1".to_vec()));
    assert_eq!(db.get_at_version(b"product:1", 200), Some(b"laptop".to_vec()));
    assert_eq!(db.get_at_version(b"user:charlie", 200), None);

    // At version 300: should see all changes
    assert_eq!(db.get_at_version(b"user:alice", 300), Some(b"alice_v2".to_vec()));
    assert_eq!(db.get_at_version(b"user:bob", 300), Some(b"bob_v1".to_vec()));
    assert_eq!(db.get_at_version(b"product:1", 300), Some(b"DELETED".to_vec()));
    assert_eq!(db.get_at_version(b"user:charlie", 300), Some(b"charlie_v1".to_vec()));
}

#[test]
fn test_version_history_iteration() {
    let mut db = MockVersionedDB::new();

    // Create multiple versions of the same key
    db.put_versioned(b"counter", b"1", 100);
    db.put_versioned(b"counter", b"2", 200);
    db.put_versioned(b"counter", b"3", 300);
    db.put_versioned(b"counter", b"4", 400);

    // Get all versions up to version 350
    let versions = db.find_versions_for_key(b"counter", 350);

    // Should get versions 300, 200, 100 (in that order due to newest-first)
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0], (300, b"3".to_vec()));
    assert_eq!(versions[1], (200, b"2".to_vec()));
    assert_eq!(versions[2], (100, b"1".to_vec()));

    // Get all versions up to version 150
    let versions_150 = db.find_versions_for_key(b"counter", 150);
    assert_eq!(versions_150.len(), 1);
    assert_eq!(versions_150[0], (100, b"1".to_vec()));

    // Get all versions up to version 50 (before any data)
    let versions_50 = db.find_versions_for_key(b"counter", 50);
    assert_eq!(versions_50.len(), 0);
}

#[test]
fn test_prefix_isolation() {
    let mut db = MockVersionedDB::new();

    // Add keys with similar prefixes
    db.put_versioned(b"user", b"admin", 100);
    db.put_versioned(b"user:", b"separator", 100);
    db.put_versioned(b"user:alice", b"alice_data", 100);
    db.put_versioned(b"user:alice:profile", b"profile_data", 100);
    db.put_versioned(b"user:bob", b"bob_data", 100);
    db.put_versioned(b"users", b"users_table", 100);

    // Each key should be completely isolated
    assert_eq!(db.get_at_version(b"user", 100), Some(b"admin".to_vec()));
    assert_eq!(db.get_at_version(b"user:", 100), Some(b"separator".to_vec()));
    assert_eq!(db.get_at_version(b"user:alice", 100), Some(b"alice_data".to_vec()));
    assert_eq!(db.get_at_version(b"user:alice:profile", 100), Some(b"profile_data".to_vec()));
    assert_eq!(db.get_at_version(b"user:bob", 100), Some(b"bob_data".to_vec()));
    assert_eq!(db.get_at_version(b"users", 100), Some(b"users_table".to_vec()));

    // Verify that similar keys don't interfere with each other
    let user_versions = db.find_versions_for_key(b"user", 100);
    assert_eq!(user_versions.len(), 1);
    assert_eq!(user_versions[0].1, b"admin".to_vec());

    let user_alice_versions = db.find_versions_for_key(b"user:alice", 100);
    assert_eq!(user_alice_versions.len(), 1);
    assert_eq!(user_alice_versions[0].1, b"alice_data".to_vec());
}

#[test]
fn test_snapshot_consistency() {
    let mut db = MockVersionedDB::new();

    // Build up a complex history
    db.put_versioned(b"balance:alice", b"1000", 100);
    db.put_versioned(b"balance:bob", b"500", 100);

    // Transaction 1: Transfer 200 from alice to bob at version 200
    db.put_versioned(b"balance:alice", b"800", 200);
    db.put_versioned(b"balance:bob", b"700", 200);

    // Transaction 2: Alice deposits 300 at version 300
    db.put_versioned(b"balance:alice", b"1100", 300);

    // Transaction 3: Bob withdraws 100 at version 400
    db.put_versioned(b"balance:bob", b"600", 400);

    // Verify snapshot consistency at different points

    // At version 150 (between initial and first transaction)
    let snapshot_150 = db.snapshot_scan(150);
    let snapshot_150_map: HashMap<_, _> = snapshot_150.into_iter().collect();
    assert_eq!(snapshot_150_map.get(b"balance:alice".as_slice()), Some(&b"1000".to_vec()));
    assert_eq!(snapshot_150_map.get(b"balance:bob".as_slice()), Some(&b"500".to_vec()));

    // At version 250 (after transfer)
    let snapshot_250 = db.snapshot_scan(250);
    let snapshot_250_map: HashMap<_, _> = snapshot_250.into_iter().collect();
    assert_eq!(snapshot_250_map.get(b"balance:alice".as_slice()), Some(&b"800".to_vec()));
    assert_eq!(snapshot_250_map.get(b"balance:bob".as_slice()), Some(&b"700".to_vec()));

    // At version 350 (after alice's deposit)
    let snapshot_350 = db.snapshot_scan(350);
    let snapshot_350_map: HashMap<_, _> = snapshot_350.into_iter().collect();
    assert_eq!(snapshot_350_map.get(b"balance:alice".as_slice()), Some(&b"1100".to_vec()));
    assert_eq!(snapshot_350_map.get(b"balance:bob".as_slice()), Some(&b"700".to_vec()));

    // At version 450 (after bob's withdrawal)
    let snapshot_450 = db.snapshot_scan(450);
    let snapshot_450_map: HashMap<_, _> = snapshot_450.into_iter().collect();
    assert_eq!(snapshot_450_map.get(b"balance:alice".as_slice()), Some(&b"1100".to_vec()));
    assert_eq!(snapshot_450_map.get(b"balance:bob".as_slice()), Some(&b"600".to_vec()));
}

#[test]
fn test_mce_boundary_detection_in_practice() {
    // Test that MCE boundary detection works correctly when versioned keys
    // are concatenated with other data (simulating RocksDB storage)

    let vk1 = VersionedKey::new(b"key1".to_vec(), 100);
    let vk2 = VersionedKey::new(b"key22".to_vec(), 200);  // Longer key
    let vk3 = VersionedKey::new(b"key333".to_vec(), 300); // Even longer key

    let encoded1 = vk1.encode();
    let encoded2 = vk2.encode();
    let encoded3 = vk3.encode();

    // Verify each can be decoded correctly
    assert_eq!(VersionedKey::decode(&encoded1).unwrap(), vk1);
    assert_eq!(VersionedKey::decode(&encoded2).unwrap(), vk2);
    assert_eq!(VersionedKey::decode(&encoded3).unwrap(), vk3);

    // Verify boundary detection when concatenated with arbitrary data
    let extra_data = b"this_is_extra_metadata_after_the_versioned_key";

    for (vk, encoded) in [
        (&vk1, &encoded1),
        (&vk2, &encoded2),
        (&vk3, &encoded3)
    ] {
        let combined = [encoded.as_slice(), extra_data].concat();

        // Should be able to decode just the versioned key part
        let decoded = VersionedKey::decode(&combined[..encoded.len()]).unwrap();
        assert_eq!(&decoded, vk);

        // Verify the boundary calculation
        let (mce_key, mce_end) = decode_mce(encoded).unwrap();
        assert_eq!(mce_key, vk.original_key);
        assert_eq!(mce_end + 8, encoded.len()); // MCE + 8 bytes for version

        // Verify that the extra data starts where we expect
        assert_eq!(&combined[encoded.len()..], extra_data);
    }
}

#[test]
fn test_large_scale_versioning() {
    let mut db = MockVersionedDB::new();

    // Simulate a workload with many keys and versions
    for key_id in 0..100 {
        for version in (100..=500).step_by(50) {
            let key = format!("item:{:03}", key_id).into_bytes();
            let value = format!("value_{}_{}", key_id, version).into_bytes();
            db.put_versioned(&key, &value, version);
        }
    }

    // Verify we can read consistently at any version
    for read_version in [150, 250, 350, 450] {
        let snapshot = db.snapshot_scan(read_version);

        // Should have exactly 100 keys (one per item)
        assert_eq!(snapshot.len(), 100);

        // Verify each key has the correct version for this snapshot
        for (key, value) in snapshot {
            let key_str = String::from_utf8(key.clone()).unwrap();
            let key_id: u32 = key_str[5..8].parse().unwrap();

            // Find the latest version for this key that's <= read_version
            let expected_version = (100..=read_version).step_by(50).last().unwrap();
            let expected_value = format!("value_{}_{}", key_id, expected_version).into_bytes();

            assert_eq!(value, expected_value,
                "Key {} at read version {} should have value from version {}",
                key_str, read_version, expected_version);
        }
    }
}