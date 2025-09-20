use std::collections::HashMap;
use std::sync::Mutex;
use async_trait::async_trait;
use kv_storage_api::{KvDatabase, GetResult, OpResult, GetRangeResult, AtomicCommitRequest, AtomicCommitResult, FaultInjectionConfig, KeyValue};

#[derive(Debug)]
pub struct MockDatabase {
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    version_counter: std::sync::atomic::AtomicU64,
}

impl MockDatabase {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            version_counter: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

impl Default for MockDatabase {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KvDatabase for MockDatabase {
    async fn get(&self, key: &[u8], _column_family: Option<&str>) -> Result<GetResult, String> {
        let data = self.data.lock().unwrap();
        match data.get(key) {
            Some(value) => Ok(GetResult {
                value: value.clone(),
                found: true,
            }),
            None => Ok(GetResult {
                value: Vec::new(),
                found: false,
            }),
        }
    }

    async fn put(&self, key: &[u8], value: &[u8], _column_family: Option<&str>) -> OpResult {
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        OpResult {
            success: true,
            error: String::new(),
            error_code: None,
        }
    }

    async fn delete(&self, key: &[u8], _column_family: Option<&str>) -> OpResult {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        OpResult {
            success: true,
            error: String::new(),
            error_code: None,
        }
    }

    async fn list_keys(&self, prefix: &[u8], limit: u32, _column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
        let data = self.data.lock().unwrap();
        let mut matching_keys: Vec<Vec<u8>> = data
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        matching_keys.sort();
        matching_keys.truncate(limit as usize);
        Ok(matching_keys)
    }

    async fn get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        _begin_offset: i32,
        begin_or_equal: bool,
        _end_offset: i32,
        end_or_equal: bool,
        limit: Option<i32>,
        _column_family: Option<&str>,
    ) -> Result<GetRangeResult, String> {
        let data = self.data.lock().unwrap();
        let mut key_values: Vec<KeyValue> = data
            .iter()
            .filter(|(key, _)| {
                let include_start = if begin_or_equal {
                    key.as_slice() >= begin_key
                } else {
                    key.as_slice() > begin_key
                };
                let include_end = if end_or_equal {
                    key.as_slice() <= end_key
                } else {
                    key.as_slice() < end_key
                };
                include_start && include_end
            })
            .map(|(k, v)| KeyValue {
                key: k.clone(),
                value: v.clone(),
            })
            .collect();

        key_values.sort_by(|a, b| a.key.cmp(&b.key));

        if let Some(limit) = limit {
            if limit > 0 {
                key_values.truncate(limit as usize);
            }
        }

        Ok(GetRangeResult {
            key_values,
            success: true,
            error: String::new(),
            has_more: false,
        })
    }

    async fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        let mut data = self.data.lock().unwrap();

        for operation in request.operations {
            match operation.op_type.as_str() {
                "PUT" => {
                    if let Some(value) = operation.value {
                        data.insert(operation.key, value);
                    }
                }
                "DELETE" => {
                    data.remove(&operation.key);
                }
                _ => {}
            }
        }

        let committed_version = self.version_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        AtomicCommitResult {
            success: true,
            error: String::new(),
            error_code: None,
            committed_version: Some(committed_version),
            generated_keys: Vec::new(),
            generated_values: Vec::new(),
        }
    }

    async fn get_read_version(&self) -> u64 {
        self.version_counter.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn snapshot_read(&self, key: &[u8], _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        self.get(key, column_family).await
    }

    async fn snapshot_get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        _read_version: u64,
        limit: Option<i32>,
        column_family: Option<&str>,
    ) -> Result<GetRangeResult, String> {
        self.get_range(begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit, column_family).await
    }

    async fn set_fault_injection(&self, _config: Option<FaultInjectionConfig>) -> OpResult {
        OpResult {
            success: true,
            error: String::new(),
            error_code: None,
        }
    }
}