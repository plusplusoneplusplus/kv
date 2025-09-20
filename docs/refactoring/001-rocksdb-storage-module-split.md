# RocksDB Storage Module Split (Minimal-Change Plan)

## Why
The current `rocksdb_server` crate mixes storage interfaces with the RocksDB-backed implementation. We need a lighter-weight separation so upcoming multi-node work can depend on storage traits without pulling in RocksDB. This plan keeps behavior identical while only moving files and adjusting module paths.

## Goals
- Keep public APIs and behavior intact; the top-level crate continues to re-export the same symbols.
- Limit work to file moves and simple `use` path updates—no logic rewrites or new abstractions.
- Ensure everything still builds (`cargo check`) after each stage.

## Target Layout
- `crates/kv-storage-api`
  - `src/lib.rs` (new)
  - `src/traits.rs` ← `rust/src/lib/db_trait.rs`
  - `src/types.rs` ← shared structs/enums currently in `rust/src/lib/db.rs`
- `crates/kv-storage-rocksdb`
  - `src/lib.rs` (new)
  - `src/engine.rs` ← remaining RocksDB code from `rust/src/lib/db.rs`
  - `src/config.rs` ← `rust/src/lib/config.rs`
  - `src/factory.rs` ← `rust/src/lib/database_factory.rs`
- `rust/src/lib.rs`
  - Drops `mod db_trait; mod db; mod config;` etc.
  - Adds `pub use kv_storage_api::*;` and `pub use kv_storage_rocksdb::*;`

## File Moves (exact)
1. Move `rust/src/lib/db_trait.rs` → `crates/kv-storage-api/src/traits.rs`.
2. Copy the non-RocksDB structs/enums from `rust/src/lib/db.rs` into `crates/kv-storage-api/src/types.rs` and remove them from the original file.
3. Move remaining contents of `rust/src/lib/db.rs` → `crates/kv-storage-rocksdb/src/engine.rs`.
4. Move `rust/src/lib/config.rs` → `crates/kv-storage-rocksdb/src/config.rs`.
5. Move `rust/src/lib/database_factory.rs` → `crates/kv-storage-rocksdb/src/factory.rs`.
6. Create `crates/kv-storage-api/src/lib.rs` that `pub mod traits; pub mod types;` and re-exports key items for current callers.
7. Create `crates/kv-storage-rocksdb/src/lib.rs` that wires `engine`, `config`, `factory` and re-exports their public items.
8. Update `rust/src/lib.rs` to remove old `mod` statements and add the new crate re-exports.

## Minimal Touch Updates
- Adjust any `use crate::lib::db_trait::...` imports to `use kv_storage_api::...` (or similar) across the codebase.
- Update integration/unit tests under `rust/tests` to reference the new paths.
- Update `Cargo.toml` workspace members and crate dependencies (add the two new crates, set `rocksdb_server` to depend on them).

## Validation Checklist
- [ ] `cargo fmt`
- [ ] `cargo check`
- [ ] Server binaries still compile.
- [ ] No behavioral diffs (logic untouched).

## Notes
- Keep commit history clean by grouping moves + path updates per step.
- Defer further refactors (new traits, feature flags, in-memory engine) until after this split.
