# CLAUDE.md - KV Store Implementation

This file provides guidance to Claude Code when working with the high-performance key-value store service.

## Project Overview

This is a transactional key-value store service implemented in Rust, supporting both gRPC and Thrift protocols, using RocksDB as the storage engine. The implementation uses a unified workspace structure combining server and client components for better code sharing and maintainability.

## Key Changes (Recent)

The project has been refactored to use a unified Rust workspace structure:
- **Consolidated Structure**: Client and server are now in a single Rust crate (`rust/`)
- **Unified Testing**: All tests are organized under `rust/tests/` with subdirectories
- **Shared Code**: Protocol definitions and utilities are shared between client and server
- **Simplified Build**: Single `cargo build` command builds everything
- **FFI Support**: C FFI bindings integrated into main library with `--features ffi`

## Language-Specific Documentation

- **Rust Implementation**: See [rust/CLAUDE.md](rust/CLAUDE.md) for Rust-specific guidance including the unified workspace structure, async Tokio implementation, client SDK with C FFI bindings, and build instructions.

## Build and Test
1. Run cmake --build build in the git root to build. Give enough timeout like 15 min.
2. Run ./build/bin/rocksdbserver-thrift to launch the thrift server
3. Run cargo test --workspace under rust/ to run rust tests
4. Run ./build/bin/cpp_ffi_test to run c++ tests