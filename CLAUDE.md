# CLAUDE.md - KV Store Multi-Language Implementation

This file provides guidance to Claude Code when working with the high-performance key-value store service.

## Project Overview

This is a transactional key-value store service with implementations in multiple languages, supporting both gRPC and Thrift protocols, using RocksDB as the storage engine.

## Language-Specific Documentation

- **Rust Implementation**: See [rust/CLAUDE.md](rust/CLAUDE.md) for Rust-specific guidance including async Tokio implementation, client SDK with C FFI bindings, and build instructions.
- **Go Implementation**: Documentation for Go gRPC/Thrift servers and clients
- **C++ Implementation**: Documentation for C++ gRPC servers and protocol compatibility