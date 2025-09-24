# Thrift Code Generation Process

This document explains how Thrift protocol definitions are automatically converted to language-specific bindings in the KV store project.

## Overview

The project uses Apache Thrift to define protocol interfaces in a language-neutral format, then automatically generates language-specific client and server code during the build process.

## Source and Output Files

### Input
- **Source File**: `thrift/kvstore.thrift`
- **Format**: Thrift IDL (Interface Definition Language)
- **Contains**: Service definitions, data structures, enums, exceptions

### Generated Output
- **Rust**: `rust/src/generated/kvstore.rs`
- **Node.js**: `nodejs/generated/kvstore_types.js` and `TransactionalKV.js`

## Build Integration

### CMake Configuration (CMakeLists.txt)

The generation process is configured in the project's CMakeLists.txt:

```cmake
# Thrift Generation
set(THRIFT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/thrift")
set(THRIFT_FILE "${THRIFT_DIR}/kvstore.thrift")

# Generate Rust thrift files directly into source generated folders
set(RUST_GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/generated")

# Generate Rust thrift files (unified for both server and client)
add_custom_command(
    OUTPUT "${RUST_GENERATED_DIR}/kvstore.rs"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${RUST_GENERATED_DIR}"
    COMMAND ${THRIFT_COMPILER} --gen rs -out "${RUST_GENERATED_DIR}" "${THRIFT_FILE}"
    DEPENDS ${THRIFT_FILE}
    COMMENT "Generating Rust thrift files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
```

### Generation Targets

```cmake
add_custom_target(generate_thrift ALL
    DEPENDS
        "${RUST_GENERATED_DIR}/kvstore.rs"
        "${NODEJS_GENERATED_DIR}/kvstore_types.js"
)
```

## Workflow

### Automatic Generation
1. **Trigger**: Any change to `thrift/kvstore.thrift`
2. **Build**: Run `cmake --build build`
3. **Process**: CMake detects the dependency and runs thrift compiler
4. **Output**: Generated files are updated automatically
5. **Integration**: Rust code imports via `mod generated;`

### Manual Generation (if needed)
```bash
# Rust bindings
thrift --gen rs -out rust/src/generated thrift/kvstore.thrift

# Node.js bindings
thrift --gen js:node -out nodejs/generated thrift/kvstore.thrift
```

## Language-Specific Details

### Rust Generation
- **Compiler**: `thrift --gen rs`
- **Output**: Single file `kvstore.rs` with all definitions
- **Features**: Structs, enums, service traits, client implementations
- **Integration**: Used via `use crate::generated::kvstore::*;`

### Node.js Generation
- **Compiler**: `thrift --gen js:node`
- **Output**: Multiple files (types, service implementations)
- **Features**: Service clients, type definitions
- **Integration**: Required in Node.js applications

## Adding New Definitions

### Process
1. **Edit** `thrift/kvstore.thrift` to add new structures or services
2. **Build** using `cmake --build build`
3. **Verify** generated files are updated
4. **Implement** the new service methods in language-specific servers
5. **Test** the new functionality

### Example: Adding Consensus Service
```thrift
// Add to kvstore.thrift
struct AppendEntriesRequest {
    1: required i64 term,
    2: required i32 leader_id,
    // ... other fields
}

service ConsensusService {
    AppendEntriesResponse appendEntries(1: AppendEntriesRequest request),
}
```

After build, this generates Rust structs and traits automatically.

## Troubleshooting

### Common Issues
- **Missing thrift compiler**: Install Apache Thrift tools
- **Generation not triggered**: Clean and rebuild with `cmake --build build --target clean_all`
- **Import errors**: Ensure `mod generated;` is present in Rust modules

### Verification
```bash
# Check if files were generated
ls -la rust/src/generated/
ls -la nodejs/generated/

# Verify thrift compiler is available
thrift --version
```

## Dependencies

### Build Requirements
- **Apache Thrift Compiler**: `thrift` command available in PATH
- **CMake**: Version 3.12 or higher
- **Languages**: Rust (cargo), Node.js for respective bindings

### Runtime Requirements
- **Rust**: thrift crate for protocol support
- **Node.js**: thrift npm package for protocol support