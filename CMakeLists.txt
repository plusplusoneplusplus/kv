# CMakeLists.txt for KV Store Multi-Language Build System
cmake_minimum_required(VERSION 3.12)
project(kv-store LANGUAGES C CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Build configuration
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug")
endif()

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Create bin directory
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Find required packages
find_package(Protobuf REQUIRED)
find_program(THRIFT_COMPILER thrift REQUIRED)
find_program(GO_EXECUTABLE go REQUIRED)
find_program(CARGO_EXECUTABLE cargo REQUIRED)

# Include utilities
include(ExternalProject)

#==============================================================================
# Protocol Generation
#==============================================================================

# Protocol Buffers Generation
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_DIR}/kvstore.proto")

# Generate Go protobuf files using generate.sh
add_custom_command(
    OUTPUT 
        "${CMAKE_CURRENT_SOURCE_DIR}/go/proto/kvstore.pb.go"
        "${CMAKE_CURRENT_SOURCE_DIR}/go/proto/kvstore_grpc.pb.go"
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/generate.sh
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating Go protobuf files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Generate C++ protobuf files (if needed in the future)
set(CPP_PROTO_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cpp/generated")
add_custom_command(
    OUTPUT 
        "${CPP_PROTO_OUT_DIR}/kvstore.pb.cc"
        "${CPP_PROTO_OUT_DIR}/kvstore.pb.h"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CPP_PROTO_OUT_DIR}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        --proto_path=${PROTO_DIR}
        --cpp_out=${CPP_PROTO_OUT_DIR}
        --grpc_out=${CPP_PROTO_OUT_DIR}
        --plugin=protoc-gen-grpc=grpc_cpp_plugin
        kvstore.proto
    DEPENDS ${PROTO_FILE}
    COMMENT "Generating C++ protobuf files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Thrift Generation
set(THRIFT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/thrift")
set(THRIFT_FILE "${THRIFT_DIR}/kvstore.thrift")

# Generate Go thrift files
set(GO_THRIFT_OUT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/go/thrift")
add_custom_command(
    OUTPUT "${GO_THRIFT_OUT_DIR}/kvstore/kvstore.go"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${GO_THRIFT_OUT_DIR}"
    COMMAND ${THRIFT_COMPILER} --gen go -out "${GO_THRIFT_OUT_DIR}" "${THRIFT_FILE}"
    DEPENDS ${THRIFT_FILE}
    COMMENT "Generating Go thrift files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Generate Rust thrift files
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/kvstore.rs"
    COMMAND ${THRIFT_COMPILER} --gen rs -out "${CMAKE_CURRENT_SOURCE_DIR}/rust/src" "${THRIFT_FILE}"
    DEPENDS ${THRIFT_FILE}
    COMMENT "Generating Rust thrift files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Create protocol generation targets
add_custom_target(generate_proto ALL
    DEPENDS 
        "${CMAKE_CURRENT_SOURCE_DIR}/go/proto/kvstore.pb.go"
        "${CMAKE_CURRENT_SOURCE_DIR}/go/proto/kvstore_grpc.pb.go"
)

add_custom_target(generate_thrift ALL
    DEPENDS 
        "${GO_THRIFT_OUT_DIR}/kvstore/kvstore.go"
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/kvstore.rs"
)

#==============================================================================
# Go Builds
#==============================================================================

# Determine Go build flags based on CMAKE_BUILD_TYPE
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(GO_BUILD_FLAGS "-ldflags=-s -w" "-gcflags=all=-l -B")
else()
    set(GO_BUILD_FLAGS "-gcflags=all=-N -l")
endif()

# Go server
add_custom_target(go_server ALL
    COMMAND ${GO_EXECUTABLE} mod tidy
    COMMAND ${GO_EXECUTABLE} mod download
    COMMAND ${GO_EXECUTABLE} build 
            ${GO_BUILD_FLAGS}
            -o "${CMAKE_BINARY_DIR}/bin/rocksdbserver"
            main.go server.go
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/go"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Go server"
)

# Go client
add_custom_target(go_client ALL
    COMMAND ${GO_EXECUTABLE} mod tidy
    COMMAND ${GO_EXECUTABLE} mod download
    COMMAND ${GO_EXECUTABLE} build
            ${GO_BUILD_FLAGS}
            -o "${CMAKE_BINARY_DIR}/bin/client"
            client.go
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/go"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Go client"
)

#==============================================================================
# Rust Builds
#==============================================================================

# Determine Rust build flags based on CMAKE_BUILD_TYPE
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(CARGO_BUILD_FLAGS "--release")
    set(CARGO_TARGET_DIR "release")
else()
    set(CARGO_BUILD_FLAGS "")
    set(CARGO_TARGET_DIR "debug")
endif()

# Rust gRPC server
add_custom_target(rust_grpc_server ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} --bin server
    COMMAND ${CMAKE_COMMAND} -E copy 
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/target/${CARGO_TARGET_DIR}/server"
        "${CMAKE_BINARY_DIR}/bin/rocksdbserver-rust"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust gRPC server"
)

# Rust Thrift server
add_custom_target(rust_thrift_server ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} --bin thrift-server
    COMMAND ${CMAKE_COMMAND} -E copy 
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/target/${CARGO_TARGET_DIR}/thrift-server"
        "${CMAKE_BINARY_DIR}/bin/rocksdbserver-thrift"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust Thrift server"
)

# Rust benchmark tool
add_custom_target(rust_benchmark ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} --bin benchmark
    COMMAND ${CMAKE_COMMAND} -E copy 
        "${CMAKE_CURRENT_SOURCE_DIR}/benchmark-rust/target/${CARGO_TARGET_DIR}/benchmark"
        "${CMAKE_BINARY_DIR}/bin/benchmark-rust"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/benchmark-rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust benchmark tool"
)

#==============================================================================
# Rust Client Library with FFI Support
#==============================================================================

# Define the Rust client library paths
set(KV_CLIENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/client")
set(KV_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/client/include")
set(KV_LIB_DIR "${KV_CLIENT_DIR}/target/${CARGO_TARGET_DIR}")

# Set library name based on platform
if(WIN32)
    set(KV_LIB_NAME "kvstore_client.dll")
    set(KV_STATIC_LIB_NAME "kvstore_client.lib")
elseif(APPLE)
    set(KV_LIB_NAME "libkvstore_client.dylib")
    set(KV_STATIC_LIB_NAME "libkvstore_client.a")
else()
    set(KV_LIB_NAME "libkvstore_client.so")
    set(KV_STATIC_LIB_NAME "libkvstore_client.a")
endif()

# Build the Rust client library
add_custom_target(rust_client_lib ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} --features ffi
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${KV_LIB_DIR}/${KV_LIB_NAME}"
        "${CMAKE_BINARY_DIR}/bin/${KV_LIB_NAME}"
    WORKING_DIRECTORY ${KV_CLIENT_DIR}
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust client library with FFI support"
    BYPRODUCTS 
        "${KV_LIB_DIR}/${KV_LIB_NAME}" 
        "${KV_LIB_DIR}/${KV_STATIC_LIB_NAME}"
        "${CMAKE_BINARY_DIR}/bin/${KV_LIB_NAME}"
)

# Create imported target for the library
add_library(kv_client SHARED IMPORTED GLOBAL)
set_target_properties(kv_client PROPERTIES
    IMPORTED_LOCATION "${CMAKE_BINARY_DIR}/bin/${KV_LIB_NAME}"
    INTERFACE_INCLUDE_DIRECTORIES ${KV_INCLUDE_DIR}
)

# Add dependency so the library is built before anything tries to use it
add_dependencies(kv_client rust_client_lib)

# For static linking option (if needed)
add_library(kv_client_static STATIC IMPORTED GLOBAL)
set_target_properties(kv_client_static PROPERTIES
    IMPORTED_LOCATION ${KV_LIB_DIR}/${KV_STATIC_LIB_NAME}
    INTERFACE_INCLUDE_DIRECTORIES ${KV_INCLUDE_DIR}
)
add_dependencies(kv_client_static rust_client_lib)

# Create an alias for easier usage
add_library(kv::client ALIAS kv_client)

#==============================================================================
# Top-Level Build Targets
#==============================================================================

# Main build target (equivalent to "make build")
add_custom_target(build ALL
    DEPENDS 
        generate_proto
        generate_thrift
        go_server
        go_client
        rust_grpc_server
        rust_thrift_server
        rust_benchmark
        rust_client_lib
)

#==============================================================================
# Dependency Management Targets
#==============================================================================

# Go dependencies
add_custom_target(go_deps
    COMMAND ${GO_EXECUTABLE} mod tidy
    COMMAND ${GO_EXECUTABLE} mod download
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/go"
    COMMENT "Installing Go dependencies"
)

# Rust dependencies
add_custom_target(rust_deps
    COMMAND ${CARGO_EXECUTABLE} fetch
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    COMMENT "Installing Rust dependencies (main)"
)

add_custom_target(rust_benchmark_deps
    COMMAND ${CARGO_EXECUTABLE} fetch
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/benchmark-rust"
    COMMENT "Installing Rust dependencies (benchmark)"
)

add_custom_target(rust_client_deps
    COMMAND ${CARGO_EXECUTABLE} fetch
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust/client"
    COMMENT "Installing Rust dependencies (client)"
)

# All dependencies target
add_custom_target(deps
    DEPENDS go_deps rust_deps rust_benchmark_deps rust_client_deps
    COMMENT "Installing all dependencies"
)

#==============================================================================
# Clean Targets
#==============================================================================

# Clean target
add_custom_target(clean_all
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_BINARY_DIR}/bin"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_SOURCE_DIR}/go/thrift"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_SOURCE_DIR}/go/proto"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_SOURCE_DIR}/cpp/generated"
    COMMAND ${CMAKE_COMMAND} -E remove "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/kvstore.rs"
    COMMAND ${CARGO_EXECUTABLE} clean || true
    COMMAND ${CARGO_EXECUTABLE} clean || true
    COMMAND ${CARGO_EXECUTABLE} clean || true
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Cleaning all build artifacts"
)

#==============================================================================
# Help Target
#==============================================================================

add_custom_target(build_help
    COMMAND ${CMAKE_COMMAND} -E echo "Available targets:"
    COMMAND ${CMAKE_COMMAND} -E echo "  build          - Build all components (default)"
    COMMAND ${CMAKE_COMMAND} -E echo "  deps           - Install all dependencies"
    COMMAND ${CMAKE_COMMAND} -E echo "  go_deps        - Install Go dependencies"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_deps      - Install Rust dependencies"
    COMMAND ${CMAKE_COMMAND} -E echo "  generate_proto - Generate protobuf files"
    COMMAND ${CMAKE_COMMAND} -E echo "  generate_thrift- Generate thrift files"
    COMMAND ${CMAKE_COMMAND} -E echo "  go_server      - Build Go gRPC server"
    COMMAND ${CMAKE_COMMAND} -E echo "  go_client      - Build Go client"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_grpc_server   - Build Rust gRPC server"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_thrift_server - Build Rust Thrift server"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_benchmark - Build Rust benchmark tool"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_client_lib- Build Rust client library with FFI"
    COMMAND ${CMAKE_COMMAND} -E echo "  clean_all      - Clean all build artifacts"
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMAND ${CMAKE_COMMAND} -E echo "Usage:"
    COMMAND ${CMAKE_COMMAND} -E echo "  cmake -B build -S ."
    COMMAND ${CMAKE_COMMAND} -E echo "  cmake --build build [--target TARGET]"
    COMMAND ${CMAKE_COMMAND} -E echo ""
    COMMAND ${CMAKE_COMMAND} -E echo "For release build:"
    COMMAND ${CMAKE_COMMAND} -E echo "  cmake -B build -S . -DCMAKE_BUILD_TYPE=Release"
    COMMAND ${CMAKE_COMMAND} -E echo "  cmake --build build"
    COMMENT "Displaying help information"
)