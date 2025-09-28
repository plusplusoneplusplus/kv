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
find_program(CARGO_EXECUTABLE cargo REQUIRED)

# Google Test - check if targets already exist from parent project
if(NOT TARGET gtest)
  include(FetchContent)
  FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG release-1.12.1
  )
  # For Windows: Prevent overriding the parent project's compiler/linker settings
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

# Include utilities
include(ExternalProject)
include(ProcessorCount)

#==============================================================================
# Protocol Generation
#==============================================================================

# Protocol Buffers Generation
set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_DIR}/kvstore.proto")



# Thrift Generation
set(THRIFT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/thrift")
set(THRIFT_FILE "${THRIFT_DIR}/kvstore.thrift")


# Generated files directory - Use source tree for better organization
set(GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}")

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

# Generate Node.js thrift files
set(NODEJS_GENERATED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/generated")
add_custom_command(
    OUTPUT "${NODEJS_GENERATED_DIR}/kvstore_types.js"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${NODEJS_GENERATED_DIR}"
    COMMAND ${THRIFT_COMPILER} --gen js:node -out "${NODEJS_GENERATED_DIR}" "${THRIFT_FILE}"
    DEPENDS ${THRIFT_FILE}
    COMMENT "Generating Node.js thrift files"
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

# Create protocol generation targets
add_custom_target(generate_proto ALL)

add_custom_target(generate_thrift ALL
    DEPENDS 
        "${RUST_GENERATED_DIR}/kvstore.rs"
        "${NODEJS_GENERATED_DIR}/kvstore_types.js"
)

# Node.js build target to copy generated files
add_custom_target(nodejs_app ALL
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${NODEJS_GENERATED_DIR}/kvstore_types.js"
        "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/thrift/kvstore_types.js"
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        "${NODEJS_GENERATED_DIR}/TransactionalKV.js"
        "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/thrift/TransactionalKV.js"
    DEPENDS generate_thrift
    COMMENT "Copying Node.js Thrift files for application"
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

set(CARGO_BUILD_JOB_COUNT "" CACHE STRING "Number of parallel jobs to pass to cargo (-j). Empty uses detected processor count.")
if(CARGO_BUILD_JOB_COUNT STREQUAL "")
    ProcessorCount(_cargo_detected_jobs)
    set(_cargo_jobs ${_cargo_detected_jobs})
else()
    set(_cargo_jobs ${CARGO_BUILD_JOB_COUNT})
endif()

if(_cargo_jobs GREATER 0)
    set(CARGO_BUILD_JOBS_ARGS -j${_cargo_jobs})
else()
    set(CARGO_BUILD_JOBS_ARGS "")
endif()

# Rust gRPC server
add_custom_target(rust_grpc_server ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} ${CARGO_BUILD_JOBS_ARGS} --bin server
    COMMAND ${CMAKE_COMMAND} -E copy 
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/target/${CARGO_TARGET_DIR}/server"
        "${CMAKE_BINARY_DIR}/bin/rocksdbserver-rust"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust gRPC server"
)

# Rust shard server
add_custom_target(rust_shard_server ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} ${CARGO_BUILD_JOBS_ARGS} --bin shard-server
    COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/target/${CARGO_TARGET_DIR}/shard-server"
        "${CMAKE_BINARY_DIR}/bin/shard-server"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust shard server"
)

# Rust benchmark tool
add_custom_target(rust_benchmark ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} ${CARGO_BUILD_JOBS_ARGS} -p kv-benchmark
    COMMAND ${CMAKE_COMMAND} -E copy 
        "${CMAKE_CURRENT_SOURCE_DIR}/rust/target/${CARGO_TARGET_DIR}/benchmark"
        "${CMAKE_BINARY_DIR}/bin/benchmark-rust"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    DEPENDS generate_proto generate_thrift
    COMMENT "Building Rust benchmark tool"
)

#==============================================================================
# Rust Client Library with FFI Support
#==============================================================================

# Define the Rust client library paths (now unified in main rust directory)
set(KV_CLIENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust")
set(KV_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/client")
set(KV_LIB_DIR "${KV_CLIENT_DIR}/target/${CARGO_TARGET_DIR}")

# Set library name based on platform (now using unified rocksdb_server library)
if(WIN32)
    set(KV_LIB_NAME "rocksdb_server.dll")
    set(KV_STATIC_LIB_NAME "rocksdb_server.lib")
elseif(APPLE)
    set(KV_LIB_NAME "librocksdb_server.dylib")
    set(KV_STATIC_LIB_NAME "librocksdb_server.a")
else()
    set(KV_LIB_NAME "librocksdb_server.so")
    set(KV_STATIC_LIB_NAME "librocksdb_server.a")
endif()

# Build the Rust client library
add_custom_target(rust_client_lib ALL
    COMMAND ${CARGO_EXECUTABLE} build ${CARGO_BUILD_FLAGS} ${CARGO_BUILD_JOBS_ARGS} --features ffi
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_BINARY_DIR}/bin"
    # List directory contents to verify source file exists
    COMMAND ls -la "${KV_LIB_DIR}/"
    # Copy with verbose output
    COMMAND cp -v "${KV_LIB_DIR}/${KV_LIB_NAME}" "${CMAKE_BINARY_DIR}/bin/${KV_LIB_NAME}"
    # Verify the copy was successful
    COMMAND ls -la "${CMAKE_BINARY_DIR}/bin/${KV_LIB_NAME}"
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
    IMPORTED_NO_SONAME ON
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
# C++ FFI Tests
#==============================================================================

# Set the cpp_tests directory (now in unified tests directory)
set(CPP_TESTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/tests/cpp_tests")

# Create C++ FFI test executable with all test files
add_executable(cpp_ffi_test
    "${CPP_TESTS_DIR}/main.cpp"
    "${CPP_TESTS_DIR}/test_basic_operations.cpp"
    "${CPP_TESTS_DIR}/test_configuration.cpp"
    "${CPP_TESTS_DIR}/test_binary_data.cpp"
    "${CPP_TESTS_DIR}/test_range_operations.cpp"
    "${CPP_TESTS_DIR}/test_transactions.cpp"
    "${CPP_TESTS_DIR}/test_concurrency.cpp"
    "${CPP_TESTS_DIR}/test_error_handling.cpp"
    "${CPP_TESTS_DIR}/test_versionstamped_operations.cpp"
    "${CPP_TESTS_DIR}/test_async_callbacks.cpp"
)

# Link against the unified Rust library and gtest
target_link_libraries(cpp_ffi_test PRIVATE 
    -L"${CMAKE_BINARY_DIR}/bin" -lrocksdb_server
    gtest_main
    gtest
)
target_include_directories(cpp_ffi_test PRIVATE 
    ${KV_INCLUDE_DIR}
    ${CPP_TESTS_DIR}
)

# Set C++ standard for the tests
set_target_properties(cpp_ffi_test PROPERTIES
    CXX_STANDARD 14
    CXX_STANDARD_REQUIRED ON
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "$ORIGIN"
)

# Ensure the test is built after the client library
add_dependencies(cpp_ffi_test rust_client_lib)

# Copy the test executable to the bin directory
add_custom_command(TARGET cpp_ffi_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        $<TARGET_FILE:cpp_ffi_test>
        "${CMAKE_BINARY_DIR}/bin/cpp_ffi_test"
    COMMENT "Copying C++ FFI test to bin directory"
)

# Add test target for running the C++ FFI tests (requires server running on localhost:9090)
add_custom_target(test_ffi
    COMMAND ${CMAKE_COMMAND} -E env "LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/bin:$ENV{LD_LIBRARY_PATH}" 
            ${CMAKE_BINARY_DIR}/bin/cpp_ffi_test
    DEPENDS cpp_ffi_test
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/bin
    COMMENT "Running C++ FFI tests"
)

#==============================================================================
# Top-Level Build Targets
#==============================================================================

# Main build target (equivalent to "make build")
add_custom_target(build ALL
    DEPENDS 
        generate_proto
        generate_thrift
        rust_grpc_server
        rust_shard_server
        rust_benchmark
        rust_client_lib
        cpp_ffi_test
        nodejs_app
)

#==============================================================================
# Dependency Management Targets
#==============================================================================


# Rust dependencies
add_custom_target(rust_deps
    COMMAND ${CARGO_EXECUTABLE} fetch
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    COMMENT "Installing Rust dependencies (main)"
)

# Benchmark dependencies are now part of the main workspace
# No separate dependency target needed

# All dependencies target (client deps now included in main rust workspace)
add_custom_target(deps
    DEPENDS rust_deps
    COMMENT "Installing all dependencies"
)

#==============================================================================
# Clean Targets
#==============================================================================

# Clean target
add_custom_target(clean_all
    # Clean build directories
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_BINARY_DIR}/bin"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_BINARY_DIR}/generated"
    
    # Clean generated protocol files
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_SOURCE_DIR}/rust/src/generated"
    COMMAND ${CMAKE_COMMAND} -E remove_directory "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/generated"
    
    # Clean copied Node.js thrift files
    COMMAND ${CMAKE_COMMAND} -E remove "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/thrift/kvstore_types.js" || true
    COMMAND ${CMAKE_COMMAND} -E remove "${CMAKE_CURRENT_SOURCE_DIR}/nodejs/thrift/TransactionalKV.js" || true
    
    # Clean Rust build artifacts
    COMMAND ${CARGO_EXECUTABLE} clean || true
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
    COMMENT "Cleaning all build artifacts and generated files"
)

#==============================================================================
# Help Target
#==============================================================================

add_custom_target(build_help
    COMMAND ${CMAKE_COMMAND} -E echo "Available targets:"
    COMMAND ${CMAKE_COMMAND} -E echo "  build          - Build all components (default)"
    COMMAND ${CMAKE_COMMAND} -E echo "  deps           - Install all dependencies"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_deps      - Install Rust dependencies"
    COMMAND ${CMAKE_COMMAND} -E echo "  generate_proto - Generate protobuf files"
    COMMAND ${CMAKE_COMMAND} -E echo "  generate_thrift- Generate thrift files"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_grpc_server   - Build Rust gRPC server"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_shard_server  - Build Rust shard server"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_benchmark - Build Rust benchmark tool"
    COMMAND ${CMAKE_COMMAND} -E echo "  rust_client_lib- Build Rust client library with FFI"
    COMMAND ${CMAKE_COMMAND} -E echo "  cpp_ffi_test   - Build C++ FFI tests (organized by category)"
    COMMAND ${CMAKE_COMMAND} -E echo "  test_ffi       - Run C++ FFI tests (requires server on localhost:9090)"
    COMMAND ${CMAKE_COMMAND} -E echo "  clean_all      - Clean all build artifacts and generated files"
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
