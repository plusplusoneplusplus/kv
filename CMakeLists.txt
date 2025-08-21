# CMakeLists.txt for kv client library integration
cmake_minimum_required(VERSION 3.12)

# Define the Rust client library paths
set(KV_CLIENT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/client")
set(KV_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/rust/client/include")

# Determine target directory based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_BUILD_TYPE "debug")
    set(CARGO_FLAGS "")
else()
    set(CARGO_BUILD_TYPE "release")
    set(CARGO_FLAGS "--release")
endif()

set(KV_LIB_DIR "${KV_CLIENT_DIR}/target/${CARGO_BUILD_TYPE}")

# Set library name based on platform
if(WIN32)
    set(KV_LIB_NAME "kvstore_client.dll")
    set(KV_STATIC_LIB_NAME "kvstore_client.lib")
else()
    set(KV_LIB_NAME "libkvstore_client.so")
    set(KV_STATIC_LIB_NAME "libkvstore_client.a")
endif()

# Build the Rust library using cargo
add_custom_target(kv_client_rust ALL
    COMMAND cargo build ${CARGO_FLAGS} --features ffi
    WORKING_DIRECTORY ${KV_CLIENT_DIR}
    COMMENT "Building kv client library with Rust/Cargo"
    BYPRODUCTS ${KV_LIB_DIR}/${KV_LIB_NAME} ${KV_LIB_DIR}/${KV_STATIC_LIB_NAME}
)

# Create imported target for the library
add_library(kv_client SHARED IMPORTED GLOBAL)
set_target_properties(kv_client PROPERTIES
    IMPORTED_LOCATION ${KV_LIB_DIR}/${KV_LIB_NAME}
    INTERFACE_INCLUDE_DIRECTORIES ${KV_INCLUDE_DIR}
)

# Add dependency so the library is built before anything tries to use it
add_dependencies(kv_client kv_client_rust)

# For static linking option (if needed)
add_library(kv_client_static STATIC IMPORTED GLOBAL)
set_target_properties(kv_client_static PROPERTIES
    IMPORTED_LOCATION ${KV_LIB_DIR}/${KV_STATIC_LIB_NAME}
    INTERFACE_INCLUDE_DIRECTORIES ${KV_INCLUDE_DIR}
)
add_dependencies(kv_client_static kv_client_rust)

# Create an alias for easier usage
add_library(kv::client ALIAS kv_client)