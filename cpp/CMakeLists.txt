cmake_minimum_required(VERSION 3.16)
project(rocksdb_server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find required packages
find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)

# Find gRPC using pkg-config
pkg_check_modules(GRPC REQUIRED grpc++)
pkg_check_modules(GRPC_CPP REQUIRED grpc)

# Find RocksDB manually (Ubuntu package doesn't provide pkg-config)
find_library(ROCKSDB_LIB rocksdb PATHS /usr/lib /usr/local/lib)
if(NOT ROCKSDB_LIB)
    message(FATAL_ERROR "RocksDB library not found")
endif()

set(ROCKSDB_INCLUDE_DIRS /usr/include)
set(ROCKSDB_LIBRARIES ${ROCKSDB_LIB})

# Set up protobuf and gRPC
set(PROTO_FILES "${CMAKE_SOURCE_DIR}/../proto/kvstore.proto")
set(PROTO_SRC_DIR "${CMAKE_BINARY_DIR}/proto")
file(MAKE_DIRECTORY ${PROTO_SRC_DIR})

# Generate protobuf and gRPC files
set(PROTO_SRCS "${PROTO_SRC_DIR}/kvstore.pb.cc")
set(PROTO_HDRS "${PROTO_SRC_DIR}/kvstore.pb.h")
set(GRPC_SRCS "${PROTO_SRC_DIR}/kvstore.grpc.pb.cc")
set(GRPC_HDRS "${PROTO_SRC_DIR}/kvstore.grpc.pb.h")

add_custom_command(
    OUTPUT "${PROTO_SRCS}" "${PROTO_HDRS}" "${GRPC_SRCS}" "${GRPC_HDRS}"
    COMMAND ${Protobuf_PROTOC_EXECUTABLE}
    ARGS --grpc_out "${PROTO_SRC_DIR}"
         --cpp_out "${PROTO_SRC_DIR}"
         -I "${CMAKE_SOURCE_DIR}/../proto"
         --plugin=protoc-gen-grpc=`which grpc_cpp_plugin`
         "${PROTO_FILES}"
    DEPENDS "${PROTO_FILES}")

# Create the executable
add_executable(rocksdbserver-cpp
    src/main.cpp
    src/kvstore_service.cpp
    ${PROTO_SRCS}
    ${GRPC_SRCS}
)

# Include directories
target_include_directories(rocksdbserver-cpp PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${PROTO_SRC_DIR}
    ${ROCKSDB_INCLUDE_DIRS}
    ${GRPC_INCLUDE_DIRS}
)

# Link libraries
target_link_libraries(rocksdbserver-cpp
    ${GRPC_LIBRARIES}
    ${GRPC_CPP_LIBRARIES}
    protobuf::libprotobuf
    ${ROCKSDB_LIBRARIES}
    pthread
)

# Compiler flags
target_compile_options(rocksdbserver-cpp PRIVATE 
    ${GRPC_CFLAGS_OTHER}
)

# Set build type specific flags
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    target_compile_options(rocksdbserver-cpp PRIVATE -g -O0)
else()
    target_compile_options(rocksdbserver-cpp PRIVATE -O3 -DNDEBUG)
endif()
