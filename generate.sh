#!/bin/bash

# Install protoc if not already installed
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Please install Protocol Buffers compiler."
    echo "On Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "On macOS: brew install protobuf"
    exit 1
fi

# Install Go plugins for protoc
cd go
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
cd ..

# Create kvstore directory for generated Go files in the go folder
mkdir -p go/kvstore

# Get GOPATH and add bin to PATH
export PATH="$(go env GOPATH)/bin:$PATH"

# Generate Go code from protobuf
protoc --go_out=go --go_opt=paths=source_relative \
    --go-grpc_out=go --go-grpc_opt=paths=source_relative \
    proto/kvstore.proto

# Generate C++ code from protobuf (will be handled by CMake)
# The C++ protobuf generation is handled in the CMakeLists.txt file
# to ensure proper integration with the build system

echo "Generated gRPC code successfully!"
