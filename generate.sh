#!/bin/bash

# Install protoc if not already installed
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Please install Protocol Buffers compiler."
    echo "On Ubuntu/Debian: sudo apt-get install protobuf-compiler"
    echo "On macOS: brew install protobuf"
    exit 1
fi

# Install Go plugins for protoc
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

# Create kvstore directory for generated Go files
mkdir -p kvstore

# Generate Go code from protobuf
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/kvstore.proto

echo "Generated gRPC code successfully!"
