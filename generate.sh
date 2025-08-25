#!/bin/bash

# Script to generate Go protobuf files for the KV store

set -e

# Create output directory
mkdir -p go/proto

# Add Go bin to PATH
export PATH="$PATH:$(go env GOPATH)/bin"

# Generate Go protobuf files
protoc \
  --proto_path=proto \
  --go_out=go/proto \
  --go_opt=paths=source_relative \
  --go-grpc_out=go/proto \
  --go-grpc_opt=paths=source_relative \
  proto/kvstore.proto

echo "Generated Go protobuf files successfully"