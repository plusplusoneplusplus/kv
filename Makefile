
.PHONY: build build-release clean proto thrift

# Build flags
DEBUG_FLAGS = -gcflags="all=-N -l"
RELEASE_FLAGS = -ldflags="-s -w" -gcflags="all=-l -B"

# Create bin directory if it doesn't exist
BIN_DIR = ./bin
$(BIN_DIR):
	mkdir -p $(BIN_DIR)


# Build all (debug mode)
build: proto thrift $(BIN_DIR)
	go build $(DEBUG_FLAGS) -o $(BIN_DIR)/rocksdbserver go/main.go
	go build $(DEBUG_FLAGS) -o $(BIN_DIR)/client go/client.go
	go build $(DEBUG_FLAGS) -o $(BIN_DIR)/benchmark go/benchmark.go
	cd rust && cargo build --bin server && cp target/debug/server ../$(BIN_DIR)/rocksdbserver-rust
	cd rust && cargo build --bin thrift-server && cp target/debug/thrift-server ../$(BIN_DIR)/rocksdbserver-thrift
	cd cpp && make debug && cp build/rocksdbserver-cpp ../$(BIN_DIR)/rocksdbserver-cpp

# Build all (release mode)
build-release: proto thrift $(BIN_DIR)
	go build $(RELEASE_FLAGS) -o $(BIN_DIR)/rocksdbserver go/main.go
	go build $(RELEASE_FLAGS) -o $(BIN_DIR)/client go/client.go
	go build $(RELEASE_FLAGS) -o $(BIN_DIR)/benchmark go/benchmark.go
	cd rust && cargo build --release --bin server && cp target/release/server ../$(BIN_DIR)/rocksdbserver-rust
	cd rust && cargo build --release --bin thrift-server && cp target/release/thrift-server ../$(BIN_DIR)/rocksdbserver-thrift
	cd cpp && make release && cp build/rocksdbserver-cpp ../$(BIN_DIR)/rocksdbserver-cpp

# Generate protobuf files
proto:
	./generate.sh

# Generate thrift files
thrift:
	thrift --gen rs -out rust/src thrift/kvstore.thrift


# Clean build artifacts
clean:
	rm -rf $(BIN_DIR)
	rm -f rocksdbserver client benchmark rocksdbserver-rust rocksdbserver-cpp rocksdbserver-thrift
	rm -f rust/src/kvstore.rs
	cd rust && cargo clean 2>/dev/null || true
	cd cpp && make clean 2>/dev/null || true

# Install Go dependencies
go-deps:
	go mod tidy && go mod download

# Install Rust dependencies
rust-deps:
	cd rust && cargo fetch

# Install C++ dependencies
cpp-deps:
	cd cpp && make install-deps
