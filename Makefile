
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
	go build $(DEBUG_FLAGS) -o $(BIN_DIR)/rocksdbserver go/main.go go/server.go
	go build $(DEBUG_FLAGS) -o $(BIN_DIR)/client go/client.go
	cd benchmark-rust && cargo build --bin benchmark && cp target/debug/benchmark ../$(BIN_DIR)/benchmark
	cd rust && cargo build --bin server && cp target/debug/server ../$(BIN_DIR)/rocksdbserver-rust
	cd rust && cargo build --bin thrift-server && cp target/debug/thrift-server ../$(BIN_DIR)/rocksdbserver-thrift
# Disabled Go benchmark - replaced with Rust version
# cd benchmark && go build $(DEBUG_FLAGS) -o ../$(BIN_DIR)/benchmark .
# ignore c++
# cd cpp && make debug && cp build/rocksdbserver-cpp ../$(BIN_DIR)/rocksdbserver-cpp

# Build all (release mode)
build-release: proto thrift $(BIN_DIR)
	go build $(RELEASE_FLAGS) -o $(BIN_DIR)/rocksdbserver go/main.go go/server.go
	go build $(RELEASE_FLAGS) -o $(BIN_DIR)/client go/client.go
	cd benchmark-rust && cargo build --release --bin benchmark && cp target/release/benchmark ../$(BIN_DIR)/benchmark
	cd rust && cargo build --release --bin server && cp target/release/server ../$(BIN_DIR)/rocksdbserver-rust
	cd rust && cargo build --release --bin thrift-server && cp target/release/thrift-server ../$(BIN_DIR)/rocksdbserver-thrift
# Disabled Go benchmark - replaced with Rust version
# cd benchmark && go build $(RELEASE_FLAGS) -o ../$(BIN_DIR)/benchmark .
# ignore c++
# cd cpp && make release && cp build/rocksdbserver-cpp ../$(BIN_DIR)/rocksdbserver-cpp

# Generate protobuf files
proto:
	./generate.sh

# Generate thrift files
thrift:
	mkdir -p go/thrift
	thrift --gen go -out go/thrift thrift/kvstore.thrift
	thrift --gen rs -out rust/src thrift/kvstore.thrift


# Clean build artifacts
clean:
	rm -rf $(BIN_DIR)
	rm -f rocksdbserver client rocksdbserver-rust rocksdbserver-cpp rocksdbserver-thrift
	rm -rf go/thrift/
	rm -f rust/src/kvstore.rs
	cd rust && cargo clean 2>/dev/null || true
	cd benchmark-rust && cargo clean 2>/dev/null || true
	cd cpp && make clean 2>/dev/null || true

# Install Go dependencies
go-deps:
	go mod tidy && go mod download

# Install Rust dependencies
rust-deps:
	cd rust && cargo fetch
	cd benchmark-rust && cargo fetch

# Install C++ dependencies
cpp-deps:
	cd cpp && make install-deps
