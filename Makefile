.PHONY: all build clean proto go-server go-client go-benchmark go-release go-debug rust-server rust-release rust-debug

# Build flags
DEBUG_FLAGS = -gcflags="all=-N -l"
RELEASE_FLAGS = -ldflags="-s -w" -gcflags="all=-l -B"

# Build all Go binaries (debug mode by default)
all: proto go-server go-client go-benchmark

# Build Rust server
rust: rust-server

# Build all Go binaries in release/optimized mode
go-release: proto go-server-release go-client-release go-benchmark-release

# Build Rust server in release mode
rust-release: rust-server-release

# Build all Go binaries in debug mode (explicit)
go-debug: proto go-server-debug go-client-debug go-benchmark-debug

# Build Rust server in debug mode
rust-debug: rust-server

# Generate protobuf files
proto:
	./generate.sh

# Build the Go server binary (renamed to rocksdbserver) - debug mode
go-server:
	go build $(DEBUG_FLAGS) -o rocksdbserver go/main.go

# Build the Go server binary - release mode
go-server-release:
	go build $(RELEASE_FLAGS) -o rocksdbserver go/main.go

# Build the Go server binary - debug mode (explicit)
go-server-debug:
	go build $(DEBUG_FLAGS) -o rocksdbserver go/main.go

# Build the Go client binary - debug mode
go-client:
	go build $(DEBUG_FLAGS) -o client go/client.go

# Build the Go client binary - release mode
go-client-release:
	go build $(RELEASE_FLAGS) -o client go/client.go

# Build the Go client binary - debug mode (explicit)
go-client-debug:
	go build $(DEBUG_FLAGS) -o client go/client.go

# Build the Go benchmark client - debug mode
go-benchmark:
	go build $(DEBUG_FLAGS) -o benchmark go/benchmark.go

# Build the Go benchmark client - release mode
go-benchmark-release:
	go build $(RELEASE_FLAGS) -o benchmark go/benchmark.go

# Build the Go benchmark client - debug mode (explicit)
go-benchmark-debug:
	go build $(DEBUG_FLAGS) -o benchmark go/benchmark.go

# Build the Rust server binary - debug mode
rust-server:
	cd rust && cargo build --bin server && cp target/debug/server ../rocksdbserver-rust

# Build the Rust server binary - release mode
rust-server-release:
	cd rust && cargo build --release --bin server && cp target/release/server ../rocksdbserver-rust

# Clean build artifacts
clean:
	rm -f rocksdbserver client benchmark rocksdbserver-rust
	cd rust && cargo clean 2>/dev/null || true

# Install Go dependencies
go-deps:
	go mod tidy && go mod download

# Install Rust dependencies
rust-deps:
	cd rust && cargo fetch
