.PHONY: all build test clean release debug c-example install check

# Default target
all: build

# Build debug version
build:
	cargo build

# Build release version
release:
	cargo build --release

# Build with FFI features
ffi:
	cargo build --release --features ffi

# Run tests
test:
	cargo test



# Check code quality
check:
	cargo check
	cargo clippy -- -D warnings
	cargo fmt --check

# Format code
fmt:
	cargo fmt

# Clean build artifacts
clean:
	cargo clean

# Install locally (for development)
install:
	cargo install --path . --force

# Generate documentation
docs:
	cargo doc --open

# Run benchmarks (if any)
bench:
	cargo bench

# Package for distribution
package:
	cargo package

# Check dependencies for security issues
audit:
	cargo audit



# Coverage report (requires cargo-tarpaulin)
coverage:
	cargo tarpaulin --out Html --output-dir coverage

help:
	@echo "Available targets:"
	@echo "  build      - Build debug version"
	@echo "  release    - Build release version"
	@echo "  ffi        - Build with C FFI support"
	@echo "  test       - Run tests"
	@echo "  check      - Run code quality checks"
	@echo "  fmt        - Format code"
	@echo "  clean      - Clean build artifacts"
	@echo "  docs       - Generate documentation"
	@echo "  package    - Package for distribution"
	@echo "  coverage   - Generate coverage report"
	@echo "  help       - Show this help"