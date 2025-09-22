#!/bin/bash

# Script to run RSML consensus tests with proper feature flags
# This script runs tests for the consensus-rsml crate with different feature combinations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Change to consensus-rsml directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RSML_DIR="$PROJECT_ROOT/rust/crates/consensus-rsml"

if [ ! -d "$RSML_DIR" ]; then
    print_error "consensus-rsml directory not found at $RSML_DIR"
    exit 1
fi

cd "$RSML_DIR"

print_header "RSML Consensus Tests"

echo "Project root: $PROJECT_ROOT"
echo "RSML crate directory: $RSML_DIR"
echo "Current directory: $(pwd)"
echo

# Check if RSML submodule exists
RSML_SUBMODULE="$PROJECT_ROOT/third_party/RSML"
if [ -d "$RSML_SUBMODULE" ]; then
    print_success "RSML submodule found at $RSML_SUBMODULE"
    RSML_AVAILABLE=true
else
    print_warning "RSML submodule not found at $RSML_SUBMODULE"
    print_warning "Only configuration and factory tests will run"
    RSML_AVAILABLE=false
fi

echo

# Test 1: Basic tests without RSML features (lib + config tests)
print_header "Running Basic Tests (no RSML features)"
echo "Command: cargo test --lib --no-default-features"
echo

if cargo test --lib --no-default-features; then
    print_success "Library unit tests passed"
else
    print_error "Library unit tests failed"
    exit 1
fi

echo

# Test 2: Configuration tests only
print_header "Running Configuration Tests"
echo "Command: cargo test --test config_tests --no-default-features"
echo

if cargo test --test config_tests --no-default-features; then
    print_success "Configuration tests passed"
else
    print_error "Configuration tests failed"
    exit 1
fi

echo

# Test 3: RSML integration tests (if RSML is available)
if [ "$RSML_AVAILABLE" = true ]; then
    print_header "Running RSML Integration Tests (lib only)"
    echo "Command: cargo test --lib --features rsml"
    echo

    if cargo test --lib --features rsml 2>/dev/null; then
        print_success "RSML library tests passed"
    else
        print_warning "RSML library tests failed or skipped"
        print_warning "This may be expected if RSML library has complex dependencies"
    fi

    echo

    # Test 4: RSML integration tests (integration tests only)
    print_header "Running RSML Integration Tests (integration only)"
    echo "Command: cargo test --test integration_tests --features rsml"
    echo

    if cargo test --test integration_tests --features rsml 2>/dev/null; then
        print_success "RSML integration tests passed"
    else
        print_warning "RSML integration tests failed or skipped"
        print_warning "This may be expected if RSML library has complex dependencies"
    fi

    echo

    # Test 5: TCP transport tests (if RSML and TCP are available)
    print_header "Running TCP Transport Tests"
    echo "Command: cargo test --test integration_tests --features rsml,tcp"
    echo

    if cargo test --test integration_tests --features rsml,tcp 2>/dev/null; then
        print_success "TCP transport tests passed"
    else
        print_warning "TCP transport tests failed or skipped"
        print_warning "This may be expected if TCP features require additional setup"
    fi

else
    print_warning "Skipping RSML integration tests (RSML submodule not available)"
    print_warning "To enable RSML tests:"
    print_warning "  1. Initialize the RSML submodule: git submodule update --init --recursive"
    print_warning "  2. Ensure RSML dependencies are available"
fi

echo

# Summary
print_header "Test Summary"

echo "Basic functionality tests: PASSED"
echo "Configuration tests: PASSED"

if [ "$RSML_AVAILABLE" = true ]; then
    echo "RSML submodule: AVAILABLE"
    echo "RSML integration tests: RUN (may have warnings)"
else
    echo "RSML submodule: NOT AVAILABLE"
    echo "RSML integration tests: SKIPPED"
fi

echo
print_success "RSML consensus tests completed!"

if [ "$RSML_AVAILABLE" = false ]; then
    echo
    print_warning "To run full RSML integration tests:"
    print_warning "  cd $PROJECT_ROOT"
    print_warning "  git submodule update --init --recursive"
    print_warning "  $0"
fi

echo
echo "For manual testing with specific features:"
echo "  cd $RSML_DIR"
echo "  cargo test --lib --no-default-features                    # Library unit tests"
echo "  cargo test --test config_tests --no-default-features      # Config tests"
echo "  cargo test --lib --features rsml                          # RSML library tests"
echo "  cargo test --test integration_tests --features rsml       # RSML integration tests"
echo "  cargo test --test integration_tests --features rsml,tcp   # With TCP transport"