#!/bin/bash

# install.sh - Install script for RocksDB Key-Value Service
# Installs all required dependencies for Go, Rust, gRPC, and Thrift implementations

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if command -v apt-get &> /dev/null; then
            OS="ubuntu"
        elif command -v yum &> /dev/null; then
            OS="rhel"
        elif command -v dnf &> /dev/null; then
            OS="fedora"
        else
            log_error "Unsupported Linux distribution"
            exit 1
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    else
        log_error "Unsupported operating system: $OSTYPE"
        exit 1
    fi
    log_info "Detected OS: $OS"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install system packages for Ubuntu/Debian
install_ubuntu_packages() {
    log_info "Installing system packages for Ubuntu/Debian..."
    
    sudo apt update
    
    # Install required packages
    sudo apt install -y \
        build-essential \
        pkg-config \
        librocksdb-dev \
        protobuf-compiler \
        libprotobuf-dev \
        thrift-compiler \
        git \
        curl \
        wget
    
    # Optional: gRPC C++ libraries (for C++ implementation)
    if [[ "${INSTALL_CPP:-no}" == "yes" ]]; then
        log_info "Installing C++ gRPC libraries..."
        sudo apt install -y libgrpc++-dev libgrpc-dev protobuf-compiler-grpc
    fi
}

# Install system packages for macOS
install_macos_packages() {
    log_info "Installing system packages for macOS..."
    
    if ! command_exists brew; then
        log_error "Homebrew not found. Please install Homebrew first:"
        log_error "  /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    
    brew install rocksdb protobuf thrift git
    
    # Install Go and Rust if not already installed
    if ! command_exists go; then
        brew install go
    fi
    
    if ! command_exists rustc; then
        brew install rust
    fi
}

# Install system packages for RHEL/CentOS/Fedora
install_rhel_packages() {
    log_info "Installing system packages for RHEL/CentOS/Fedora..."
    
    # Enable EPEL for RHEL/CentOS
    if [[ "$OS" == "rhel" ]]; then
        if command_exists yum; then
            sudo yum install -y epel-release
            PACKAGE_MANAGER="yum"
        else
            sudo dnf install -y epel-release
            PACKAGE_MANAGER="dnf"
        fi
    else
        PACKAGE_MANAGER="dnf"
    fi
    
    # Install packages
    sudo $PACKAGE_MANAGER install -y \
        gcc gcc-c++ make \
        pkg-config \
        rocksdb-devel \
        protobuf-compiler \
        protobuf-devel \
        thrift \
        git \
        curl \
        wget
}

# Install Go
install_go() {
    # Check if Go is in PATH or in /usr/local/go/bin
    if command_exists go || [[ -x "/usr/local/go/bin/go" ]]; then
        if command_exists go; then
            GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
            log_info "Go is already installed and in PATH: $GO_VERSION"
        else
            # Go is installed but not in PATH
            export PATH=$PATH:/usr/local/go/bin
            GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
            log_info "Go found and added to PATH: $GO_VERSION"
        fi
        
        # Install Go protobuf plugins
        install_go_plugins
        return
    fi
    
    log_info "Installing Go..."
    
    # Determine architecture
    ARCH=$(uname -m)
    case $ARCH in
        x86_64) GO_ARCH="amd64" ;;
        arm64|aarch64) GO_ARCH="arm64" ;;
        armv6l) GO_ARCH="armv6l" ;;
        armv7l) GO_ARCH="armv7l" ;;
        *) log_error "Unsupported architecture: $ARCH"; exit 1 ;;
    esac
    
    GO_VERSION="1.21.5"
    GO_TAR="go${GO_VERSION}.linux-${GO_ARCH}.tar.gz"
    
    # Download and install Go
    cd /tmp
    wget "https://go.dev/dl/${GO_TAR}"
    sudo tar -C /usr/local -xzf "${GO_TAR}"
    
    # Add Go to PATH
    if ! grep -q "/usr/local/go/bin" ~/.bashrc; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
        log_info "Added Go to PATH in ~/.bashrc"
    fi
    
    # Add to current session
    export PATH=$PATH:/usr/local/go/bin
    
    log_info "Go installed successfully: $(go version)"
    
    # Install Go protobuf plugins
    install_go_plugins
}

# Install Go protobuf plugins
install_go_plugins() {
    log_info "Installing Go protobuf plugins..."
    
    # Install protoc-gen-go and protoc-gen-go-grpc
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    
    # Add GOPATH/bin to PATH if not already there
    GOPATH=$(go env GOPATH)
    if [[ -n "$GOPATH" ]] && [[ ":$PATH:" != *":$GOPATH/bin:"* ]]; then
        if ! grep -q "GOPATH/bin" ~/.bashrc; then
            echo 'export PATH=$PATH:$(go env GOPATH)/bin' >> ~/.bashrc
            log_info "Added GOPATH/bin to PATH in ~/.bashrc"
        fi
        export PATH=$PATH:$GOPATH/bin
    fi
    
    log_info "Go protobuf plugins installed successfully"
}

# Install Rust
install_rust() {
    if command_exists rustc; then
        RUST_VERSION=$(rustc --version | awk '{print $2}')
        log_info "Rust is already installed: $RUST_VERSION"
        return
    fi
    
    log_info "Installing Rust..."
    
    # Install Rust via rustup
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    
    # Source cargo env
    source ~/.cargo/env
    
    log_info "Rust installed successfully: $(rustc --version)"
}

# Verify installations
verify_installation() {
    log_info "Verifying installations..."
    
    # Check system tools
    local tools=("protoc" "thrift" "pkg-config")
    for tool in "${tools[@]}"; do
        if command_exists "$tool"; then
            log_info "$tool: $(command -v $tool)"
        else
            log_error "$tool not found!"
            exit 1
        fi
    done
    
    # Check Go
    if command_exists go; then
        log_info "Go: $(go version)"
    else
        log_error "Go not found!"
        exit 1
    fi
    
    # Check Rust
    if command_exists rustc; then
        log_info "Rust: $(rustc --version)"
    else
        log_error "Rust not found!"
        exit 1
    fi
    
    # Check RocksDB
    if pkg-config --libs rocksdb >/dev/null 2>&1; then
        log_info "RocksDB: $(pkg-config --modversion rocksdb)"
    else
        log_error "RocksDB development libraries not found!"
        exit 1
    fi
    
    # Check protobuf version
    log_info "Protobuf: $(protoc --version)"
    
    # Check thrift version
    log_info "Thrift: $(thrift --version)"
    
    log_info "All tools verified successfully!"
}

# Main installation function
main() {
    log_info "Starting RocksDB Key-Value Service installation..."
    
    # Detect OS
    detect_os
    
    # Install system packages based on OS
    case $OS in
        ubuntu)
            install_ubuntu_packages
            ;;
        macos)
            install_macos_packages
            ;;
        rhel|fedora)
            install_rhel_packages
            ;;
    esac
    
    # Install Go and Rust (skip on macOS if using brew)
    if [[ "$OS" != "macos" ]]; then
        install_go
        install_rust
    fi
    
    # Verify everything is installed correctly
    verify_installation
    
    log_info "Installation completed successfully!"
    log_info ""
    log_info "IMPORTANT: You must restart your shell or source your environment:"
    log_info "  source ~/.bashrc"
    log_info "  source ~/.cargo/env"
    log_info "  # OR restart your terminal"
    log_info ""
    log_info "Then verify installation:"
    log_info "  go version"
    log_info "  rustc --version"
    log_info "  protoc --version"
    log_info ""
    log_info "Next steps:"
    log_info "  1. Build the project:"
    log_info "     make build-release"
    log_info "  2. Run tests:"
    log_info "     ./bin/rocksdbserver &"
    log_info "     ./bin/client -op ping"
    log_info "     kill %1"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --with-cpp)
            INSTALL_CPP="yes"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Install all dependencies for RocksDB Key-Value Service"
            echo ""
            echo "Options:"
            echo "  --with-cpp    Install C++ gRPC libraries (Ubuntu/Debian only)"
            echo "  --help, -h    Show this help message"
            echo ""
            echo "Supported platforms:"
            echo "  - Ubuntu/Debian 20.04+"
            echo "  - CentOS/RHEL 7+"
            echo "  - Fedora 35+"
            echo "  - macOS (with Homebrew)"
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main installation
main