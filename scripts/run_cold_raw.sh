#!/bin/bash

# Efficient Cold-Page Raw Benchmark
# - Makes RocksDB block cache tiny so the working set exceeds it (cold at block-cache level)
# - Uses a fresh DB path to avoid reuse
# - No root required; fast and compute-light

set -euo pipefail

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"

info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }

if [[ ! -x "$BIN_DIR/benchmark" ]]; then
  error "Missing bin/benchmark. Run 'make build' first from repo root."
  exit 1
fi

# Defaults (can be overridden via env or flags)
THREADS=${THREADS:-8}
REQUESTS=${REQUESTS:-100000}
PREPOP=${PREPOP:-1000000}      # working set >> block cache (1M keys = ~1GB data >> 100KB cache)
VALUE_SIZE=${VALUE_SIZE:-256}
KEY_SIZE=${KEY_SIZE:-16}
MODE=${MODE:-read}             # read-only to emphasize page cache behavior

# Parse minimal flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads|-t) THREADS="$2"; shift 2 ;;
    --requests|-r) REQUESTS="$2"; shift 2 ;;
    --prepopulate|-p) PREPOP="$2"; shift 2 ;;
    --value-size|-v) VALUE_SIZE="$2"; shift 2 ;;
    --key-size|-k) KEY_SIZE="$2"; shift 2 ;;
    --mode|-m) MODE="$2"; shift 2 ;;
    --help|-h)
      cat <<EOF
Efficient Cold-Page Raw Benchmark

Usage: $0 [options]

Options:
  -t, --threads N        Concurrent threads (default: $THREADS)
  -r, --requests N       Total requests (default: $REQUESTS)
  -p, --prepopulate N    Keys to prepopulate (default: $PREPOP)
  -v, --value-size B     Value size in bytes (default: $VALUE_SIZE)
  -k, --key-size B       Key size in bytes (default: $KEY_SIZE)
  -m, --mode MODE        read|mixed (default: $MODE)

Notes:
  - This config forces “cold” at RocksDB block-cache level by shrinking block cache.
  - For true OS-page cold, you’d need to drop OS caches (root), which this script avoids.
EOF
      exit 0
      ;;
    *)
      error "Unknown option: $1"; exit 1 ;;
  esac
done

TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
RESULTS_DIR="$ROOT_DIR/benchmark_results/run_${TIMESTAMP}_cold_raw"
mkdir -p "$RESULTS_DIR"
DB_DIR="$ROOT_DIR/scripts/data/rocksdb-cold-$TIMESTAMP"
mkdir -p "$DB_DIR"

info "Setting up cold block cache DB profile..."
CONFIG_PATH="$ROOT_DIR/configs/db/cold_block_cache.toml"
ok "Using DB profile: $CONFIG_PATH"

OUTPUT_JSON="$RESULTS_DIR/raw_${MODE}_cold_${THREADS}t.json"

info "Running raw ${MODE} benchmark with cold block-cache (threads=$THREADS, requests=$REQUESTS, prepopulate=$PREPOP, value_size=$VALUE_SIZE)"
"$BIN_DIR/benchmark" \
  -protocol=raw \
  -addr="$DB_DIR" \
  -mode="$MODE" \
  -threads="$THREADS" \
  -requests="$REQUESTS" \
  -key-size="$KEY_SIZE" \
  -value-size="$VALUE_SIZE" \
  -prepopulate="$PREPOP" \
  -no-cache-read=true \
  -config="$CONFIG_PATH" \
  -json="$OUTPUT_JSON"

ok "Done. Results: $OUTPUT_JSON"

info "Cold-page note: This run ensures cold at RocksDB block-cache level without root."
info "To pursue colder OS page cache too, consider dropping caches between prepopulate and run (requires root)."
