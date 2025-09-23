#!/bin/bash

# Single script to manage cluster
# Usage:
#   ./scripts/start_cluster.sh         # Start 3-node cluster on ports 9090-9092
#   ./scripts/start_cluster.sh 1       # Start single node on port 9090
# Press Ctrl+C to stop the cluster

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
declare -a NODE_PIDS=()

# Parse command line arguments
NODE_COUNT=${1:-3}  # Default to 3 nodes if no argument provided

# Validate node count
if [[ ! "$NODE_COUNT" =~ ^[1-9][0-9]*$ ]] || [ "$NODE_COUNT" -gt 10 ]; then
    echo "Error: Node count must be a number between 1 and 10"
    echo "Usage:"
    echo "  $0         # Start 3-node cluster"
    echo "  $0 1       # Start single node"
    echo "  $0 5       # Start 5-node cluster"
    exit 1
fi

# Cleanup function for Ctrl+C
cleanup() {
    echo -e "\n🛑 Shutting down cluster..."

    # Kill tracked processes
    for pid in "${NODE_PIDS[@]}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Stopping node PID $pid..."
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done

    # Wait a moment for graceful shutdown
    sleep 2

    # Force kill any remaining tracked processes
    for pid in "${NODE_PIDS[@]}"; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Force killing PID $pid..."
            kill -KILL "$pid" 2>/dev/null || true
        fi
    done

    # Clean up any remaining processes by name (fallback)
    echo "Cleaning up any remaining processes..."
    pkill -f "rocksdbserver-thrift" 2>/dev/null || true
    pkill -f "node.*server.js" 2>/dev/null || true

    # Clean up log files
    rm -f /tmp/kv-multinode-*.log 2>/dev/null || true
    rm -f /tmp/kv-singlenode-*.log 2>/dev/null || true
    rm -f /tmp/kv-nodejs-*.log 2>/dev/null || true

    echo "✅ Cluster stopped"
    exit 0
}

trap cleanup SIGINT SIGTERM

if [ "$NODE_COUNT" -eq 1 ]; then
    echo "🚀 Starting single-node server..."
else
    echo "🚀 Starting $NODE_COUNT-node cluster..."
fi
echo "Project: $PROJECT_ROOT"
echo "Press Ctrl+C to stop"
echo

# Build server using CMake
cd "$PROJECT_ROOT"
echo "Building..."
cmake --build build --target rust_thrift_server

# Start Node.js web server
echo "Starting Node.js web server..."
cd "$PROJECT_ROOT/nodejs"
nohup node server.js > "/tmp/kv-nodejs-$(date +%Y%m%d-%H%M%S).log" 2>&1 &
NODEJS_PID=$!
NODE_PIDS+=($NODEJS_PID)
echo "Node.js server started on port 3000 (PID: $NODEJS_PID)"
cd "$PROJECT_ROOT"

if [ "$NODE_COUNT" -eq 1 ]; then
    echo "Starting single node on port 9090..."
    # Single node mode - no config file needed
    nohup "$PROJECT_ROOT/build/bin/rocksdbserver-thrift" \
        --port 9090 \
        --verbose \
        > "/tmp/kv-singlenode-$(date +%Y%m%d-%H%M%S).log" 2>&1 &
    NODE_PIDS+=($!)

    echo
    echo "🎉 Single-node server running:"
    echo "  Node.js web server: localhost:3000 (PID: ${NODE_PIDS[0]})"
    echo "  Thrift server: localhost:9090 (PID: ${NODE_PIDS[1]})"
else
    # Multi-node cluster mode
    echo "Generating cluster configs..."
    mkdir -p "$PROJECT_ROOT/build/bin/cluster_configs"

    # Generate endpoint list for cluster
    ENDPOINTS=""
    for ((i=0; i<NODE_COUNT; i++)); do
        port=$((9090 + i))
        if [ $i -eq 0 ]; then
            ENDPOINTS="\"localhost:$port\""
        else
            ENDPOINTS="$ENDPOINTS, \"localhost:$port\""
        fi
    done

    # Generate config files for each node
    for ((node=0; node<NODE_COUNT; node++)); do
        cat > "$PROJECT_ROOT/build/bin/cluster_configs/node_$node.toml" << EOF
# Configuration for Node $node in $NODE_COUNT-node cluster

[database]
base_path = "./data/multi-node-node-$node"

[rocksdb]
write_buffer_size_mb = 32
max_write_buffer_number = 3
block_cache_size_mb = 64
block_size_kb = 4
max_background_jobs = 6
bytes_per_sync = 0
dynamic_level_bytes = true

[bloom_filter]
enabled = true
bits_per_key = 10

[compression]
l0_compression = "lz4"
l1_compression = "lz4"
bottom_compression = "zstd"

[concurrency]
max_read_concurrency = 32

[compaction]
compaction_priority = "min_overlapping_ratio"
target_file_size_base_mb = 64
target_file_size_multiplier = 2
max_bytes_for_level_base_mb = 256
max_bytes_for_level_multiplier = 10

[cache]
cache_index_and_filter_blocks = true
pin_l0_filter_and_index_blocks_in_cache = true
high_priority_pool_ratio = 0.2

[memory]
write_buffer_manager_limit_mb = 256
enable_write_buffer_manager = true

[logging]
log_level = "info"
max_log_file_size_mb = 10
keep_log_file_num = 5

[performance]
statistics_level = "except_detailed_timers"
enable_statistics = false
stats_dump_period_sec = 600

[deployment]
mode = "replicated"
instance_id = $node
replica_endpoints = [$ENDPOINTS]
EOF
    done

    # Create data directories
    mkdir -p "$PROJECT_ROOT/data/multi-node-node-"{0..$((NODE_COUNT-1))}

    # Start nodes
    echo "Starting nodes..."

    for ((node=0; node<NODE_COUNT; node++)); do
        port=$((9090 + node))
        echo "Node $node on port $port..."
        # Start each process - use nohup for better process isolation on macOS
        nohup "$PROJECT_ROOT/build/bin/rocksdbserver-thrift" \
            --config "$PROJECT_ROOT/build/bin/cluster_configs/node_$node.toml" \
            --node-id $node \
            --port $port \
            --verbose \
            > "/tmp/kv-multinode-$node-$(date +%Y%m%d-%H%M%S).log" 2>&1 &
        NODE_PIDS+=($!)
        sleep 1
    done

    echo
    echo "🎉 Cluster running:"
    echo "  Node.js web server: localhost:3000 (PID: ${NODE_PIDS[0]})"
    for ((i=0; i<NODE_COUNT; i++)); do
        thrift_pid_index=$((i + 1))
        echo "  Thrift Node $i: localhost:$((9090 + i)) (PID: ${NODE_PIDS[thrift_pid_index]})"
    done
fi
echo
echo "Press Ctrl+C to stop..."

# Wait for interrupt
while true; do
    sleep 5
    # Check if all processes are still alive
    alive=0
    for pid in "${NODE_PIDS[@]}"; do
        kill -0 "$pid" 2>/dev/null && ((alive++))
    done
    if [ $alive -eq 0 ]; then
        echo "❌ All nodes stopped unexpectedly"
        exit 1
    fi
done