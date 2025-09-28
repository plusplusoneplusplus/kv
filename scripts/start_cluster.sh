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

# Generate cluster ID with random number
CLUSTER_ID="cluster-$(date +%Y%m%d-%H%M%S)-$$"
CLUSTER_ROOT="/tmp/$CLUSTER_ID"

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
    pkill -f "shard-server" 2>/dev/null || true
    pkill -f "node.*server.js" 2>/dev/null || true

    # Clean up cluster directory
    if [ -d "$CLUSTER_ROOT" ]; then
        echo "Cleaning up cluster directory: $CLUSTER_ROOT"
        rm -rf "$CLUSTER_ROOT" 2>/dev/null || true
    fi

    # Clean up old-style log files (for backwards compatibility)
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
echo "Cluster directory: $CLUSTER_ROOT"
echo "Press Ctrl+C to stop"
echo

# Create cluster directory structure
echo "Creating cluster directory structure..."
mkdir -p "$CLUSTER_ROOT"
for ((i=1; i<=NODE_COUNT; i++)); do
    mkdir -p "$CLUSTER_ROOT/node$i/data"
    mkdir -p "$CLUSTER_ROOT/node$i/logs"
done

# Build server using CMake
cd "$PROJECT_ROOT"
echo "Building..."
cmake --build build --target rust_shard_server

# Start Node.js web server with cluster configuration
echo "Starting Node.js web server..."
cd "$PROJECT_ROOT/nodejs"

# Prepare cluster configuration for Node.js server
if [ "$NODE_COUNT" -eq 1 ]; then
    # Single node configuration
    CLUSTER_CONFIG="{\"nodes\": [{\"id\": 0, \"port\": 9090}], \"nodeCount\": 1, \"mode\": \"single\"}"
else
    # Multi-node configuration
    CLUSTER_NODES="["
    for ((i=0; i<NODE_COUNT; i++)); do
        port=$((9090 + i))
        if [ $i -eq 0 ]; then
            CLUSTER_NODES="$CLUSTER_NODES{\"id\": $i, \"port\": $port}"
        else
            CLUSTER_NODES="$CLUSTER_NODES, {\"id\": $i, \"port\": $port}"
        fi
    done
    CLUSTER_NODES="$CLUSTER_NODES]"
    CLUSTER_CONFIG="{\"nodes\": $CLUSTER_NODES, \"nodeCount\": $NODE_COUNT, \"mode\": \"cluster\"}"
fi

# Start Node.js server with cluster configuration and log path
nohup node server.js "$CLUSTER_CONFIG" "$CLUSTER_ROOT" > "$CLUSTER_ROOT/nodejs.log" 2>&1 &
NODEJS_PID=$!
NODE_PIDS+=($NODEJS_PID)
echo "Node.js server started on port 3000 (PID: $NODEJS_PID)"
echo "  Cluster config: $CLUSTER_CONFIG"
echo "  Log path: $CLUSTER_ROOT"
cd "$PROJECT_ROOT"

if [ "$NODE_COUNT" -eq 1 ]; then
    echo "Starting single node on port 9090..."
    # Single node mode - no config file needed, use Rust binary directly
    nohup "$PROJECT_ROOT/rust/target/debug/shard-server" \
        --port 9090 \
        --verbose \
        --log-dir "$CLUSTER_ROOT/node1/logs" \
        --db-path "$CLUSTER_ROOT/node1/data" \
        > "$CLUSTER_ROOT/node1/logs/stdout.log" 2>&1 &
    NODE_PIDS+=($!)

    echo
    echo "🎉 Single-node server running:"
    echo "  Node.js web server: localhost:3000 (PID: ${NODE_PIDS[0]})"
    echo "  Thrift server: localhost:9090 (PID: ${NODE_PIDS[1]})"
    echo "    Data: $CLUSTER_ROOT/node1/data"
    echo "    Logs: $CLUSTER_ROOT/node1/logs"
else
    # Multi-node cluster mode
    echo "Generating cluster configs..."
    mkdir -p "$PROJECT_ROOT/build/bin/cluster_configs"

    # Generate endpoint lists for cluster
    ENDPOINTS=""
    CONSENSUS_ENDPOINTS=""
    for ((i=0; i<NODE_COUNT; i++)); do
        kv_port=$((9090 + i))
        consensus_port=$((7090 + i))

        if [ $i -eq 0 ]; then
            ENDPOINTS="\"localhost:$kv_port\""
            CONSENSUS_ENDPOINTS="\"localhost:$consensus_port\""
        else
            ENDPOINTS="$ENDPOINTS, \"localhost:$kv_port\""
            CONSENSUS_ENDPOINTS="$CONSENSUS_ENDPOINTS, \"localhost:$consensus_port\""
        fi
    done

    # Generate config files for each node
    for ((node=0; node<NODE_COUNT; node++)); do
        node_num=$((node + 1))
        cat > "$PROJECT_ROOT/build/bin/cluster_configs/node_$node.toml" << EOF
# Configuration for Node $node in $NODE_COUNT-node cluster

[database]
base_path = "$CLUSTER_ROOT/node$node_num/data"

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

[consensus]
algorithm = "mock"
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
max_batch_size = 100
max_outstanding_proposals = 1000
endpoints = [$CONSENSUS_ENDPOINTS]
EOF
    done

    # Start nodes
    echo "Starting nodes..."

    for ((node=0; node<NODE_COUNT; node++)); do
        port=$((9090 + node))
        node_num=$((node + 1))
        echo "Node $node on port $port..."
        # Start each process using Rust binary directly
        nohup "$PROJECT_ROOT/rust/target/debug/shard-server" \
            --config "$PROJECT_ROOT/build/bin/cluster_configs/node_$node.toml" \
            --node-id $node \
            --port $port \
            --verbose \
            --log-dir "$CLUSTER_ROOT/node$node_num/logs" \
            > "$CLUSTER_ROOT/node$node_num/logs/stdout.log" 2>&1 &
        NODE_PIDS+=($!)
        sleep 1
    done

    echo
    echo "🎉 Cluster running:"
    echo "  Node.js web server: localhost:3000 (PID: ${NODE_PIDS[0]})"
    for ((i=0; i<NODE_COUNT; i++)); do
        thrift_pid_index=$((i + 1))
        node_num=$((i + 1))
        kv_port=$((9090 + i))
        consensus_port=$((7090 + i))
        role=$([ $i -eq 0 ] && echo "Leader" || echo "Follower")
        echo "  Node $i ($role): KV=localhost:$kv_port, Consensus=localhost:$consensus_port (PID: ${NODE_PIDS[thrift_pid_index]})"
        echo "    Data: $CLUSTER_ROOT/node$node_num/data"
        echo "    Logs: $CLUSTER_ROOT/node$node_num/logs"
    done
fi
echo
echo "📁 Cluster directory: $CLUSTER_ROOT"
echo "🔍 Log search options:"
echo "  Command line:"
echo "    grep -r 'ERROR' $CLUSTER_ROOT/*/logs/"
echo "    grep -r 'Starting' $CLUSTER_ROOT/*/logs/"
echo "    ls -la $CLUSTER_ROOT/*/logs/"
echo "  Web interface (http://localhost:3000):"
echo "    GET /api/logs/search?query=ERROR"
echo "    GET /api/logs/search?query=Starting&node_id=0"
echo "    GET /api/logs/files"
echo "    GET /api/cluster/config"
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