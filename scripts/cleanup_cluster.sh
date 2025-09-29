#!/bin/bash

# Cleanup script for KV store cluster processes
# This script finds and kills any dangling cluster processes from previous runs

set -e

echo "🧹 Cleaning up KV cluster processes..."

# Function to safely kill processes
kill_processes() {
    local pattern="$1"
    local description="$2"

    local pids=$(pgrep -f "$pattern" 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "Found $description processes: $pids"
        echo "Terminating $description processes..."

        # Try graceful termination first
        echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
        sleep 2

        # Force kill any remaining processes
        local remaining=$(pgrep -f "$pattern" 2>/dev/null || true)
        if [ -n "$remaining" ]; then
            echo "Force killing remaining $description processes: $remaining"
            echo "$remaining" | xargs -r kill -KILL 2>/dev/null || true
        fi

        echo "✅ $description processes cleaned"
    else
        echo "✅ No $description processes found"
    fi
}

# Function to kill processes by port
kill_by_port() {
    local port="$1"
    local description="$2"

    local pids=$(lsof -ti :$port 2>/dev/null || true)
    if [ -n "$pids" ]; then
        echo "Found processes using port $port ($description): $pids"
        echo "Killing processes on port $port..."
        echo "$pids" | xargs -r kill -TERM 2>/dev/null || true
        sleep 1

        # Force kill any remaining
        local remaining=$(lsof -ti :$port 2>/dev/null || true)
        if [ -n "$remaining" ]; then
            echo "Force killing remaining processes on port $port: $remaining"
            echo "$remaining" | xargs -r kill -KILL 2>/dev/null || true
        fi

        echo "✅ Port $port cleaned"
    else
        echo "✅ Port $port is free"
    fi
}

echo
echo "1. Killing shard-server processes..."
kill_processes "shard-server" "shard-server"

echo
echo "2. Killing Node.js cluster server processes..."
kill_processes "node.*server.js" "Node.js cluster server"

echo
echo "3. Killing any nohup cluster processes..."
kill_processes "nohup.*shard-server" "nohup shard-server"

echo
echo "4. Cleaning up cluster ports..."
kill_by_port 3000 "Node.js web server"
kill_by_port 9090 "shard-server node 0"
kill_by_port 9091 "shard-server node 1"
kill_by_port 9092 "shard-server node 2"
kill_by_port 7090 "consensus node 0"
kill_by_port 7091 "consensus node 1"
kill_by_port 7092 "consensus node 2"

echo
echo "5. Cleaning up cluster directories..."
cluster_dirs=$(ls -d /tmp/cluster-* 2>/dev/null || true)
if [ -n "$cluster_dirs" ]; then
    echo "Found cluster directories:"
    echo "$cluster_dirs"
    read -p "Remove cluster directories? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf /tmp/cluster-*
        echo "✅ Cluster directories removed"
    else
        echo "ℹ️  Cluster directories preserved"
    fi
else
    echo "✅ No cluster directories found"
fi

echo
echo "6. Cleaning up old-style log files..."
old_logs=$(ls /tmp/kv-*-*.log 2>/dev/null || true)
if [ -n "$old_logs" ]; then
    echo "Found old log files: $old_logs"
    rm -f /tmp/kv-*-*.log
    echo "✅ Old log files removed"
else
    echo "✅ No old log files found"
fi

echo
echo "🎉 Cluster cleanup complete!"
echo
echo "Verification:"
ps aux | grep -E "(shard-server|node.*server.js)" | grep -v grep || echo "✅ No cluster processes running"
lsof -i :3000,:9090,:9091,:9092,:7090,:7091,:7092 2>/dev/null || echo "✅ All cluster ports are free"