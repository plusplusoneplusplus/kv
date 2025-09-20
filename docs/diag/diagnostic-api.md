# Diagnostic API Documentation

The Thrift server includes essential diagnostic API endpoints for cluster management and monitoring. These endpoints provide real-time information about cluster health, database statistics, and node information.

## Overview

The diagnostic API is designed to enable monitoring and management of KV store clusters. It provides:

- **Cluster health monitoring**: Real-time status of all nodes in the cluster
- **Database statistics**: Performance metrics and operational data
- **Node management**: Individual node status and information

## Authentication

All diagnostic endpoints accept an optional `auth_token` parameter for authentication. Currently, the authentication system is basic:

- **Read operations**: Accept any non-empty token (or no token)
- **Future enhancement**: Will implement proper JWT or API key-based authentication

## Endpoints

### 1. Get Cluster Health

**Endpoint**: `getClusterHealth`

**Request**:
```thrift
struct GetClusterHealthRequest {
    1: optional string auth_token
}
```

**Response**:
```thrift
struct GetClusterHealthResponse {
    1: required bool success,
    2: optional ClusterHealth cluster_health,
    3: optional string error
}

struct ClusterHealth {
    1: required list<NodeStatus> nodes,
    2: required i32 healthy_nodes_count,
    3: required i32 total_nodes_count,
    4: optional NodeStatus leader,
    5: required string cluster_status,  // "healthy", "degraded", "unhealthy"
    6: required i64 current_term
}
```

**Description**: Returns comprehensive information about cluster health, including status of all nodes, leader information, and overall cluster state.

**Performance Impact**: Minimal (<0.1% overhead)

### 2. Get Database Statistics

**Endpoint**: `getDatabaseStats`

**Request**:
```thrift
struct GetDatabaseStatsRequest {
    1: optional string auth_token,
    2: optional bool include_detailed_stats = false
}
```

**Response**:
```thrift
struct GetDatabaseStatsResponse {
    1: required bool success,
    2: optional DatabaseStats database_stats,
    3: optional string error
}

struct DatabaseStats {
    1: required i64 total_keys,
    2: required i64 total_size_bytes,
    3: required i64 write_operations_count,
    4: required i64 read_operations_count,
    5: required double average_response_time_ms,
    6: required i64 active_transactions,
    7: required i64 committed_transactions,
    8: required i64 aborted_transactions,
    9: optional i64 cache_hit_rate_percent,
    10: optional i64 compaction_pending_bytes
}
```

**Description**: Provides database performance and operational statistics. The `include_detailed_stats` parameter can be used to request additional detailed metrics (future enhancement).

**Performance Impact**: Minimal for basic stats, <1% for detailed stats

### 3. Get Node Information

**Endpoint**: `getNodeInfo`

**Request**:
```thrift
struct GetNodeInfoRequest {
    1: optional string auth_token,
    2: optional i32 node_id  // If not provided, returns info for current node
}
```

**Response**:
```thrift
struct GetNodeInfoResponse {
    1: required bool success,
    2: optional NodeStatus node_info,
    3: optional string error
}

struct NodeStatus {
    1: required i32 node_id,
    2: required string endpoint,
    3: required string status,  // "healthy", "degraded", "unreachable", "unknown"
    4: required bool is_leader,
    5: required i64 last_seen_timestamp,
    6: required i64 term,
    7: optional i64 uptime_seconds
}
```

**Description**: Returns detailed information about a specific node or the current node if no node_id is specified.

## Usage Examples

### Python Client Example

```python
import thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from generated.TransactionalKV import Client
from generated.kvstore_types import *

# Connect to server
transport = TSocket.TSocket('localhost', 9090)
transport = TTransport.TBufferedTransport(transport)
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Client(protocol)

transport.open()

try:
    # Get cluster health
    health_req = GetClusterHealthRequest()
    health_resp = client.getClusterHealth(health_req)
    
    if health_resp.success:
        print(f"Cluster Status: {health_resp.cluster_health.cluster_status}")
        print(f"Healthy Nodes: {health_resp.cluster_health.healthy_nodes_count}/{health_resp.cluster_health.total_nodes_count}")
    
    # Get database stats
    stats_req = GetDatabaseStatsRequest()
    stats_resp = client.getDatabaseStats(stats_req)
    
    if stats_resp.success:
        stats = stats_resp.database_stats
        print(f"Total Keys: {stats.total_keys}")
        print(f"Database Size: {stats.total_size_bytes} bytes")
        print(f"Active Transactions: {stats.active_transactions}")
    
    # Get node info
    node_req = GetNodeInfoRequest()
    node_resp = client.getNodeInfo(node_req)
    
    if node_resp.success:
        node = node_resp.node_info
        print(f"Node ID: {node.node_id}")
        print(f"Status: {node.status}")
        print(f"Is Leader: {node.is_leader}")
        print(f"Uptime: {node.uptime_seconds} seconds")

finally:
    transport.close()
```

### JavaScript/Node.js Example

```javascript
const thrift = require('thrift');
const TransactionalKV = require('./generated/TransactionalKV');
const ttypes = require('./generated/kvstore_types');

// Create connection
const connection = thrift.createConnection('localhost', 9090, {
    transport: thrift.TBufferedTransport,
    protocol: thrift.TBinaryProtocol
});

const client = thrift.createClient(TransactionalKV, connection);

// Get cluster health
const healthReq = new ttypes.GetClusterHealthRequest();
client.getClusterHealth(healthReq, (err, result) => {
    if (err) {
        console.error('Error:', err);
        return;
    }
    
    if (result.success) {
        console.log(`Cluster Status: ${result.cluster_health.cluster_status}`);
        console.log(`Healthy Nodes: ${result.cluster_health.healthy_nodes_count}/${result.cluster_health.total_nodes_count}`);
        
        result.cluster_health.nodes.forEach(node => {
            console.log(`Node ${node.node_id}: ${node.status} at ${node.endpoint}`);
        });
    }
});

// Get database stats
const statsReq = new ttypes.GetDatabaseStatsRequest();
client.getDatabaseStats(statsReq, (err, result) => {
    if (err) {
        console.error('Error:', err);
        return;
    }
    
    if (result.success) {
        const stats = result.database_stats;
        console.log(`Database Statistics:`);
        console.log(`  Total Keys: ${stats.total_keys}`);
        console.log(`  Size: ${stats.total_size_bytes} bytes`);
        console.log(`  Read Operations: ${stats.read_operations_count}`);
        console.log(`  Write Operations: ${stats.write_operations_count}`);
        console.log(`  Active Transactions: ${stats.active_transactions}`);
    }
});

// Get node info
const nodeReq = new ttypes.GetNodeInfoRequest();
client.getNodeInfo(nodeReq, (err, result) => {
    if (err) {
        console.error('Error:', err);
        return;
    }
    
    if (result.success) {
        const node = result.node_info;
        console.log(`Node Information:`);
        console.log(`  Node ID: ${node.node_id}`);
        console.log(`  Status: ${node.status}`);
        console.log(`  Is Leader: ${node.is_leader}`);
        console.log(`  Uptime: ${node.uptime_seconds} seconds`);
    }
});
```

## Error Handling

All diagnostic endpoints return a structured response with:
- `success`: Boolean indicating if the operation succeeded
- `error`: Optional error message if the operation failed
- Data field: The requested information if successful

Common error scenarios:
- **Node not found**: When requesting info for non-existent node
- **Internal error**: Database or system errors

## Monitoring Integration

The diagnostic API is designed to integrate with monitoring systems:

### Prometheus Integration (Future)
- Metrics endpoints will expose Prometheus-compatible metrics
- Custom collectors for database and cluster metrics

### Health Check Integration
- `/health` endpoint for load balancer health checks
- Detailed health information via `getClusterHealth`

### Alerting
- Cluster status changes (healthy -> degraded -> unhealthy)
- Node failures and recoveries
- Performance threshold breaches

## Performance Considerations

- **Read operations**: <0.1% performance overhead
- **Database stats**: <1% overhead for detailed statistics
- **Caching**: Metrics are cached for 1 second to reduce overhead
- **Rate limiting**: Consider implementing rate limiting for production use

## Security Considerations

- **Authentication**: Implement proper token validation
- **Authorization**: Role-based access control for different operations
- **Rate limiting**: Prevent diagnostic API abuse
- **Audit logging**: Log administrative operations
- **Network security**: Use TLS for production deployments

## Future Enhancements

1. **Enhanced Authentication**: JWT tokens, API keys, RBAC
2. **Real-time Metrics**: WebSocket streaming for live metrics
3. **Configuration Hot-reload**: Runtime configuration updates
4. **Alerting Integration**: Built-in alerting for critical events
5. **Metrics Export**: Prometheus, StatsD, CloudWatch integration
6. **Performance Profiling**: Built-in profiling and tracing
7. **Cluster Topology**: Visual cluster topology and relationships

## Testing

Comprehensive tests are available in `rust/tests/diagnostic_tests.rs`:

```bash
# Run diagnostic API tests
cd rust
cargo test --test diagnostic_tests

# Run all tests
cargo test
```

Tests cover:
- Single-node and multi-node cluster health
- Database statistics retrieval
- Node information queries
- Error conditions
