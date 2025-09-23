const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const thrift = require('thrift');
const path = require('path');
const cors = require('cors');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
// Authentication modules removed

// Import the generated Thrift files
const TransactionalKV = require('./thrift/TransactionalKV');
const kvstore_types = require('./thrift/kvstore_types');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: process.env.ALLOWED_ORIGINS || "*",
        methods: ["GET", "POST"]
    }
});

const PORT = process.env.PORT || 3000;
let THRIFT_HOST = process.env.THRIFT_HOST || 'localhost';
let THRIFT_PORT = process.env.THRIFT_PORT || 9090;

// Security configuration (authentication removed)

// Rate limiting (disabled in test environment)
const limiter = process.env.NODE_ENV === 'test' ? 
    (req, res, next) => next() : // No rate limiting in tests
    rateLimit({
        windowMs: 15 * 60 * 1000, // 15 minutes
        max: 500, // limit each IP to 500 requests per windowMs
        message: { error: 'Too many requests, please try again later.' }
    });

// Middleware
app.use(helmet({
    contentSecurityPolicy: {
        directives: {
            defaultSrc: ["'self'"],
            connectSrc: ["'self'", "wss:", "ws:", "https://cdn.jsdelivr.net", "https://cdn.socket.io"],
            scriptSrc: ["'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net", "https://cdn.socket.io"],
            scriptSrcAttr: ["'unsafe-inline'"],
            styleSrc: ["'self'", "'unsafe-inline'", "https://cdn.jsdelivr.net", "https://cdnjs.cloudflare.com"],
            fontSrc: ["'self'", "https://cdn.jsdelivr.net", "https://cdnjs.cloudflare.com"],
            imgSrc: ["'self'", "data:"]
        }
    },
    crossOriginResourcePolicy: false,
    crossOriginOpenerPolicy: false,
    crossOriginEmbedderPolicy: false
}));
app.use(cors());
app.use(limiter);
app.use(express.json());
app.use(express.static('public'));

// Create Thrift connection
function createThriftClient() {
    const connection = thrift.createConnection(THRIFT_HOST, THRIFT_PORT, {
        transport: thrift.TBufferedTransport,
        protocol: thrift.TBinaryProtocol
    });

    connection.on('error', (err) => {
        console.error('Thrift connection error:', err);
    });

    return thrift.createClient(TransactionalKV, connection);
}

// Authentication middleware removed

// WebSocket connection handling
io.on('connection', (socket) => {
    console.log('Client connected:', socket.id);

    // Join dashboard room for real-time updates
    socket.join('dashboard');

    socket.on('disconnect', () => {
        console.log('Client disconnected:', socket.id);
    });

    socket.on('request-cluster-update', async () => {
        try {
            const clusterHealth = await getClusterHealthData();
            socket.emit('cluster-update', clusterHealth);
        } catch (error) {
            socket.emit('error', { message: 'Failed to get cluster update' });
        }
    });
});

// Helper function to get cluster health data
async function getClusterHealthData() {
    return new Promise((resolve, reject) => {
        const client = createThriftClient();
        const request = new kvstore_types.GetClusterHealthRequest({});

        client.getClusterHealth(request, (err, result) => {
            if (err) {
                reject(err);
                return;
            }

            if (!result.success) {
                reject(new Error(result.error || 'Failed to get cluster health'));
                return;
            }

            const processedHealth = processClusterHealthData(result.cluster_health);
            resolve(processedHealth);
        });
    });
}

// Helper function to convert buffer values to numbers
function convertBufferValue(value) {
    // Handle the specific buffer format we're getting
    if (value && typeof value === 'object' && value.buffer && value.buffer.data) {
        const bytes = value.buffer.data;
        // Convert buffer data to BigInt, then to number
        let result = 0n;
        for (let i = 0; i < bytes.length; i++) {
            result = (result << 8n) | BigInt(bytes[i]);
        }
        return Number(result);
    }

    // Also handle the case where value has buffer and offset properties directly
    if (value && typeof value === 'object' && value.buffer && typeof value.offset === 'number') {
        // Check if it's a Node.js Buffer object
        if (Buffer.isBuffer(value.buffer)) {
            let result = 0n;
            for (let i = 0; i < value.buffer.length; i++) {
                result = (result << 8n) | BigInt(value.buffer[i]);
            }
            return Number(result);
        }
        // Handle serialized buffer format
        if (value.buffer.type === 'Buffer' && value.buffer.data) {
            const bytes = value.buffer.data;
            let result = 0n;
            for (let i = 0; i < bytes.length; i++) {
                result = (result << 8n) | BigInt(bytes[i]);
            }
            return Number(result);
        }
    }

    return value;
}

// Helper function to process stats object and convert buffer values
function processStatsData(stats) {
    if (!stats) return stats;

    const processed = {};
    for (const [key, value] of Object.entries(stats)) {
        processed[key] = convertBufferValue(value);
    }
    return processed;
}

// Helper function to process cluster health data
function processClusterHealthData(clusterHealth) {
    if (!clusterHealth) return clusterHealth;

    const processed = { ...clusterHealth };

    // Convert numeric fields
    processed.total_nodes_count = convertBufferValue(clusterHealth.total_nodes_count);
    processed.healthy_nodes_count = convertBufferValue(clusterHealth.healthy_nodes_count);
    processed.current_term = convertBufferValue(clusterHealth.current_term);

    // Handle current_leader_id - it might be in different places
    if (clusterHealth.current_leader_id !== undefined) {
        processed.current_leader_id = convertBufferValue(clusterHealth.current_leader_id);
    } else if (clusterHealth.leader && clusterHealth.leader.node_id !== undefined) {
        processed.current_leader_id = convertBufferValue(clusterHealth.leader.node_id);
    } else {
        processed.current_leader_id = null;
    }

    // Process nodes array
    if (clusterHealth.nodes) {
        processed.nodes = clusterHealth.nodes.map(node => ({
            ...node,
            node_id: convertBufferValue(node.node_id),
            last_seen_timestamp: convertBufferValue(node.last_seen_timestamp),
            term: convertBufferValue(node.term),
            uptime_seconds: convertBufferValue(node.uptime_seconds)
        }));
    }

    // Process leader object if it exists
    if (clusterHealth.leader) {
        processed.leader = {
            ...clusterHealth.leader,
            node_id: convertBufferValue(clusterHealth.leader.node_id),
            last_seen_timestamp: convertBufferValue(clusterHealth.leader.last_seen_timestamp),
            term: convertBufferValue(clusterHealth.leader.term),
            uptime_seconds: convertBufferValue(clusterHealth.leader.uptime_seconds)
        };
    }

    return processed;
}

// Helper function to get database stats
async function getDatabaseStatsData(includeDetailed = false) {
    return new Promise((resolve, reject) => {
        const client = createThriftClient();
        const request = new kvstore_types.GetDatabaseStatsRequest({
            include_detailed_stats: includeDetailed
        });

        client.getDatabaseStats(request, (err, result) => {
            if (err) {
                reject(err);
                return;
            }

            if (!result.success) {
                reject(new Error(result.error || 'Failed to get database stats'));
                return;
            }

            const processedStats = processStatsData(result.database_stats);
            resolve(processedStats);
        });
    });
}

// Helper function to get node info
async function getNodeInfoData(nodeId) {
    return new Promise((resolve, reject) => {
        const client = createThriftClient();
        const request = new kvstore_types.GetNodeInfoRequest({
            node_id: nodeId
        });

        client.getNodeInfo(request, (err, result) => {
            if (err) {
                reject(err);
                return;
            }

            if (!result.success) {
                reject(new Error(result.error || 'Failed to get node info'));
                return;
            }

            const processedNodeInfo = {
                ...result.node_info,
                node_id: convertBufferValue(result.node_info.node_id),
                uptime_seconds: convertBufferValue(result.node_info.uptime_seconds)
            };
            resolve(processedNodeInfo);
        });
    });
}

// Routes

// Authentication routes removed

// Dashboard routes (serve unified admin dashboard)
app.get('/dashboard', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Tab navigation routes
app.get('/clear', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/settings', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/cluster', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});



// Diagnostic API endpoints
app.get('/api/cluster/health', async (req, res) => {
    try {
        const clusterHealth = await getClusterHealthData();
        res.json({
            success: true,
            data: clusterHealth
        });

        // Broadcast to connected clients
        io.to('dashboard').emit('cluster-update', clusterHealth);
    } catch (error) {
        console.error('Error getting cluster health:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get cluster health',
            details: error.message
        });
    }
});

app.get('/api/cluster/stats', async (req, res) => {
    try {
        const includeDetailed = req.query.detailed === 'true';
        const stats = await getDatabaseStatsData(includeDetailed);
        res.json({
            success: true,
            data: stats
        });
    } catch (error) {
        console.error('Error getting database stats:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get database stats',
            details: error.message
        });
    }
});

app.get('/api/cluster/nodes', async (req, res) => {
    try {
        const clusterHealth = await getClusterHealthData();
        res.json({
            success: true,
            data: {
                nodes: clusterHealth.nodes,
                totalNodes: clusterHealth.total_nodes_count,
                healthyNodes: clusterHealth.healthy_nodes_count,
                currentLeader: clusterHealth.current_leader_id
            }
        });
    } catch (error) {
        console.error('Error getting cluster nodes:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get cluster nodes',
            details: error.message
        });
    }
});

// Individual node info endpoint
app.get('/api/cluster/nodes/:nodeId', async (req, res) => {
    try {
        const nodeId = parseInt(req.params.nodeId);

        // Validate node ID parameter
        if (isNaN(nodeId) || nodeId < 0) {
            return res.status(400).json({
                success: false,
                error: 'Invalid node ID'
            });
        }

        const nodeInfo = await getNodeInfoData(nodeId);
        res.json({
            success: true,
            data: nodeInfo
        });
    } catch (error) {
        console.error('Error getting node info:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get node info',
            details: error.message
        });
    }
});

// Replication status endpoint
app.get('/api/cluster/replication', async (req, res) => {
    try {
        const clusterHealth = await getClusterHealthData();

        // Process the cluster health data to extract replication information
        const nodeStates = {};
        const replicationLag = {};

        if (clusterHealth.nodes) {
            clusterHealth.nodes.forEach(node => {
                const nodeId = node.node_id;
                if (node.is_leader) {
                    nodeStates[nodeId] = 'leader';
                    replicationLag[nodeId] = 0; // Leader has no lag
                } else {
                    nodeStates[nodeId] = 'follower';
                    replicationLag[nodeId] = 0; // For simplicity, assume no lag
                }
            });
        }

        // Determine if consensus is active (more than one node means consensus is needed)
        const consensusActive = clusterHealth.nodes && clusterHealth.nodes.length > 1;

        res.json({
            success: true,
            data: {
                currentTerm: clusterHealth.current_term,
                leaderId: clusterHealth.current_leader_id,
                consensusActive: consensusActive,
                nodeStates: nodeStates,
                replicationLag: replicationLag
            }
        });
    } catch (error) {
        console.error('Error getting replication status:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to get replication status',
            details: error.message
        });
    }
});

// Real-time data broadcast (every 5 seconds) - disabled in test environment
if (process.env.NODE_ENV !== 'test') {
    setInterval(async () => {
        try {
            const clusterHealth = await getClusterHealthData();
            io.to('dashboard').emit('cluster-update', clusterHealth);
        } catch (error) {
            console.error('Error broadcasting cluster update:', error);
        }
    }, 5000);
}

// Home page - serve original index.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// API endpoint to get all key-value pairs (using range query)
app.get('/api/keys', async (req, res) => {
    try {
        const client = createThriftClient();
        const limit = parseInt(req.query.limit) || 1000;
        const startKey = req.query.startKey || '';
        const prefix = req.query.prefix || '';
        
        // Calculate range for prefix filtering
        let actualStartKey = startKey;
        let endKey = null;
        
        if (prefix) {
            actualStartKey = prefix;
            // Calculate lexicographically next string after prefix
            // by incrementing the last byte
            if (prefix.length > 0) {
                const prefixBytes = Buffer.from(prefix, 'utf8');
                const lastByte = prefixBytes[prefixBytes.length - 1];
                if (lastByte < 255) {
                    const endKeyBytes = Buffer.from(prefixBytes);
                    endKeyBytes[endKeyBytes.length - 1] = lastByte + 1;
                    endKey = endKeyBytes;
                }
            }
        }
        
        const getRangeRequest = new kvstore_types.GetRangeRequest({
            start_key: actualStartKey,
            end_key: endKey,
            limit: limit
        });
        
        client.getRange(getRangeRequest, (err, result) => {
            if (err) {
                console.error('Error fetching keys:', err);
                return res.status(500).json({ error: 'Failed to fetch keys', details: err.message });
            }
            
            if (!result.success) {
                return res.status(500).json({ error: 'Range query failed', details: result.error });
            }
            
            let keyValues = result.key_values.map(kv => {
                return {
                    key: Buffer.isBuffer(kv.key) ? kv.key.toString('base64') : kv.key,
                    value: Buffer.isBuffer(kv.value) ? kv.value.toString('base64') : kv.value,
                    keyIsBuffer: Buffer.isBuffer(kv.key),
                    valueIsBuffer: Buffer.isBuffer(kv.value)
                };
            });
            
            // Apply client-side prefix filtering as backup
            if (prefix) {
                keyValues = keyValues.filter(kv => {
                    const keyStr = kv.keyIsBuffer ? Buffer.from(kv.key, 'base64').toString('utf8') : kv.key;
                    return keyStr.startsWith(prefix);
                });
            }
            
            res.json({
                success: true,
                count: keyValues.length,
                keyValues: keyValues
            });
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to get a specific key
app.get('/api/key/:key', async (req, res) => {
    try {
        const client = createThriftClient();
        const key = req.params.key;
        
        const getRequest = new kvstore_types.GetRequest({
            key: key
        });
        
        client.get(getRequest, (err, result) => {
            if (err) {
                console.error('Error fetching key:', err);
                return res.status(500).json({ error: 'Failed to fetch key', details: err.message });
            }
            
            if (result.error) {
                return res.status(500).json({ error: 'Get operation failed', details: result.error });
            }
            
            if (result.found) {
                res.json({
                    key: key,
                    value: Buffer.isBuffer(result.value) ? result.value.toString('base64') : result.value,
                    valueIsBuffer: Buffer.isBuffer(result.value),
                    found: result.found
                });
            } else {
                res.json({
                    key: key,
                    value: '',
                    valueIsBuffer: false,
                    found: result.found
                });
            }
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to set a key-value pair
app.post('/api/key', async (req, res) => {
    try {
        const { key, value } = req.body;
        
        if (!key || value === undefined) {
            return res.status(400).json({ error: 'Key and value are required' });
        }
        
        const client = createThriftClient();
        
        const setRequest = new kvstore_types.SetRequest({
            key: key,
            value: String(value)
        });
        
        client.setKey(setRequest, (err, result) => {
            if (err) {
                console.error('Error setting key:', err);
                return res.status(500).json({ error: 'Failed to set key', details: err.message });
            }
            
            if (!result.success) {
                return res.status(500).json({ error: 'Set operation failed', details: result.error });
            }
            
            res.json({
                success: true,
                key: key,
                value: String(value)
            });
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to delete a key
app.delete('/api/key/:key', async (req, res) => {
    try {
        const client = createThriftClient();
        const key = req.params.key;
        
        const deleteRequest = new kvstore_types.DeleteRequest({
            key: key
        });
        
        client.deleteKey(deleteRequest, (err, result) => {
            if (err) {
                console.error('Error deleting key:', err);
                return res.status(500).json({ error: 'Failed to delete key', details: err.message });
            }
            
            if (!result.success) {
                return res.status(500).json({ error: 'Delete operation failed', details: result.error });
            }
            
            res.json({
                success: true,
                key: key,
                deleted: result.success
            });
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to clear all keys (DANGEROUS - Admin only)
app.delete('/api/admin/clear-all', async (req, res) => {
    try {
        const { confirmation } = req.body;
        
        // Require explicit confirmation
        if (confirmation !== 'DELETE ALL DATA') {
            return res.status(400).json({
                error: 'Missing confirmation',
                details: 'You must send {"confirmation": "DELETE ALL DATA"} in the request body'
            });
        }
        
        const client = createThriftClient();
        
        // First, get all keys using range query
        const getRangeRequest = new kvstore_types.GetRangeRequest({
            start_key: '',
            end_key: null,
            limit: 100000 // Large limit to get all keys
        });
        
        client.getRange(getRangeRequest, (err, result) => {
            if (err) {
                console.error('Error fetching keys for deletion:', err);
                return res.status(500).json({ error: 'Failed to fetch keys', details: err.message });
            }
            
            if (!result.success) {
                return res.status(500).json({ error: 'Range query failed', details: result.error });
            }
            
            const keys = result.key_values.map(kv => kv.key);
            let deletedCount = 0;
            let errors = [];
            
            if (keys.length === 0) {
                return res.json({
                    success: true,
                    message: 'No keys to delete',
                    deletedCount: 0,
                    totalKeys: 0
                });
            }
            
            console.log(`Admin clear-all: Deleting ${keys.length} keys...`);
            
            // Delete each key individually
            const deletePromises = keys.map(key => {
                return new Promise((resolve) => {
                    const deleteRequest = new kvstore_types.DeleteRequest({ key: key });
                    
                    client.deleteKey(deleteRequest, (deleteErr, deleteResult) => {
                        if (deleteErr || !deleteResult.success) {
                            errors.push({ key: key, error: deleteErr?.message || deleteResult.error });
                        } else {
                            deletedCount++;
                        }
                        resolve();
                    });
                });
            });
            
            Promise.all(deletePromises).then(() => {
                console.log(`Admin clear-all completed: ${deletedCount}/${keys.length} keys deleted`);
                
                res.json({
                    success: errors.length === 0,
                    message: `Deleted ${deletedCount} out of ${keys.length} keys`,
                    deletedCount: deletedCount,
                    totalKeys: keys.length,
                    errors: errors.length > 0 ? errors.slice(0, 10) : [] // Limit error reporting
                });
            });
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to ping the server
app.get('/api/ping', async (req, res) => {
    try {
        const client = createThriftClient();
        const pingRequest = new kvstore_types.PingRequest({
            message: 'ping from web interface',
            timestamp: Date.now()
        });

        client.ping(pingRequest, (err, result) => {
            if (err) {
                console.error('Error pinging server:', err);
                return res.status(500).json({ error: 'Failed to ping server', details: err.message });
            }

            res.json({
                success: true,
                message: result.message,
                clientTimestamp: result.timestamp,
                serverTimestamp: result.server_timestamp,
                roundTripTime: Date.now() - parseInt(result.timestamp)
            });
        });
    } catch (error) {
        console.error('API error:', error);
        res.status(500).json({ error: 'Internal server error', details: error.message });
    }
});

// API endpoint to get current endpoint configuration
app.get('/api/admin/current-endpoint', (req, res) => {
    res.json({
        success: true,
        host: THRIFT_HOST,
        port: THRIFT_PORT
    });
});

// API endpoint to update endpoint configuration
app.post('/api/admin/update-endpoint', (req, res) => {
    try {
        const { host, port } = req.body;

        if (!host || !port) {
            return res.status(400).json({
                success: false,
                error: 'Host and port are required'
            });
        }

        // Validate port range
        const portNum = parseInt(port);
        if (isNaN(portNum) || portNum < 1 || portNum > 65535) {
            return res.status(400).json({
                success: false,
                error: 'Port must be a number between 1 and 65535'
            });
        }

        // Validate host (basic validation)
        if (typeof host !== 'string' || host.trim().length === 0 || host.includes(' ')) {
            return res.status(400).json({
                success: false,
                error: 'Invalid host format'
            });
        }

        // Update the global variables
        THRIFT_HOST = host.trim();
        THRIFT_PORT = portNum;

        console.log(`Endpoint updated to ${THRIFT_HOST}:${THRIFT_PORT}`);

        res.json({
            success: true,
            host: THRIFT_HOST,
            port: THRIFT_PORT,
            message: `Endpoint updated to ${THRIFT_HOST}:${THRIFT_PORT}`
        });

    } catch (error) {
        console.error('Error updating endpoint:', error);
        res.status(500).json({
            success: false,
            error: 'Internal server error',
            details: error.message
        });
    }
});

// Export the app for testing
module.exports = app;

// Only start the server if this file is run directly
if (require.main === module) {
    server.listen(PORT, () => {
        console.log(`KV Store Admin Dashboard running on port ${PORT}`);
        console.log(`Connecting to Thrift server at ${THRIFT_HOST}:${THRIFT_PORT}`);
        console.log(`Unified Admin Portal: http://localhost:${PORT}/`);
        console.log(`  • Cluster Monitoring & Data Browser integrated`);
        console.log(`  • Real-time updates via WebSocket enabled`);
        console.log(`  • No authentication required`);
    });
}