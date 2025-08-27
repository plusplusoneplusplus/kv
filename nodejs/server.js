const express = require('express');
const thrift = require('thrift');
const path = require('path');

// Import the generated Thrift files
const TransactionalKV = require('./TransactionalKV');
const kvstore_types = require('./kvstore_types');

const app = express();
const PORT = process.env.PORT || 3000;
const THRIFT_HOST = process.env.THRIFT_HOST || 'localhost';
const THRIFT_PORT = process.env.THRIFT_PORT || 9090;

// Middleware
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

// Routes

// Home page - serve the web interface
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// API endpoint to get all key-value pairs (using range query)
app.get('/api/keys', async (req, res) => {
    try {
        const client = createThriftClient();
        const limit = parseInt(req.query.limit) || 1000;
        const startKey = req.query.startKey || '';
        
        const getRangeRequest = new kvstore_types.GetRangeRequest({
            start_key: startKey,
            end_key: null, // Get all keys from start_key onwards
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
            
            const keyValues = result.key_values.map(kv => ({
                key: kv.key,
                value: kv.value,
                valueLength: kv.value.length
            }));
            
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
            
            res.json({
                key: key,
                value: result.value,
                found: result.found,
                valueLength: result.found ? result.value.length : 0
            });
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

app.listen(PORT, () => {
    console.log(`KV Store Web Viewer running on port ${PORT}`);
    console.log(`Connecting to Thrift server at ${THRIFT_HOST}:${THRIFT_PORT}`);
    console.log(`Open http://localhost:${PORT} to view the interface`);
});