const request = require('supertest');
const thrift = require('thrift');

// Mock the thrift module
jest.mock('thrift');

// Import the server after mocking
const app = require('../server');

describe('Integration Tests', () => {
  let mockClient;
  let mockConnection;

  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks();
    
    // Mock thrift connection and client
    mockConnection = {
      on: jest.fn()
    };
    
    mockClient = {
      get: jest.fn(),
      setKey: jest.fn(),
      deleteKey: jest.fn(),
      getRange: jest.fn(),
      ping: jest.fn(),
      getClusterHealth: jest.fn(),
      getDatabaseStats: jest.fn()
    };

    thrift.createConnection.mockReturnValue(mockConnection);
    thrift.createClient.mockReturnValue(mockClient);
  });

  describe('Thrift Connection Management', () => {
    it('should create thrift connection with correct parameters', async () => {
      mockClient.ping.mockImplementation((request, callback) => {
        callback(null, { message: 'pong', timestamp: '123', server_timestamp: '124' });
      });

      await request(app).get('/api/ping');

      expect(thrift.createConnection).toHaveBeenCalledWith(
        'localhost', // host
        '9090', // port (from env var, still a string)
        expect.objectContaining({
          transport: thrift.TBufferedTransport,
          protocol: thrift.TBinaryProtocol
        })
      );
    });

    it('should handle connection errors gracefully', async () => {
      const mockError = new Error('Connection refused');
      mockConnection.on.mockImplementation((event, callback) => {
        if (event === 'error') {
          // Simulate connection error
          setTimeout(() => callback(mockError), 10);
        }
      });

      mockClient.ping.mockImplementation((request, callback) => {
        callback(mockError);
      });

      const response = await request(app)
        .get('/api/ping')
        .expect(500);

      expect(response.body.error).toBe('Failed to ping server');
    });
  });

  describe('End-to-End Key Operations', () => {
    it('should complete full CRUD cycle', async () => {
      const testKey = 'integration_test_key';
      const testValue = 'integration_test_value';
      const updatedValue = 'updated_test_value';

      // 1. Set key
      mockClient.setKey.mockImplementation((request, callback) => {
        callback(null, { success: true });
      });

      let response = await request(app)
        .post('/api/key')
        .send({ key: testKey, value: testValue })
        .expect(200);

      expect(response.body.success).toBe(true);

      // 2. Get key
      mockClient.get.mockImplementation((request, callback) => {
        callback(null, { found: true, value: testValue });
      });

      response = await request(app)
        .get(`/api/key/${testKey}`)
        .expect(200);

      expect(response.body.found).toBe(true);
      expect(response.body.value).toBe(testValue);

      // 3. Update key
      mockClient.setKey.mockImplementation((request, callback) => {
        callback(null, { success: true });
      });

      response = await request(app)
        .post('/api/key')
        .send({ key: testKey, value: updatedValue })
        .expect(200);

      expect(response.body.success).toBe(true);

      // 4. Verify update
      mockClient.get.mockImplementation((request, callback) => {
        callback(null, { found: true, value: updatedValue });
      });

      response = await request(app)
        .get(`/api/key/${testKey}`)
        .expect(200);

      expect(response.body.value).toBe(updatedValue);

      // 5. Delete key
      mockClient.deleteKey.mockImplementation((request, callback) => {
        callback(null, { success: true });
      });

      response = await request(app)
        .delete(`/api/key/${testKey}`)
        .expect(200);

      expect(response.body.success).toBe(true);

      // 6. Verify deletion
      mockClient.get.mockImplementation((request, callback) => {
        callback(null, { found: false });
      });

      response = await request(app)
        .get(`/api/key/${testKey}`)
        .expect(200);

      expect(response.body.found).toBe(false);
    });
  });

  describe('Bulk Operations', () => {
    it('should handle multiple keys in range query', async () => {
      const mockKeys = [];
      for (let i = 0; i < 100; i++) {
        mockKeys.push({
          key: `bulk_key_${i.toString().padStart(3, '0')}`,
          value: `bulk_value_${i}`
        });
      }

      mockClient.getRange.mockImplementation((request, callback) => {
        const limit = request.limit || 1000;
        const keys = mockKeys.slice(0, Math.min(limit, mockKeys.length));
        callback(null, {
          success: true,
          key_values: keys
        });
      });

      const response = await request(app)
        .get('/api/keys?limit=50')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.count).toBe(50);
      expect(response.body.keyValues).toHaveLength(50);
    });

    it('should handle pagination correctly', async () => {
      const mockKeys = [];
      for (let i = 0; i < 20; i++) {
        mockKeys.push({
          key: `page_key_${i.toString().padStart(2, '0')}`,
          value: `page_value_${i}`
        });
      }

      // First page
      mockClient.getRange.mockImplementation((request, callback) => {
        callback(null, {
          success: true,
          key_values: mockKeys.slice(0, 10)
        });
      });

      let response = await request(app)
        .get('/api/keys?limit=10')
        .expect(200);

      expect(response.body.count).toBe(10);

      // Second page
      mockClient.getRange.mockImplementation((request, callback) => {
        // The start_key should match what we're passing in the query
        callback(null, {
          success: true,
          key_values: mockKeys.slice(10, 20)
        });
      });

      response = await request(app)
        .get('/api/keys?limit=10&startKey=page_key_10')
        .expect(200);

      expect(response.body.count).toBe(10);
    });
  });

  describe('Diagnostic Integration', () => {
    it('should provide comprehensive cluster status', async () => {
      const mockClusterHealth = {
        success: true,
        cluster_health: {
          total_nodes_count: { buffer: { data: [0, 0, 0, 3] } },
          healthy_nodes_count: { buffer: { data: [0, 0, 0, 3] } },
          current_term: { buffer: { data: [0, 0, 0, 10] } },
          current_leader_id: { buffer: { data: [0, 0, 0, 1] } },
          nodes: [
            {
              node_id: { buffer: { data: [0, 0, 0, 1] } },
              status: 'healthy',
              is_leader: true,
              last_seen_timestamp: { buffer: { data: [0, 0, 0, 0, 0, 0, 0, 100] } },
              term: { buffer: { data: [0, 0, 0, 10] } },
              uptime_seconds: { buffer: { data: [0, 0, 0, 0, 0, 0, 14, 16] } }
            },
            {
              node_id: { buffer: { data: [0, 0, 0, 2] } },
              status: 'healthy',
              is_leader: false,
              last_seen_timestamp: { buffer: { data: [0, 0, 0, 0, 0, 0, 0, 99] } },
              term: { buffer: { data: [0, 0, 0, 10] } },
              uptime_seconds: { buffer: { data: [0, 0, 0, 0, 0, 0, 14, 15] } }
            }
          ]
        }
      };

      const mockStats = {
        success: true,
        database_stats: {
          total_keys: { buffer: { data: [0, 0, 0, 0, 0, 0, 39, 16] } },
          total_size_bytes: { buffer: { data: [0, 0, 0, 0, 0, 15, 66, 64] } }
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockClusterHealth);
      });

      mockClient.getDatabaseStats.mockImplementation((request, callback) => {
        callback(null, mockStats);
      });

      // Test cluster health
      let response = await request(app)
        .get('/api/cluster/health')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data.total_nodes_count).toBe(3);

      // Test database stats
      response = await request(app)
        .get('/api/cluster/stats')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data.total_keys).toBe(10000);

      // Test nodes endpoint
      response = await request(app)
        .get('/api/cluster/nodes')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data.totalNodes).toBe(3);
      expect(response.body.data.currentLeader).toBe(1);

      // Test replication status
      response = await request(app)
        .get('/api/cluster/replication')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data.currentTerm).toBe(10);
      expect(response.body.data.consensusActive).toBe(true); // Should be true since we have multiple nodes
    });
  });

  describe('Configuration Management', () => {
    it('should allow endpoint reconfiguration', async () => {
      // Get current endpoint
      let response = await request(app)
        .get('/api/admin/current-endpoint')
        .expect(200);

      const originalHost = response.body.host;
      const originalPort = response.body.port;

      // Update endpoint
      response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: 'testhost', port: 9999 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.host).toBe('testhost');
      expect(response.body.port).toBe(9999);

      // Verify endpoint was updated
      response = await request(app)
        .get('/api/admin/current-endpoint')
        .expect(200);

      expect(response.body.host).toBe('testhost');
      expect(response.body.port).toBe(9999);

      // Reset to original values
      await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: originalHost, port: originalPort })
        .expect(200);
    });
  });

  describe('Buffer Value Conversion', () => {
    it('should correctly convert buffer values in responses', async () => {
      const mockRangeResult = {
        success: true,
        key_values: [
          {
            key: Buffer.from('binary_key'),
            value: Buffer.from('binary_value')
          },
          {
            key: 'string_key',
            value: 'string_value'
          }
        ]
      };

      mockClient.getRange.mockImplementation((request, callback) => {
        callback(null, mockRangeResult);
      });

      const response = await request(app)
        .get('/api/keys')
        .expect(200);

      expect(response.body.keyValues).toHaveLength(2);
      
      // Check binary data handling
      const binaryItem = response.body.keyValues[0];
      expect(binaryItem.keyIsBuffer).toBe(true);
      expect(binaryItem.valueIsBuffer).toBe(true);
      expect(binaryItem.key).toBe(Buffer.from('binary_key').toString('base64'));
      expect(binaryItem.value).toBe(Buffer.from('binary_value').toString('base64'));

      // Check string data handling
      const stringItem = response.body.keyValues[1];
      expect(stringItem.keyIsBuffer).toBe(false);
      expect(stringItem.valueIsBuffer).toBe(false);
      expect(stringItem.key).toBe('string_key');
      expect(stringItem.value).toBe('string_value');
    });
  });
});
