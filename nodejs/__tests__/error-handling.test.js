const request = require('supertest');
const thrift = require('thrift');

// Mock the thrift module
jest.mock('thrift');

// Import the server after mocking
const app = require('../server');

describe('Error Handling and Edge Cases', () => {
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

  describe('Input Validation', () => {
    it('should reject empty key in POST /api/key', async () => {
      const response = await request(app)
        .post('/api/key')
        .send({ key: '', value: 'test' })
        .expect(400);

      expect(response.body.error).toBe('Key and value are required');
    });

    it('should reject missing value in POST /api/key', async () => {
      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test' })
        .expect(400);

      expect(response.body.error).toBe('Key and value are required');
    });

    it('should handle undefined value in POST /api/key', async () => {
      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test' }) // value is undefined
        .expect(400);

      expect(response.body.error).toBe('Key and value are required');
    });

    it('should accept zero as a valid value', async () => {
      mockClient.setKey.mockImplementation((request, callback) => {
        expect(request.value).toBe('0');
        callback(null, { success: true });
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test', value: 0 })
        .expect(200);

      expect(response.body.success).toBe(true);
    });

    it('should accept empty string as a valid value', async () => {
      mockClient.setKey.mockImplementation((request, callback) => {
        expect(request.value).toBe('');
        callback(null, { success: true });
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test', value: '' })
        .expect(200);

      expect(response.body.success).toBe(true);
    });
  });

  describe('Special Characters and Encoding', () => {
    it('should handle keys with special characters', async () => {
      const specialKey = 'key/with:special@chars#and$symbols%';
      
      mockClient.get.mockImplementation((request, callback) => {
        expect(request.key).toBe(specialKey);
        callback(null, { found: true, value: 'test_value' });
      });

      const response = await request(app)
        .get(`/api/key/${encodeURIComponent(specialKey)}`)
        .expect(200);

      expect(response.body.found).toBe(true);
    });

    it('should handle Unicode characters in keys and values', async () => {
      const unicodeKey = 'key_测试_🚀';
      const unicodeValue = 'value_测试_🎉';

      mockClient.setKey.mockImplementation((request, callback) => {
        expect(request.key).toBe(unicodeKey);
        expect(request.value).toBe(unicodeValue);
        callback(null, { success: true });
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: unicodeKey, value: unicodeValue })
        .expect(200);

      expect(response.body.success).toBe(true);
    });

    it('should handle very long keys', async () => {
      const longKey = 'a'.repeat(1000);
      
      mockClient.get.mockImplementation((request, callback) => {
        expect(request.key).toBe(longKey);
        callback(null, { found: false });
      });

      const response = await request(app)
        .get(`/api/key/${longKey}`)
        .expect(200);

      expect(response.body.found).toBe(false);
    });

    it('should handle very long values', async () => {
      const longValue = 'x'.repeat(10000);
      
      mockClient.setKey.mockImplementation((request, callback) => {
        expect(request.value).toBe(longValue);
        callback(null, { success: true });
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test', value: longValue })
        .expect(200);

      expect(response.body.success).toBe(true);
    });
  });

  describe('Thrift Connection Failures', () => {
    it('should handle connection timeout', async () => {
      mockClient.ping.mockImplementation((request, callback) => {
        // Simulate timeout by not calling callback
        setTimeout(() => {
          callback(new Error('ETIMEDOUT'));
        }, 100);
      });

      const response = await request(app)
        .get('/api/ping')
        .expect(500);

      expect(response.body.error).toBe('Failed to ping server');
      expect(response.body.details).toBe('ETIMEDOUT');
    });

    it('should handle connection refused', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        callback(new Error('ECONNREFUSED'));
      });

      const response = await request(app)
        .get('/api/keys')
        .expect(500);

      expect(response.body.error).toBe('Failed to fetch keys');
      expect(response.body.details).toBe('ECONNREFUSED');
    });

    it('should handle network interruption', async () => {
      mockClient.get.mockImplementation((request, callback) => {
        callback(new Error('ENETUNREACH'));
      });

      const response = await request(app)
        .get('/api/key/test')
        .expect(500);

      expect(response.body.error).toBe('Failed to fetch key');
      expect(response.body.details).toBe('ENETUNREACH');
    });
  });

  describe('Malformed Thrift Responses', () => {
    it('should handle null response from thrift client', async () => {
      mockClient.get.mockImplementation((request, callback) => {
        callback(null, null);
      });

      const response = await request(app)
        .get('/api/key/test')
        .expect(500);

      expect(response.body.error).toBe('Internal server error');
    });

    it('should handle malformed cluster health response', async () => {
      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, { success: true, cluster_health: null });
      });

      const response = await request(app)
        .get('/api/cluster/health')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.data).toBeNull();
    });

    it('should handle missing buffer data in numeric fields', async () => {
      const mockHealthResult = {
        success: true,
        cluster_health: {
          total_nodes_count: null,
          healthy_nodes_count: { buffer: null },
          current_term: { buffer: { data: null } },
          current_leader_id: { buffer: { data: [] } }
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/health')
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should handle gracefully without crashing
    });
  });

  describe('Rate Limiting', () => {
    it('should be disabled in test environment', async () => {
      // In test environment, rate limiting should be disabled
      // Make multiple requests to verify no rate limiting
      const requests = [];
      for (let i = 0; i < 10; i++) {
        requests.push(
          request(app)
            .get('/api/admin/current-endpoint')
        );
      }

      const responses = await Promise.all(requests);
      
      // All requests should succeed (no rate limiting)
      responses.forEach(response => {
        expect(response.status).toBe(200);
      });
    });
  });

  describe('Clear All Data Edge Cases', () => {
    it('should require exact confirmation phrase', async () => {
      const response = await request(app)
        .delete('/api/admin/clear-all')
        .send({ confirmation: 'delete all data' }) // wrong case
        .expect(400);

      expect(response.body.error).toBe('Missing confirmation');
    });

    it('should handle empty database during clear all', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        callback(null, {
          success: true,
          key_values: []
        });
      });

      const response = await request(app)
        .delete('/api/admin/clear-all')
        .send({ confirmation: 'DELETE ALL DATA' })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.message).toBe('No keys to delete');
      expect(response.body.deletedCount).toBe(0);
    });

    it('should handle partial deletion failures', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        callback(null, {
          success: true,
          key_values: [
            { key: 'key1' },
            { key: 'key2' },
            { key: 'key3' }
          ]
        });
      });

      let deleteCallCount = 0;
      mockClient.deleteKey.mockImplementation((request, callback) => {
        deleteCallCount++;
        if (deleteCallCount === 2) {
          // Fail the second deletion
          callback(null, { success: false, error: 'Delete failed' });
        } else {
          callback(null, { success: true });
        }
      });

      const response = await request(app)
        .delete('/api/admin/clear-all')
        .send({ confirmation: 'DELETE ALL DATA' })
        .expect(200);

      expect(response.body.success).toBe(false);
      expect(response.body.deletedCount).toBe(2);
      expect(response.body.totalKeys).toBe(3);
      expect(response.body.errors).toHaveLength(1);
    });
  });

  describe('Query Parameter Edge Cases', () => {
    it('should handle invalid limit parameter', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        // Should default to 1000 when limit is invalid
        expect(request.limit).toBe(1000);
        callback(null, { success: true, key_values: [] });
      });

      await request(app)
        .get('/api/keys?limit=invalid')
        .expect(200);
    });

    it('should handle negative limit parameter', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        // Negative limit is passed through as-is
        expect(request.limit).toBe(-10);
        callback(null, { success: true, key_values: [] });
      });

      const response = await request(app)
        .get('/api/keys?limit=-10')
        .expect(200);
        
      expect(response.body.success).toBe(true);
    });

    it('should handle extremely large limit parameter', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        expect(request.limit).toBe(999999999);
        callback(null, { success: true, key_values: [] });
      });

      await request(app)
        .get('/api/keys?limit=999999999')
        .expect(200);
    });
  });

  describe('Endpoint Configuration Edge Cases', () => {
    it('should reject invalid host formats', async () => {
      // Test missing host
      let response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ port: 9090 }); // no host

      expect(response.status).toBe(400);
      expect(response.body.success).toBe(false);

      // Test host with spaces (definitely invalid format)
      response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: 'host with spaces', port: 9090 });

      expect(response.status).toBe(400);
      expect(response.body.error).toBe('Invalid host format');
    });

    it('should reject invalid port numbers', async () => {
      const invalidPorts = [
        0,
        -1,
        65536,
        99999,
        'abc',
        null,
        undefined
      ];

      for (const port of invalidPorts) {
        const response = await request(app)
          .post('/api/admin/update-endpoint')
          .send({ host: 'localhost', port })
          .expect(400);

        expect(response.body.success).toBe(false);
      }
    });
  });

  describe('Concurrent Request Handling', () => {
    it('should handle multiple concurrent requests', async () => {
      mockClient.ping.mockImplementation((request, callback) => {
        // Simulate some processing time
        setTimeout(() => {
          callback(null, {
            message: 'pong',
            timestamp: request.timestamp,
            server_timestamp: Date.now().toString()
          });
        }, Math.random() * 50);
      });

      const concurrentRequests = [];
      for (let i = 0; i < 20; i++) {
        concurrentRequests.push(
          request(app).get('/api/ping')
        );
      }

      const responses = await Promise.all(concurrentRequests);
      
      // All requests should succeed
      responses.forEach(response => {
        expect(response.status).toBe(200);
        expect(response.body.success).toBe(true);
      });
    }, 5000);
  });
});
