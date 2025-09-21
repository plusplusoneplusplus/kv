const request = require('supertest');
const express = require('express');
const thrift = require('thrift');

// Mock the thrift module
jest.mock('thrift');

// Import the server after mocking
const app = require('../server');

describe('KV Store API Endpoints', () => {
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
      getDatabaseStats: jest.fn(),
      getNodeInfo: jest.fn()
    };

    thrift.createConnection.mockReturnValue(mockConnection);
    thrift.createClient.mockReturnValue(mockClient);
  });

  describe('GET /api/ping', () => {
    it('should return successful ping response', async () => {
      const mockPingResult = {
        message: 'pong',
        timestamp: '1234567890',
        server_timestamp: '1234567891'
      };

      mockClient.ping.mockImplementation((request, callback) => {
        callback(null, mockPingResult);
      });

      const response = await request(app)
        .get('/api/ping')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        message: 'pong',
        clientTimestamp: '1234567890',
        serverTimestamp: '1234567891'
      });
      expect(response.body.roundTripTime).toBeDefined();
    });

    it('should handle ping errors', async () => {
      mockClient.ping.mockImplementation((request, callback) => {
        callback(new Error('Connection failed'));
      });

      const response = await request(app)
        .get('/api/ping')
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Failed to ping server',
        details: 'Connection failed'
      });
    });
  });

  describe('GET /api/keys', () => {
    it('should return key-value pairs successfully', async () => {
      const mockRangeResult = {
        success: true,
        key_values: [
          { key: 'test1', value: 'value1' },
          { key: 'test2', value: 'value2' }
        ]
      };

      mockClient.getRange.mockImplementation((request, callback) => {
        callback(null, mockRangeResult);
      });

      const response = await request(app)
        .get('/api/keys')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        count: 2,
        keyValues: [
          { key: 'test1', value: 'value1', keyIsBuffer: false, valueIsBuffer: false },
          { key: 'test2', value: 'value2', keyIsBuffer: false, valueIsBuffer: false }
        ]
      });
    });

    it('should handle limit parameter', async () => {
      const mockRangeResult = {
        success: true,
        key_values: [{ key: 'test1', value: 'value1' }]
      };

      mockClient.getRange.mockImplementation((request, callback) => {
        expect(request.limit).toBe(5);
        callback(null, mockRangeResult);
      });

      await request(app)
        .get('/api/keys?limit=5')
        .expect(200);
    });

    it('should handle prefix filtering', async () => {
      const mockRangeResult = {
        success: true,
        key_values: [
          { key: 'prefix_test1', value: 'value1' },
          { key: 'prefix_test2', value: 'value2' }
        ]
      };

      mockClient.getRange.mockImplementation((request, callback) => {
        // The start_key should be the prefix
        callback(null, mockRangeResult);
      });

      const response = await request(app)
        .get('/api/keys?prefix=prefix_')
        .expect(200);

      expect(response.body.keyValues).toHaveLength(2);
    });

    it('should handle range query errors', async () => {
      mockClient.getRange.mockImplementation((request, callback) => {
        callback(new Error('Database error'));
      });

      const response = await request(app)
        .get('/api/keys')
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Failed to fetch keys',
        details: 'Database error'
      });
    });
  });

  describe('GET /api/key/:key', () => {
    it('should return existing key', async () => {
      const mockGetResult = {
        found: true,
        value: 'test_value',
        error: null
      };

      mockClient.get.mockImplementation((request, callback) => {
        expect(request.key).toBe('test_key');
        callback(null, mockGetResult);
      });

      const response = await request(app)
        .get('/api/key/test_key')
        .expect(200);

      expect(response.body).toMatchObject({
        key: 'test_key',
        value: 'test_value',
        valueIsBuffer: false,
        found: true
      });
    });

    it('should handle non-existent key', async () => {
      const mockGetResult = {
        found: false,
        value: null,
        error: null
      };

      mockClient.get.mockImplementation((request, callback) => {
        callback(null, mockGetResult);
      });

      const response = await request(app)
        .get('/api/key/nonexistent')
        .expect(200);

      expect(response.body).toMatchObject({
        key: 'nonexistent',
        value: '',
        valueIsBuffer: false,
        found: false
      });
    });

    it('should handle get errors', async () => {
      mockClient.get.mockImplementation((request, callback) => {
        callback(new Error('Get operation failed'));
      });

      const response = await request(app)
        .get('/api/key/test_key')
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Failed to fetch key',
        details: 'Get operation failed'
      });
    });
  });

  describe('POST /api/key', () => {
    it('should set key-value pair successfully', async () => {
      const mockSetResult = {
        success: true,
        error: null
      };

      mockClient.setKey.mockImplementation((request, callback) => {
        expect(request.key).toBe('test_key');
        expect(request.value).toBe('test_value');
        callback(null, mockSetResult);
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test_key', value: 'test_value' })
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        key: 'test_key',
        value: 'test_value'
      });
    });

    it('should validate required fields', async () => {
      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test_key' }) // missing value
        .expect(400);

      expect(response.body).toMatchObject({
        error: 'Key and value are required'
      });
    });

    it('should handle set operation errors', async () => {
      const mockSetResult = {
        success: false,
        error: 'Database write failed'
      };

      mockClient.setKey.mockImplementation((request, callback) => {
        callback(null, mockSetResult);
      });

      const response = await request(app)
        .post('/api/key')
        .send({ key: 'test_key', value: 'test_value' })
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Set operation failed',
        details: 'Database write failed'
      });
    });
  });

  describe('DELETE /api/key/:key', () => {
    it('should delete key successfully', async () => {
      const mockDeleteResult = {
        success: true,
        error: null
      };

      mockClient.deleteKey.mockImplementation((request, callback) => {
        expect(request.key).toBe('test_key');
        callback(null, mockDeleteResult);
      });

      const response = await request(app)
        .delete('/api/key/test_key')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        key: 'test_key',
        deleted: true
      });
    });

    it('should handle delete operation errors', async () => {
      const mockDeleteResult = {
        success: false,
        error: 'Key not found'
      };

      mockClient.deleteKey.mockImplementation((request, callback) => {
        callback(null, mockDeleteResult);
      });

      const response = await request(app)
        .delete('/api/key/nonexistent')
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Delete operation failed',
        details: 'Key not found'
      });
    });

    it('should handle thrift client errors', async () => {
      mockClient.deleteKey.mockImplementation((request, callback) => {
        callback(new Error('Connection lost'));
      });

      const response = await request(app)
        .delete('/api/key/test_key')
        .expect(500);

      expect(response.body).toMatchObject({
        error: 'Failed to delete key',
        details: 'Connection lost'
      });
    });
  });

  describe('GET /api/admin/current-endpoint', () => {
    it('should return current endpoint configuration', async () => {
      const response = await request(app)
        .get('/api/admin/current-endpoint')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        host: expect.any(String)
      });
      expect(typeof response.body.port === 'number' || typeof response.body.port === 'string').toBe(true);
    });
  });

  describe('POST /api/admin/update-endpoint', () => {
    it('should update endpoint configuration', async () => {
      const response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: 'newhost', port: 9091 })
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        host: 'newhost',
        port: 9091,
        message: 'Endpoint updated to newhost:9091'
      });
    });

    it('should validate host and port', async () => {
      const response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: 'test' }) // missing port
        .expect(400);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Host and port are required'
      });
    });

    it('should validate port range', async () => {
      const response = await request(app)
        .post('/api/admin/update-endpoint')
        .send({ host: 'localhost', port: 70000 }) // invalid port
        .expect(400);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Port must be a number between 1 and 65535'
      });
    });
  });
});
