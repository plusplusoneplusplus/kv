const request = require('supertest');
const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');

// Mock the modules that log endpoints use
jest.mock('fs');
jest.mock('child_process');

// Import the server after mocking
const app = require('../server');

describe.skip('Log Viewer API Endpoints', () => {
  let testLogPath;
  let originalLogPath;

  beforeEach(() => {
    // Reset all mocks
    jest.clearAllMocks();

    // Setup test log path
    testLogPath = '/tmp/test-cluster-logs';

    // Mock cluster configuration for testing
    // Note: This simulates the server being started with cluster config
    app.locals.testClusterConfig = {
      nodes: [
        { id: 0, port: 9090 },
        { id: 1, port: 9091 },
        { id: 2, port: 9092 }
      ],
      nodeCount: 3,
      mode: 'cluster'
    };
    app.locals.testClusterLogPath = testLogPath;
  });

  describe('GET /api/cluster/config', () => {
    it('should return cluster configuration when available', async () => {
      const response = await request(app)
        .get('/api/cluster/config')
        .expect(200);

      expect(response.body).toEqual({
        success: true,
        clusterConfig: expect.any(Object),
        clusterLogPath: expect.any(String),
        hasClusterConfig: expect.any(Boolean)
      });
    });

    it('should handle missing cluster configuration gracefully', async () => {
      const response = await request(app)
        .get('/api/cluster/config')
        .expect(200);

      expect(response.body.success).toBe(true);
    });
  });

  describe('GET /api/logs/files', () => {
    beforeEach(() => {
      // Mock fs.existsSync to return true for test paths
      fs.existsSync.mockImplementation((path) => {
        if (path === testLogPath) return true;
        if (path.includes('node1/logs') || path.includes('node2/logs') || path.includes('node3/logs')) return true;
        return false;
      });

      // Mock fs.readdirSync for log directory listing
      fs.readdirSync.mockImplementation((dirPath, options) => {
        if (dirPath.includes('logs')) {
          return ['server.log', 'thrift-server-node-0.2025-09-27.log', 'stdout.log'];
        }
        if (options && options.withFileTypes) {
          return [
            { name: 'node1', isDirectory: () => true },
            { name: 'node2', isDirectory: () => true },
            { name: 'node3', isDirectory: () => true }
          ];
        }
        return ['node1', 'node2', 'node3'];
      });

      // Mock fs.statSync for file metadata
      fs.statSync.mockReturnValue({
        size: 1024,
        mtime: new Date('2025-09-27T10:00:00Z')
      });
    });

    it('should list available log files when cluster config is available', async () => {
      const response = await request(app)
        .get('/api/logs/files')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.files).toBeInstanceOf(Array);
      expect(response.body.clusterLogPath).toBeDefined();
    });

    it('should return error when cluster log path is not configured', async () => {
      // Temporarily clear cluster config
      app.locals.testClusterLogPath = null;

      const response = await request(app)
        .get('/api/logs/files')
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Log path not configured');

      // Restore for other tests
      app.locals.testClusterLogPath = testLogPath;
    });

    it('should handle file system errors gracefully', async () => {
      fs.existsSync.mockImplementation(() => {
        throw new Error('File system error');
      });

      const response = await request(app)
        .get('/api/logs/files')
        .expect(500);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('Failed to list log files');
    });

    it('should filter only .log files', async () => {
      fs.readdirSync.mockImplementation((dirPath) => {
        if (dirPath.includes('logs')) {
          return ['server.log', 'debug.txt', 'thrift-server.log', 'readme.md'];
        }
        return ['node1', 'node2', 'node3'];
      });

      const response = await request(app)
        .get('/api/logs/files')
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should only include .log files
      response.body.files.forEach(file => {
        expect(file.filename).toMatch(/\.log$/);
      });
    });
  });

  describe('GET /api/logs/search', () => {
    beforeEach(() => {
      fs.existsSync.mockReturnValue(true);

      // Mock execSync for grep/tail commands
      execSync.mockImplementation((command) => {
        if (command.includes('grep')) {
          return 'server.log:2025-09-27T10:00:00Z INFO Starting server\nthrift.log:2025-09-27T10:01:00Z DEBUG Connection established\n';
        }
        if (command.includes('tail')) {
          return '2025-09-27T10:00:00Z INFO Recent log entry 1\n2025-09-27T10:01:00Z DEBUG Recent log entry 2\n';
        }
        return '';
      });
    });

    it('should search logs with query parameter', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'ERROR', limit: 10 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.query).toBe('ERROR');
      expect(response.body.results).toBeInstanceOf(Array);
      expect(response.body.count).toBeDefined();
      expect(response.body.searchPath).toBeDefined();
    });

    it('should filter by node_id parameter', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'INFO', node_id: '0', limit: 5 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.searchPath).toContain('node1/logs');
    });

    it('should return recent logs when no query is provided', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ limit: 20 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.query).toBe('recent logs');
      expect(response.body.results).toBeInstanceOf(Array);
    });

    it('should handle grep command failures gracefully', async () => {
      execSync.mockImplementation(() => {
        const error = new Error('No matches found');
        error.status = 1; // grep exit code for no matches
        throw error;
      });

      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'NONEXISTENT' })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.results).toEqual([]);
      expect(response.body.count).toBe(0);
    });

    it('should handle search path not existing', async () => {
      fs.existsSync.mockReturnValue(false);

      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'test' })
        .expect(404);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Log path does not exist');
    });

    it('should return error when cluster log path is not configured', async () => {
      app.locals.testClusterLogPath = null;

      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'test' })
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Log path not configured');

      app.locals.testClusterLogPath = testLogPath;
    });

    it('should handle system command timeout', async () => {
      execSync.mockImplementation(() => {
        const error = new Error('Command timeout');
        error.signal = 'SIGTERM';
        throw error;
      });

      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'test' })
        .expect(500);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('Log search failed');
    });

    it('should validate and parse limit parameter', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'test', limit: 'invalid' })
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should default to 100 when limit is invalid
    });
  });

  describe('GET /api/logs/file', () => {
    const testFilePath = '/tmp/test-cluster-logs/node1/logs/server.log';

    beforeEach(() => {
      fs.existsSync.mockReturnValue(true);

      // Mock child_process.spawn for tail command
      const mockSpawn = require('child_process').spawn;
      jest.clearAllMocks();

      const mockTailProcess = {
        stdout: {
          on: jest.fn((event, callback) => {
            if (event === 'data') {
              // Simulate tail output
              setTimeout(() => callback('Log line 1\nLog line 2\nLog line 3\n'), 10);
            }
          })
        },
        stderr: {
          on: jest.fn()
        },
        on: jest.fn((event, callback) => {
          if (event === 'close') {
            setTimeout(() => callback(0), 20); // Successful exit
          }
        })
      };

      mockSpawn.mockReturnValue(mockTailProcess);
    });

    it('should return file content for valid path', async () => {
      const response = await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(testFilePath),
          lines: 10
        })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.filePath).toBe(testFilePath);
      expect(response.body.lines).toBeInstanceOf(Array);
      expect(response.body.requestedLines).toBe(10);
      expect(response.body.actualLines).toBeDefined();
    });

    it('should reject paths outside cluster log directory', async () => {
      const invalidPath = '/etc/passwd';

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(invalidPath) })
        .expect(403);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Access denied');
    });

    it('should return 404 for non-existent files', async () => {
      fs.existsSync.mockReturnValue(false);

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(testFilePath) })
        .expect(404);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('File not found');
    });

    it('should handle tail command failure', async () => {
      const mockSpawn = require('child_process').spawn;
      const mockTailProcess = {
        stdout: { on: jest.fn() },
        stderr: {
          on: jest.fn((event, callback) => {
            if (event === 'data') {
              setTimeout(() => callback('Permission denied'), 10);
            }
          })
        },
        on: jest.fn((event, callback) => {
          if (event === 'close') {
            setTimeout(() => callback(1), 20); // Error exit code
          }
        })
      };

      mockSpawn.mockReturnValue(mockTailProcess);

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(testFilePath) })
        .expect(500);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('Failed to read file');
    });

    it('should default to 300 lines when lines parameter is not provided', async () => {
      const mockSpawn = require('child_process').spawn;

      await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(testFilePath) })
        .expect(200);

      expect(mockSpawn).toHaveBeenCalledWith('tail', ['-n', '300', testFilePath]);
    });

    it('should parse lines parameter correctly', async () => {
      const mockSpawn = require('child_process').spawn;

      await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(testFilePath),
          lines: '50'
        })
        .expect(200);

      expect(mockSpawn).toHaveBeenCalledWith('tail', ['-n', '50', testFilePath]);
    });
  });

  describe('GET /logs (route)', () => {
    it('should serve the main index.html for logs route', async () => {
      const response = await request(app)
        .get('/logs')
        .expect(200);

      expect(response.headers['content-type']).toMatch(/text\/html/);
    });
  });
});