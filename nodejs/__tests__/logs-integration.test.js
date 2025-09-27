const request = require('supertest');
const path = require('path');
const fs = require('fs');
const { execSync } = require('child_process');

// Import the server
const app = require('../server');

describe('Log Viewer Integration Tests', () => {
  let testClusterRoot;
  let testLogFiles;

  beforeAll(() => {
    // Create a temporary cluster structure for integration testing
    testClusterRoot = `/tmp/cluster-integration-test-${Date.now()}`;

    // Create cluster directory structure
    for (let i = 1; i <= 3; i++) {
      const nodeDir = path.join(testClusterRoot, `node${i}`);
      const logsDir = path.join(nodeDir, 'logs');
      const dataDir = path.join(nodeDir, 'data');

      fs.mkdirSync(logsDir, { recursive: true });
      fs.mkdirSync(dataDir, { recursive: true });
    }

    // Create test log files with realistic content
    testLogFiles = [];
    const testLogEntries = [
      '2025-09-26T10:00:00Z INFO Starting thrift server on port 9090',
      '2025-09-26T10:00:01Z DEBUG Connection established from client',
      '2025-09-26T10:00:02Z INFO Processing GET request for key "test"',
      '2025-09-26T10:00:03Z WARN High memory usage detected: 80%',
      '2025-09-26T10:00:04Z ERROR Failed to connect to consensus peer',
      '2025-09-26T10:00:05Z INFO Successfully stored key-value pair',
      '2025-09-26T10:00:06Z DEBUG Performing background compaction',
      '2025-09-26T10:00:07Z INFO Cluster leader election completed',
      '2025-09-26T10:00:08Z ERROR Timeout waiting for consensus response',
      '2025-09-26T10:00:09Z INFO Server shutdown complete'
    ];

    for (let nodeId = 1; nodeId <= 3; nodeId++) {
      const logsDir = path.join(testClusterRoot, `node${nodeId}`, 'logs');

      // Create server.log
      const serverLogPath = path.join(logsDir, 'server.log');
      fs.writeFileSync(serverLogPath, testLogEntries.slice(0, 5).join('\n') + '\n');
      testLogFiles.push({
        path: serverLogPath,
        node: nodeId - 1,
        filename: 'server.log'
      });

      // Create thrift-server log with node-specific content
      const thriftLogPath = path.join(logsDir, `thrift-server-node-${nodeId - 1}.2025-09-26.log`);
      const nodeSpecificEntries = testLogEntries.slice(5).map(entry =>
        entry.replace('9090', `909${nodeId - 1}`)
      );
      fs.writeFileSync(thriftLogPath, nodeSpecificEntries.join('\n') + '\n');
      testLogFiles.push({
        path: thriftLogPath,
        node: nodeId - 1,
        filename: `thrift-server-node-${nodeId - 1}.2025-09-26.log`
      });

      // Create stdout.log
      const stdoutLogPath = path.join(logsDir, 'stdout.log');
      fs.writeFileSync(stdoutLogPath, 'Standard output log content\nAnother stdout line\n');
      testLogFiles.push({
        path: stdoutLogPath,
        node: nodeId - 1,
        filename: 'stdout.log'
      });
    }

    // Configure the app with test cluster configuration
    app.locals.testClusterConfig = {
      nodes: [
        { id: 0, port: 9090 },
        { id: 1, port: 9091 },
        { id: 2, port: 9092 }
      ],
      nodeCount: 3,
      mode: 'cluster'
    };
    app.locals.testClusterLogPath = testClusterRoot;
  });

  afterAll(() => {
    // Clean up test files
    if (fs.existsSync(testClusterRoot)) {
      fs.rmSync(testClusterRoot, { recursive: true, force: true });
    }
  });

  describe('Real File System Operations', () => {
    it('should list actual log files from the file system', async () => {
      const response = await request(app)
        .get('/api/logs/files')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.files).toBeInstanceOf(Array);
      expect(response.body.files.length).toBeGreaterThan(0);

      // Verify that all expected log files are found
      const foundFilenames = response.body.files.map(f => f.filename);
      expect(foundFilenames).toContain('server.log');
      expect(foundFilenames).toContain('stdout.log');
      expect(foundFilenames.some(name => name.includes('thrift-server-node'))).toBe(true);

      // Verify file metadata
      response.body.files.forEach(file => {
        expect(file).toHaveProperty('path');
        expect(file).toHaveProperty('size');
        expect(file).toHaveProperty('modified');
        expect(file).toHaveProperty('node');
        expect(typeof file.size).toBe('number');
        expect(file.size).toBeGreaterThan(0);
      });
    });

    it('should read actual file content using tail command', async () => {
      const testFile = testLogFiles.find(f => f.filename === 'server.log');
      expect(testFile).toBeDefined();

      const response = await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(testFile.path),
          lines: 10
        })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.lines).toBeInstanceOf(Array);
      expect(response.body.lines.length).toBeGreaterThan(0);
      expect(response.body.filePath).toBe(testFile.path);
      expect(response.body.requestedLines).toBe(10);

      // Verify content matches what we wrote
      const content = response.body.lines.join('\n');
      expect(content).toContain('Starting thrift server');
      expect(content).toContain('INFO');
    });

    it('should perform actual grep search on log files', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'ERROR', limit: 10 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.results).toBeInstanceOf(Array);
      expect(response.body.count).toBeGreaterThan(0);
      expect(response.body.query).toBe('ERROR');

      // Verify that search results contain ERROR entries
      response.body.results.forEach(result => {
        expect(result.content.toUpperCase()).toContain('ERROR');
        expect(result).toHaveProperty('file');
      });
    });

    it('should filter search results by node ID', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'INFO', node_id: '0', limit: 5 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.searchPath).toContain('node1/logs');

      if (response.body.results.length > 0) {
        response.body.results.forEach(result => {
          // Results should be from files in the node1 directory
          expect(result.path).toContain('node1/logs');
        });
      }
    });

    it('should get recent logs when no query is provided', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ limit: 20 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.query).toBe('recent logs');
      expect(response.body.results).toBeInstanceOf(Array);
    });

    it('should handle large file content requests', async () => {
      // Create a larger log file
      const largeLogPath = path.join(testClusterRoot, 'node1', 'logs', 'large.log');
      const largeContent = Array.from({ length: 1000 }, (_, i) =>
        `2025-09-26T10:${String(i % 60).padStart(2, '0')}:${String(i % 60).padStart(2, '0')}Z INFO Log entry ${i}`
      ).join('\n');
      fs.writeFileSync(largeLogPath, largeContent);

      const response = await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(largeLogPath),
          lines: 500
        })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.lines.length).toBeLessThanOrEqual(500);
      expect(response.body.requestedLines).toBe(500);

      // Clean up
      fs.unlinkSync(largeLogPath);
    });

    it('should validate file paths against cluster directory', async () => {
      // Try to access a file outside the cluster directory
      const outsideFile = '/etc/passwd';

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(outsideFile) })
        .expect(403);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Access denied');
    });

    it('should handle non-existent files gracefully', async () => {
      const nonExistentPath = path.join(testClusterRoot, 'node1', 'logs', 'nonexistent.log');

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(nonExistentPath) })
        .expect(404);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('File not found');
    });

    it('should handle grep command with no matches', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'NONEXISTENT_PATTERN_12345' })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.results).toEqual([]);
      expect(response.body.count).toBe(0);
    });

    it('should respect result limit parameter', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({ query: 'INFO', limit: 2 })
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.results.length).toBeLessThanOrEqual(2);
    });

    it('should handle directory structure validation', async () => {
      // Verify that the cluster directory structure is properly validated
      const response = await request(app)
        .get('/api/logs/files')
        .expect(200);

      expect(response.body.success).toBe(true);
      expect(response.body.clusterLogPath).toBe(testClusterRoot);

      // Verify that only valid nodes are included
      const nodeNumbers = response.body.files.map(f => f.node);
      nodeNumbers.forEach(nodeNum => {
        expect(nodeNum).toBeGreaterThanOrEqual(0);
        expect(nodeNum).toBeLessThan(3);
      });
    });
  });

  describe('File System Error Handling', () => {
    it('should handle permission errors gracefully', async () => {
      // Create a file with restricted permissions
      const restrictedPath = path.join(testClusterRoot, 'node1', 'logs', 'restricted.log');
      fs.writeFileSync(restrictedPath, 'restricted content');
      fs.chmodSync(restrictedPath, 0o000); // No permissions

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(restrictedPath) })
        .expect(500);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toBe('Failed to read file');

      // Restore permissions for cleanup
      fs.chmodSync(restrictedPath, 0o644);
      fs.unlinkSync(restrictedPath);
    });

    it('should handle corrupted directory structure', async () => {
      // Temporarily rename a node directory
      const originalPath = path.join(testClusterRoot, 'node1');
      const tempPath = path.join(testClusterRoot, 'node1_backup');
      fs.renameSync(originalPath, tempPath);

      const response = await request(app)
        .get('/api/logs/files')
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should still work with remaining nodes
      expect(response.body.files.length).toBeGreaterThan(0);

      // Restore directory
      fs.renameSync(tempPath, originalPath);
    });
  });
});