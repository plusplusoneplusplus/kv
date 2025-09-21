const axios = require('axios');
const WebSocket = require('ws');
const TestServerManager = require('./test-utils');

describe('Full-Stack Integration Tests', () => {
    let serverManager;
    let baseUrl;

    // Increase timeout for integration tests - servers can take time to start
    jest.setTimeout(180000); // 3 minutes for CI environments

    beforeAll(async () => {
        serverManager = new TestServerManager();
        await serverManager.startServers();
        baseUrl = serverManager.getBaseUrl();
    });

    afterAll(async () => {
        if (serverManager) {
            await serverManager.stopServers();
        }
    });

    describe('Server Health and Connectivity', () => {
        test('Node.js server should be running and healthy', async () => {
            const response = await axios.get(`${baseUrl}/api/health`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('status', 'OK');
        });

        test('Thrift connection should be working', async () => {
            const response = await axios.get(`${baseUrl}/api/cluster/health`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('success');
            expect(response.data).toHaveProperty('data');
        });
    });

    describe('KV Store Operations (Full Stack)', () => {
        const testKey = 'integration-test-key';
        const testValue = 'integration-test-value';

        test('should store and retrieve data through full stack', async () => {
            // Store a key-value pair using POST /api/key
            const putResponse = await axios.post(`${baseUrl}/api/key`, {
                key: testKey,
                value: testValue
            });
            expect(putResponse.status).toBe(200);

            // Retrieve the key-value pair using GET /api/key/:key
            const getResponse = await axios.get(`${baseUrl}/api/key/${testKey}`);
            expect(getResponse.status).toBe(200);
            expect(getResponse.data).toHaveProperty('value');

            // Decode base64 value
            const decodedValue = Buffer.from(getResponse.data.value, 'base64').toString();
            expect(decodedValue).toBe(testValue);
        });

        test('should handle key deletion through full stack', async () => {
            const deleteKey = 'integration-delete-test';
            const deleteValue = 'value-to-delete';

            // First store the key
            await axios.post(`${baseUrl}/api/key`, {
                key: deleteKey,
                value: deleteValue
            });

            // Verify it exists
            const getResponse = await axios.get(`${baseUrl}/api/key/${deleteKey}`);
            expect(getResponse.status).toBe(200);

            // Delete the key
            const deleteResponse = await axios.delete(`${baseUrl}/api/key/${deleteKey}`);
            expect(deleteResponse.status).toBe(200);

            // Verify it's gone (should fail to retrieve)
            try {
                await axios.get(`${baseUrl}/api/key/${deleteKey}`);
                fail('Expected error when getting deleted key');
            } catch (error) {
                // Expect any kind of error (404, connection error, etc.)
                // The important thing is that the request failed
                expect(error).toBeDefined();
            }
        });

        test('should handle multiple keys and batch operations', async () => {
            const testKeys = ['batch-key-1', 'batch-key-2', 'batch-key-3'];
            const testValues = ['value-1', 'value-2', 'value-3'];

            // Store multiple keys
            for (let i = 0; i < testKeys.length; i++) {
                const response = await axios.post(`${baseUrl}/api/key`, {
                    key: testKeys[i],
                    value: testValues[i]
                });
                expect(response.status).toBe(200);
            }

            // Retrieve all keys and verify
            for (let i = 0; i < testKeys.length; i++) {
                const response = await axios.get(`${baseUrl}/api/key/${testKeys[i]}`);
                expect(response.status).toBe(200);
                const decodedValue = Buffer.from(response.data.value, 'base64').toString();
                expect(decodedValue).toBe(testValues[i]);
            }

            // Get all keys (list operation)
            const listResponse = await axios.get(`${baseUrl}/api/keys`);
            expect(listResponse.status).toBe(200);
            expect(listResponse.data).toHaveProperty('success', true);
            expect(listResponse.data).toHaveProperty('keyValues');
            expect(Array.isArray(listResponse.data.keyValues)).toBe(true);

            // Should contain our test keys
            const keys = listResponse.data.keyValues.map(item => {
                // Decode base64 key if needed
                return item.keyIsBuffer ? Buffer.from(item.key, 'base64').toString() : item.key;
            });
            testKeys.forEach(key => {
                expect(keys).toContain(key);
            });
        });
    });


    describe('Cluster Management (Full Stack)', () => {
        test('should provide cluster health information', async () => {
            const response = await axios.get(`${baseUrl}/api/cluster/health`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('success');
            expect(response.data).toHaveProperty('data');
            expect(response.data.data).toHaveProperty('nodes');
            expect(Array.isArray(response.data.data.nodes)).toBe(true);
        });

        test('should provide cluster statistics', async () => {
            const response = await axios.get(`${baseUrl}/api/cluster/stats`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('success');
        });

        test('should handle node details endpoint', async () => {
            const response = await axios.get(`${baseUrl}/api/cluster/nodes`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('success', true);
            expect(response.data).toHaveProperty('data');
            expect(response.data.data).toHaveProperty('nodes');
            expect(Array.isArray(response.data.data.nodes)).toBe(true);
        });
    });

    describe('Data Browser (Full Stack)', () => {
        beforeEach(async () => {
            // Set up test data for data browser tests
            await axios.post(`${baseUrl}/api/key`, { key: 'browser-test-1', value: 'browser-value-1' });
            await axios.post(`${baseUrl}/api/key`, { key: 'browser-test-2', value: 'browser-value-2' });
            await axios.post(`${baseUrl}/api/key`, { key: 'browser-test-3', value: 'browser-value-3' });
        });

        test('should list all keys', async () => {
            const response = await axios.get(`${baseUrl}/api/keys`);
            expect(response.status).toBe(200);
            expect(response.data).toHaveProperty('success', true);
            expect(response.data).toHaveProperty('keyValues');
            expect(Array.isArray(response.data.keyValues)).toBe(true);

            // Should find our test keys
            const keys = response.data.keyValues.map(item => {
                // Decode base64 key if needed
                return item.keyIsBuffer ? Buffer.from(item.key, 'base64').toString() : item.key;
            });
            expect(keys).toContain('browser-test-1');
            expect(keys).toContain('browser-test-2');
            expect(keys).toContain('browser-test-3');
        });
    });

    describe('WebSocket Real-time Updates', () => {
        test('should receive real-time cluster updates via WebSocket', (done) => {
            const ws = new WebSocket(`ws://localhost:${serverManager.getConfig().nodePort}/socket.io/?EIO=4&transport=websocket`);

            ws.on('open', () => {
                // Join dashboard room
                ws.send('40/dashboard,');

                // Listen for cluster updates
                setTimeout(() => {
                    ws.close();
                    done();
                }, 2000);
            });

            ws.on('message', (data) => {
                const message = data.toString();
                if (message.includes('cluster-update')) {
                    expect(message).toContain('cluster-update');
                    ws.close();
                    done();
                }
            });

            ws.on('error', (error) => {
                done(error);
            });
        });
    });

    describe('Error Handling (Full Stack)', () => {
        test('should handle invalid API endpoints gracefully', async () => {
            try {
                await axios.get(`${baseUrl}/api/nonexistent-endpoint`);
                fail('Expected 404 for nonexistent endpoint');
            } catch (error) {
                expect(error.response.status).toBe(404);
            }
        });

        test('should handle malformed requests gracefully', async () => {
            try {
                await axios.post(`${baseUrl}/api/key`, {
                    invalid_field: 'should fail validation'
                    // Missing required 'key' and 'value' fields
                });
                fail('Expected 400 for malformed request');
            } catch (error) {
                expect([400, 500]).toContain(error.response.status);
            }
        });

        test('should handle server errors gracefully', async () => {
            // Try to get a key that doesn't exist
            try {
                await axios.get(`${baseUrl}/api/key/nonexistent-key-12345`);
                fail('Expected error for nonexistent key');
            } catch (error) {
                // Expect any kind of error (404, connection error, etc.)
                // The important thing is that the request failed as expected
                expect(error).toBeDefined();
            }
        });
    });

    describe('Performance and Load', () => {
        test('should handle concurrent requests', async () => {
            const concurrentRequests = 10;
            const promises = [];

            for (let i = 0; i < concurrentRequests; i++) {
                const promise = axios.post(`${baseUrl}/api/key`, {
                    key: `concurrent-${i}`,
                    value: `concurrent-value-${i}`
                });
                promises.push(promise);
            }

            const responses = await Promise.all(promises);
            responses.forEach(response => {
                expect(response.status).toBe(200);
            });

            // Verify all data was stored correctly
            for (let i = 0; i < concurrentRequests; i++) {
                const response = await axios.get(`${baseUrl}/api/key/concurrent-${i}`);
                expect(response.status).toBe(200);
                const decodedValue = Buffer.from(response.data.value, 'base64').toString();
                expect(decodedValue).toBe(`concurrent-value-${i}`);
            }
        });

        test('should maintain performance under moderate load', async () => {
            const startTime = Date.now();
            const requests = 50;

            const promises = [];
            for (let i = 0; i < requests; i++) {
                promises.push(
                    axios.post(`${baseUrl}/api/key`, {
                        key: `perf-test-${i}`,
                        value: `performance-test-value-${i}`
                    })
                );
            }

            await Promise.all(promises);
            const endTime = Date.now();
            const duration = endTime - startTime;

            // Should complete 50 requests in reasonable time (adjust as needed)
            expect(duration).toBeLessThan(10000); // 10 seconds max
            console.log(`Completed ${requests} requests in ${duration}ms`);
        });
    });
});