const { spawn } = require('child_process');
const axios = require('axios');
const path = require('path');
const fs = require('fs').promises;
const net = require('net');

/**
 * Test utilities for integration tests that start real servers
 */
class TestServerManager {
    constructor() {
        this.thriftServer = null;
        this.nodeServer = null;
        this.thriftPort = null;
        this.nodePort = null;
        this.testDbPath = null;
    }

    /**
     * Find an available port
     */
    async findAvailablePort(startPort = 9000) {
        return new Promise((resolve, reject) => {
            const server = net.createServer();
            server.listen(startPort, (err) => {
                if (err) {
                    return this.findAvailablePort(startPort + 1).then(resolve).catch(reject);
                }
                const port = server.address().port;
                server.close(() => resolve(port));
            });
        });
    }

    /**
     * Start the Rust Thrift server with test database
     */
    async startThriftServer() {
        this.thriftPort = await this.findAvailablePort(9090);
        this.testDbPath = path.join(__dirname, '../../test-rocksdb-data');

        // Clean up any existing test database
        try {
            await fs.rm(this.testDbPath, { recursive: true, force: true });
        } catch (error) {
            // Ignore if directory doesn't exist
        }

        console.log(`Starting Thrift server on port ${this.thriftPort} with test DB: ${this.testDbPath}`);

        return new Promise((resolve, reject) => {
            const serverPath = path.join(__dirname, '../../../rust/target/debug/thrift-server');

            this.thriftServer = spawn(serverPath, [], {
                cwd: path.join(__dirname, '../../..'),
                env: {
                    ...process.env,
                    THRIFT_PORT: this.thriftPort.toString(),
                    ROCKSDB_PATH: this.testDbPath,
                    RUST_LOG: 'info'
                },
                stdio: ['pipe', 'pipe', 'pipe']
            });

            let output = '';
            this.thriftServer.stdout.on('data', (data) => {
                output += data.toString();
                console.log(`Thrift server: ${data}`);

                // Check if server is ready based on actual server output
                if (output.includes('Server started successfully, waiting for connections')) {
                    resolve();
                }
            });

            this.thriftServer.stderr.on('data', (data) => {
                console.error(`Thrift server error: ${data}`);
            });

            this.thriftServer.on('error', (error) => {
                reject(new Error(`Failed to start Thrift server: ${error.message}`));
            });

            this.thriftServer.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Thrift server exited with code ${code}`));
                }
            });

            // Timeout fallback - assume server is ready after 5 seconds
            setTimeout(() => {
                if (this.thriftServer && this.thriftServer.pid) {
                    resolve();
                }
            }, 5000);
        });
    }

    /**
     * Start the Node.js server
     */
    async startNodeServer() {
        this.nodePort = await this.findAvailablePort(3000);

        console.log(`Starting Node.js server on port ${this.nodePort}`);

        return new Promise((resolve, reject) => {
            const serverPath = path.join(__dirname, '../../server.js');

            this.nodeServer = spawn('node', [serverPath], {
                cwd: path.join(__dirname, '../..'),
                env: {
                    ...process.env,
                    PORT: this.nodePort.toString(),
                    THRIFT_PORT: this.thriftPort.toString(),
                    THRIFT_HOST: 'localhost',
                    NODE_ENV: 'integration-test'
                },
                stdio: ['pipe', 'pipe', 'pipe']
            });

            let output = '';
            this.nodeServer.stdout.on('data', (data) => {
                output += data.toString();
                console.log(`Node.js server: ${data}`);

                // Check if server is ready
                if (output.includes(`KV Store Admin Dashboard running on port ${this.nodePort}`)) {
                    resolve();
                }
            });

            this.nodeServer.stderr.on('data', (data) => {
                console.error(`Node.js server error: ${data}`);
            });

            this.nodeServer.on('error', (error) => {
                reject(new Error(`Failed to start Node.js server: ${error.message}`));
            });

            this.nodeServer.on('exit', (code) => {
                if (code !== 0) {
                    reject(new Error(`Node.js server exited with code ${code}`));
                }
            });

            // Timeout fallback
            setTimeout(() => {
                if (this.nodeServer && this.nodeServer.pid) {
                    resolve();
                }
            }, 5000);
        });
    }

    /**
     * Wait for servers to be healthy
     */
    async waitForServersReady() {
        const maxRetries = 30;
        const retryDelay = 1000;

        console.log('Waiting for servers to be ready...');

        for (let i = 0; i < maxRetries; i++) {
            try {
                // Check Node.js server health
                const response = await axios.get(`http://localhost:${this.nodePort}/api/health`, {
                    timeout: 2000
                });

                if (response.status === 200) {
                    console.log('✅ Servers are ready!');
                    return;
                }
            } catch (error) {
                console.log(`Retry ${i + 1}/${maxRetries}: Servers not ready yet...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));
            }
        }

        throw new Error('Servers failed to become ready within timeout');
    }

    /**
     * Start both servers and wait for readiness
     */
    async startServers() {
        console.log('🚀 Starting integration test servers...');

        await this.startThriftServer();
        await this.startNodeServer();
        await this.waitForServersReady();

        console.log(`✅ Integration test environment ready:`);
        console.log(`   Node.js server: http://localhost:${this.nodePort}`);
        console.log(`   Thrift server: localhost:${this.thriftPort}`);
        console.log(`   Test database: ${this.testDbPath}`);
    }

    /**
     * Stop all servers and clean up
     */
    async stopServers() {
        console.log('🛑 Stopping integration test servers...');

        const stopPromises = [];

        if (this.nodeServer) {
            stopPromises.push(new Promise((resolve) => {
                this.nodeServer.on('exit', resolve);
                this.nodeServer.kill('SIGTERM');

                // Force kill after 5 seconds
                setTimeout(() => {
                    if (this.nodeServer) {
                        this.nodeServer.kill('SIGKILL');
                    }
                    resolve();
                }, 5000);
            }));
        }

        if (this.thriftServer) {
            stopPromises.push(new Promise((resolve) => {
                this.thriftServer.on('exit', resolve);
                this.thriftServer.kill('SIGTERM');

                // Force kill after 5 seconds
                setTimeout(() => {
                    if (this.thriftServer) {
                        this.thriftServer.kill('SIGKILL');
                    }
                    resolve();
                }, 5000);
            }));
        }

        await Promise.all(stopPromises);

        // Clean up test database
        if (this.testDbPath) {
            try {
                await fs.rm(this.testDbPath, { recursive: true, force: true });
                console.log('🗑️  Cleaned up test database');
            } catch (error) {
                console.warn('Warning: Could not clean up test database:', error.message);
            }
        }

        this.thriftServer = null;
        this.nodeServer = null;
        this.thriftPort = null;
        this.nodePort = null;
        this.testDbPath = null;

        console.log('✅ Integration test cleanup complete');
    }

    /**
     * Get the base URL for HTTP requests
     */
    getBaseUrl() {
        return `http://localhost:${this.nodePort}`;
    }

    /**
     * Get server configuration
     */
    getConfig() {
        return {
            nodePort: this.nodePort,
            thriftPort: this.thriftPort,
            baseUrl: this.getBaseUrl(),
            testDbPath: this.testDbPath
        };
    }
}

module.exports = TestServerManager;