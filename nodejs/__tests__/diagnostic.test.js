const request = require('supertest');
const thrift = require('thrift');

// Mock the thrift module
jest.mock('thrift');

// Import the server after mocking
const app = require('../server');

describe('Diagnostic API Endpoints', () => {
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
      getClusterHealth: jest.fn(),
      getDatabaseStats: jest.fn(),
      getNodeInfo: jest.fn()
    };

    thrift.createConnection.mockReturnValue(mockConnection);
    thrift.createClient.mockReturnValue(mockClient);
  });

  describe('GET /api/cluster/health', () => {
    it('should return cluster health data successfully', async () => {
      const mockHealthResult = {
        success: true,
        cluster_health: {
          total_nodes_count: { buffer: { data: [0, 0, 0, 3] } },
          healthy_nodes_count: { buffer: { data: [0, 0, 0, 2] } },
          current_term: { buffer: { data: [0, 0, 0, 5] } },
          current_leader_id: { buffer: { data: [0, 0, 0, 1] } },
          nodes: [
            {
              node_id: { buffer: { data: [0, 0, 0, 1] } },
              status: 'healthy',
              is_leader: true,
              last_seen_timestamp: { buffer: { data: [0, 0, 0, 0, 0, 0, 0, 100] } },
              term: { buffer: { data: [0, 0, 0, 5] } },
              uptime_seconds: { buffer: { data: [0, 0, 0, 0, 0, 0, 3, 232] } }
            },
            {
              node_id: { buffer: { data: [0, 0, 0, 2] } },
              status: 'healthy',
              is_leader: false,
              last_seen_timestamp: { buffer: { data: [0, 0, 0, 0, 0, 0, 0, 99] } },
              term: { buffer: { data: [0, 0, 0, 5] } },
              uptime_seconds: { buffer: { data: [0, 0, 0, 0, 0, 0, 3, 200] } }
            }
          ]
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/health')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: {
          total_nodes_count: 3,
          healthy_nodes_count: 2,
          current_term: 5,
          current_leader_id: 1,
          nodes: expect.arrayContaining([
            expect.objectContaining({
              node_id: 1,
              status: 'healthy',
              is_leader: true,
              term: 5
            }),
            expect.objectContaining({
              node_id: 2,
              status: 'healthy',
              is_leader: false,
              term: 5
            })
          ])
        }
      });
    });

    it('should handle cluster health errors', async () => {
      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(new Error('Cluster unavailable'));
      });

      const response = await request(app)
        .get('/api/cluster/health')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get cluster health',
        details: 'Cluster unavailable'
      });
    });

    it('should handle unsuccessful cluster health response', async () => {
      const mockHealthResult = {
        success: false,
        error: 'No cluster configured'
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/health')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get cluster health',
        details: 'No cluster configured'
      });
    });
  });

  describe('GET /api/cluster/stats', () => {
    it('should return database stats successfully', async () => {
      const mockStatsResult = {
        success: true,
        database_stats: {
          total_keys: { buffer: { data: [0, 0, 0, 0, 0, 0, 39, 16] } },
          total_size_bytes: { buffer: { data: [0, 0, 0, 0, 0, 15, 66, 64] } },
          memory_usage_bytes: { buffer: { data: [0, 0, 0, 0, 0, 1, 0, 0] } },
          disk_usage_bytes: { buffer: { data: [0, 0, 0, 0, 0, 14, 66, 64] } }
        }
      };

      mockClient.getDatabaseStats.mockImplementation((request, callback) => {
        expect(request.include_detailed_stats).toBe(false);
        callback(null, mockStatsResult);
      });

      const response = await request(app)
        .get('/api/cluster/stats')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: {
          total_keys: 10000,
          total_size_bytes: 1000000,
          memory_usage_bytes: 65536,
          disk_usage_bytes: 934464
        }
      });
    });

    it('should handle detailed stats parameter', async () => {
      const mockStatsResult = {
        success: true,
        database_stats: {
          total_keys: { buffer: { data: [0, 0, 0, 100] } },
          detailed_info: 'Additional stats here'
        }
      };

      mockClient.getDatabaseStats.mockImplementation((request, callback) => {
        expect(request.include_detailed_stats).toBe(true);
        callback(null, mockStatsResult);
      });

      const response = await request(app)
        .get('/api/cluster/stats?detailed=true')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: {
          total_keys: 100,
          detailed_info: 'Additional stats here'
        }
      });
    });

    it('should handle database stats errors', async () => {
      mockClient.getDatabaseStats.mockImplementation((request, callback) => {
        callback(new Error('Database connection failed'));
      });

      const response = await request(app)
        .get('/api/cluster/stats')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get database stats',
        details: 'Database connection failed'
      });
    });
  });

  describe('GET /api/cluster/nodes', () => {
    it('should return cluster nodes information', async () => {
      const mockHealthResult = {
        success: true,
        cluster_health: {
          total_nodes_count: { buffer: { data: [0, 0, 0, 2] } },
          healthy_nodes_count: { buffer: { data: [0, 0, 0, 2] } },
          current_leader_id: { buffer: { data: [0, 0, 0, 1] } },
          nodes: [
            {
              node_id: { buffer: { data: [0, 0, 0, 1] } },
              status: 'healthy',
              is_leader: true
            },
            {
              node_id: { buffer: { data: [0, 0, 0, 2] } },
              status: 'healthy',
              is_leader: false
            }
          ]
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/nodes')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: {
          totalNodes: 2,
          healthyNodes: 2,
          currentLeader: 1,
          nodes: expect.arrayContaining([
            expect.objectContaining({
              node_id: 1,
              status: 'healthy',
              is_leader: true
            }),
            expect.objectContaining({
              node_id: 2,
              status: 'healthy',
              is_leader: false
            })
          ])
        }
      });
    });

    it('should handle cluster nodes errors', async () => {
      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(new Error('Cluster query failed'));
      });

      const response = await request(app)
        .get('/api/cluster/nodes')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get cluster nodes',
        details: 'Cluster query failed'
      });
    });
  });

  describe('GET /api/cluster/nodes/:nodeId', () => {
    it('should return specific node information', async () => {
      const mockNodeInfo = {
        node_id: 1,
        status: 'healthy',
        is_leader: true,
        address: 'localhost:9090',
        uptime_seconds: 3600
      };

      mockClient.getNodeInfo.mockImplementation((request, callback) => {
        expect(request.node_id).toBe(1);
        callback(null, {
          success: true,
          node_info: mockNodeInfo
        });
      });

      const response = await request(app)
        .get('/api/cluster/nodes/1')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: mockNodeInfo
      });
    });

    it('should validate node ID parameter', async () => {
      const response = await request(app)
        .get('/api/cluster/nodes/invalid')
        .expect(400);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Invalid node ID'
      });
    });

    it('should handle node info errors', async () => {
      mockClient.getNodeInfo.mockImplementation((request, callback) => {
        callback(new Error('Node not found'));
      });

      const response = await request(app)
        .get('/api/cluster/nodes/999')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get node info',
        details: 'Node not found'
      });
    });
  });

  describe('GET /api/cluster/replication', () => {
    it('should return replication status', async () => {
      const mockHealthResult = {
        success: true,
        cluster_health: {
          current_term: { buffer: { data: [0, 0, 0, 5] } },
          current_leader_id: { buffer: { data: [0, 0, 0, 1] } },
          nodes: [
            {
              node_id: { buffer: { data: [0, 0, 0, 1] } },
              status: 'healthy',
              is_leader: true
            },
            {
              node_id: { buffer: { data: [0, 0, 0, 2] } },
              status: 'healthy',
              is_leader: false
            }
          ]
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/replication')
        .expect(200);

      expect(response.body).toMatchObject({
        success: true,
        data: {
          currentTerm: 5,
          leaderId: 1,
          consensusActive: true,
          nodeStates: {
            1: 'leader',
            2: 'follower'
          },
          replicationLag: {
            1: 0,
            2: 0
          }
        }
      });
    });

    it('should handle single node scenario', async () => {
      const mockHealthResult = {
        success: true,
        cluster_health: {
          current_term: { buffer: { data: [0, 0, 0, 1] } },
          current_leader_id: { buffer: { data: [0, 0, 0, 1] } },
          nodes: [
            {
              node_id: { buffer: { data: [0, 0, 0, 1] } },
              status: 'healthy',
              is_leader: true
            }
          ]
        }
      };

      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(null, mockHealthResult);
      });

      const response = await request(app)
        .get('/api/cluster/replication')
        .expect(200);

      expect(response.body.data.consensusActive).toBe(false);
    });

    it('should handle replication status errors', async () => {
      mockClient.getClusterHealth.mockImplementation((request, callback) => {
        callback(new Error('Replication query failed'));
      });

      const response = await request(app)
        .get('/api/cluster/replication')
        .expect(500);

      expect(response.body).toMatchObject({
        success: false,
        error: 'Failed to get replication status',
        details: 'Replication query failed'
      });
    });
  });
});
