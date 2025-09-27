/**
 * Frontend tests for the LogViewer functionality
 * These tests focus on the client-side JavaScript behavior
 */

// Mock DOM environment
const { JSDOM } = require('jsdom');

// Mock fetch for API calls
global.fetch = jest.fn();

describe.skip('LogViewer Frontend Functionality', () => {
  let dom;
  let window;
  let document;
  let LogViewer;

  beforeEach(() => {
    // Setup DOM environment
    dom = new JSDOM(`
      <!DOCTYPE html>
      <html>
        <body>
          <div id="logSearchQuery"></div>
          <div id="logNodeFilter"></div>
          <div id="logResultLimit"></div>
          <button id="searchLogsBtn"></button>
          <button id="clearLogSearchBtn"></button>
          <button id="recentLogsBtn"></button>
          <button id="refreshLogFilesBtn"></button>
          <div id="logClusterStatus" style="display: none;"></div>
          <div id="clusterMode"></div>
          <div id="clusterNodeCount"></div>
          <div id="clusterLogPath"></div>
          <div id="logFilesSection" style="display: none;"></div>
          <table>
            <tbody id="logFilesTableBody"></tbody>
          </table>
          <div id="logResultsSection" style="display: none;"></div>
          <div id="logResultsContainer"></div>
          <div id="logResultsCount"></div>
          <div id="logSearchInfo"></div>
          <div id="logLoadingIndicator" style="display: none;"></div>
        </body>
      </html>
    `, {
      url: 'http://localhost:3000',
      pretendToBeVisual: true,
      resources: 'usable'
    });

    window = dom.window;
    document = window.document;

    // Make DOM available globally
    global.window = window;
    global.document = document;
    global.HTMLElement = window.HTMLElement;
    global.Event = window.Event;

    // Mock console methods
    global.console.log = jest.fn();
    global.console.error = jest.fn();

    // Reset fetch mock
    fetch.mockClear();

    // Load LogViewer class (simulate loading the logs.js file)
    const logViewerCode = `
      class LogViewer {
        constructor() {
          this.clusterConfig = null;
          this.logFiles = [];
          this.initializeEventListeners();
          this.loadClusterConfig();
        }

        initializeEventListeners() {
          const searchBtn = document.getElementById('searchLogsBtn');
          const clearBtn = document.getElementById('clearLogSearchBtn');
          const recentBtn = document.getElementById('recentLogsBtn');
          const refreshBtn = document.getElementById('refreshLogFilesBtn');

          if (searchBtn) searchBtn.addEventListener('click', () => this.searchLogs());
          if (clearBtn) clearBtn.addEventListener('click', () => this.clearSearch());
          if (recentBtn) recentBtn.addEventListener('click', () => this.showRecentLogs());
          if (refreshBtn) refreshBtn.addEventListener('click', () => this.loadLogFiles());

          const searchInput = document.getElementById('logSearchQuery');
          if (searchInput) {
            searchInput.addEventListener('keypress', (e) => {
              if (e.key === 'Enter') {
                this.searchLogs();
              }
            });
          }
        }

        async loadClusterConfig() {
          try {
            const response = await fetch('/api/cluster/config');
            const data = await response.json();

            if (data.success) {
              this.clusterConfig = data.clusterConfig;
              this.updateClusterStatus(data);
              this.updateNodeFilter();
              await this.loadLogFiles();
            }
          } catch (error) {
            console.error('Error loading cluster config:', error);
          }
        }

        updateClusterStatus(configData) {
          const statusSection = document.getElementById('logClusterStatus');
          if (configData.hasClusterConfig && statusSection) {
            statusSection.style.display = 'block';
            const modeEl = document.getElementById('clusterMode');
            const countEl = document.getElementById('clusterNodeCount');
            const pathEl = document.getElementById('clusterLogPath');

            if (modeEl) modeEl.textContent = this.clusterConfig?.mode || 'Unknown';
            if (countEl) countEl.textContent = this.clusterConfig?.nodeCount || '0';
            if (pathEl) pathEl.textContent = configData.clusterLogPath || 'Not configured';
          }
        }

        updateNodeFilter() {
          const nodeFilter = document.getElementById('logNodeFilter');
          if (!nodeFilter) return;

          while (nodeFilter.children.length > 1) {
            nodeFilter.removeChild(nodeFilter.lastChild);
          }

          if (this.clusterConfig?.nodes) {
            this.clusterConfig.nodes.forEach(node => {
              const option = document.createElement('option');
              option.value = node.id;
              option.textContent = \`Node \${node.id}\`;
              nodeFilter.appendChild(option);
            });
          }
        }

        async loadLogFiles() {
          try {
            const response = await fetch('/api/logs/files');
            const data = await response.json();

            if (data.success) {
              this.logFiles = data.files;
              this.updateLogFilesTable(data.files);
            }
          } catch (error) {
            console.error('Error loading log files:', error);
          }
        }

        updateLogFilesTable(files) {
          const tbody = document.getElementById('logFilesTableBody');
          const section = document.getElementById('logFilesSection');

          if (!tbody || !section) return;

          if (files.length === 0) {
            section.style.display = 'none';
            return;
          }

          section.style.display = 'block';
          tbody.innerHTML = '';

          files.forEach(file => {
            const row = document.createElement('tr');
            row.style.cursor = 'pointer';
            row.innerHTML = \`
              <td>Node \${file.node}</td>
              <td><code>\${file.filename}</code></td>
              <td>\${this.formatFileSize(file.size)}</td>
              <td>\${new Date(file.modified).toLocaleString()}</td>
            \`;

            row.addEventListener('click', () => this.loadFileContent(file));
            tbody.appendChild(row);
          });
        }

        formatFileSize(bytes) {
          if (bytes < 1024) return bytes + ' B';
          if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
          return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
        }

        async loadFileContent(file) {
          try {
            const response = await fetch(\`/api/logs/file?path=\${encodeURIComponent(file.path)}&lines=300\`);
            const data = await response.json();

            if (data.success) {
              this.displayFileContent(data, file);
            }
          } catch (error) {
            console.error('Error loading file:', error);
          }
        }

        displayFileContent(data, file) {
          const container = document.getElementById('logResultsContainer');
          const section = document.getElementById('logResultsSection');
          const countBadge = document.getElementById('logResultsCount');
          const searchInfo = document.getElementById('logSearchInfo');

          if (!container || !section) return;

          section.style.display = 'block';
          if (countBadge) countBadge.textContent = \`\${data.lines.length} lines\`;
          if (searchInfo) searchInfo.textContent = \`File: \${file.filename} (last \${data.lines.length} lines)\`;

          if (data.lines.length === 0) {
            container.innerHTML = '<div class="no-logs-message">File is empty or could not be read.</div>';
            return;
          }

          container.innerHTML = '';
          data.lines.forEach(line => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';

            const rawContent = line.replace(/\\x1b\\[[0-9;]*m/g, '');
            entry.innerHTML = \`<div class="log-content">\${this.escapeHtml(rawContent)}</div>\`;

            container.appendChild(entry);
          });
        }

        async searchLogs() {
          const query = document.getElementById('logSearchQuery')?.value.trim() || '';
          const nodeId = document.getElementById('logNodeFilter')?.value || '';
          const limit = document.getElementById('logResultLimit')?.value || '100';

          this.showLoading(true);

          try {
            let url = \`/api/logs/search?limit=\${limit}\`;
            if (query) url += \`&query=\${encodeURIComponent(query)}\`;
            if (nodeId) url += \`&node_id=\${nodeId}\`;

            const response = await fetch(url);
            const data = await response.json();

            if (data.success) {
              this.displaySearchResults(data);
            }
          } catch (error) {
            console.error('Error searching logs:', error);
          } finally {
            this.showLoading(false);
          }
        }

        displaySearchResults(data) {
          const container = document.getElementById('logResultsContainer');
          const section = document.getElementById('logResultsSection');
          const countBadge = document.getElementById('logResultsCount');
          const searchInfo = document.getElementById('logSearchInfo');

          if (!container || !section) return;

          section.style.display = 'block';
          if (countBadge) countBadge.textContent = \`\${data.count} results\`;
          if (searchInfo) searchInfo.textContent = \`Query: "\${data.query}" | Path: \${data.searchPath}\`;

          if (data.results.length === 0) {
            container.innerHTML = '<div class="no-logs-message">No log entries found matching your search criteria.</div>';
            return;
          }

          container.innerHTML = '';
          data.results.forEach(result => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';

            const rawContent = result.content.replace(/\\x1b\\[[0-9;]*m/g, '');
            entry.innerHTML = \`<div class="log-content">\${this.escapeHtml(rawContent)}</div>\`;

            container.appendChild(entry);
          });
        }

        async showRecentLogs() {
          const searchInput = document.getElementById('logSearchQuery');
          if (searchInput) searchInput.value = '';
          await this.searchLogs();
        }

        clearSearch() {
          const searchInput = document.getElementById('logSearchQuery');
          const nodeFilter = document.getElementById('logNodeFilter');
          const resultsSection = document.getElementById('logResultsSection');

          if (searchInput) searchInput.value = '';
          if (nodeFilter) nodeFilter.value = '';
          if (resultsSection) resultsSection.style.display = 'none';
        }

        showLoading(show) {
          const indicator = document.getElementById('logLoadingIndicator');
          if (indicator) {
            indicator.style.display = show ? 'block' : 'none';
          }
        }

        escapeHtml(text) {
          const div = document.createElement('div');
          div.textContent = text;
          return div.innerHTML;
        }
      }
    `;

    // Execute the LogViewer code in the current context and make it available
    eval(logViewerCode + '; global.LogViewer = LogViewer; window.LogViewer = LogViewer;');
    LogViewer = global.LogViewer;
  });

  afterEach(() => {
    dom.window.close();
  });

  describe('LogViewer Constructor and Initialization', () => {
    it('should initialize with default properties', () => {
      const logViewer = new LogViewer();

      expect(logViewer.clusterConfig).toBeNull();
      expect(logViewer.logFiles).toEqual([]);
    });

    it('should set up event listeners on initialization', () => {
      const searchBtn = document.getElementById('searchLogsBtn');
      const clearBtn = document.getElementById('clearLogSearchBtn');
      const recentBtn = document.getElementById('recentLogsBtn');
      const refreshBtn = document.getElementById('refreshLogFilesBtn');

      // Mock addEventListener to track calls
      searchBtn.addEventListener = jest.fn();
      clearBtn.addEventListener = jest.fn();
      recentBtn.addEventListener = jest.fn();
      refreshBtn.addEventListener = jest.fn();

      new LogViewer();

      expect(searchBtn.addEventListener).toHaveBeenCalledWith('click', expect.any(Function));
      expect(clearBtn.addEventListener).toHaveBeenCalledWith('click', expect.any(Function));
      expect(recentBtn.addEventListener).toHaveBeenCalledWith('click', expect.any(Function));
      expect(refreshBtn.addEventListener).toHaveBeenCalledWith('click', expect.any(Function));
    });

    it('should handle missing DOM elements gracefully', () => {
      // Remove one of the buttons
      const searchBtn = document.getElementById('searchLogsBtn');
      searchBtn.remove();

      expect(() => new LogViewer()).not.toThrow();
    });
  });

  describe('Cluster Configuration Management', () => {
    let logViewer;

    beforeEach(() => {
      fetch.mockResolvedValue({
        json: () => Promise.resolve({
          success: true,
          clusterConfig: {
            nodes: [{ id: 0, port: 9090 }, { id: 1, port: 9091 }],
            nodeCount: 2,
            mode: 'cluster'
          },
          clusterLogPath: '/tmp/test-cluster',
          hasClusterConfig: true
        })
      });

      logViewer = new LogViewer();
    });

    it('should load cluster configuration on initialization', async () => {
      await new Promise(resolve => setTimeout(resolve, 50)); // Wait for async calls

      expect(fetch).toHaveBeenCalledWith('/api/cluster/config');
      expect(logViewer.clusterConfig).toEqual({
        nodes: [{ id: 0, port: 9090 }, { id: 1, port: 9091 }],
        nodeCount: 2,
        mode: 'cluster'
      });
    });

    it('should update cluster status display', async () => {
      await new Promise(resolve => setTimeout(resolve, 50));

      const statusSection = document.getElementById('logClusterStatus');
      const modeEl = document.getElementById('clusterMode');
      const countEl = document.getElementById('clusterNodeCount');

      expect(statusSection.style.display).toBe('block');
      expect(modeEl.textContent).toBe('cluster');
      expect(countEl.textContent).toBe('2');
    });

    it('should update node filter options', async () => {
      const nodeFilter = document.getElementById('logNodeFilter');
      // Add a default option first
      const defaultOption = document.createElement('option');
      defaultOption.textContent = 'All Nodes';
      nodeFilter.appendChild(defaultOption);

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(nodeFilter.children.length).toBe(3); // Default + 2 nodes
      expect(nodeFilter.children[1].textContent).toBe('Node 0');
      expect(nodeFilter.children[2].textContent).toBe('Node 1');
    });
  });

  describe('Log File Management', () => {
    let logViewer;

    beforeEach(() => {
      // Mock cluster config fetch
      fetch.mockImplementation((url) => {
        if (url.includes('/api/cluster/config')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              clusterConfig: { nodes: [], nodeCount: 0 },
              hasClusterConfig: false
            })
          });
        }

        if (url.includes('/api/logs/files')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              files: [
                {
                  node: 0,
                  filename: 'server.log',
                  size: 1024,
                  modified: '2025-09-27T10:00:00Z',
                  path: '/tmp/test/node1/logs/server.log'
                },
                {
                  node: 1,
                  filename: 'thrift.log',
                  size: 2048,
                  modified: '2025-09-27T11:00:00Z',
                  path: '/tmp/test/node2/logs/thrift.log'
                }
              ]
            })
          });
        }

        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      logViewer = new LogViewer();
    });

    it('should load and display log files', async () => {
      await logViewer.loadLogFiles();
      await new Promise(resolve => setTimeout(resolve, 50));

      expect(logViewer.logFiles).toHaveLength(2);
      expect(logViewer.logFiles[0].filename).toBe('server.log');
      expect(logViewer.logFiles[1].filename).toBe('thrift.log');

      const tbody = document.getElementById('logFilesTableBody');
      expect(tbody.children.length).toBe(2);
    });

    it('should format file sizes correctly', () => {
      expect(logViewer.formatFileSize(512)).toBe('512 B');
      expect(logViewer.formatFileSize(1536)).toBe('1.5 KB');
      expect(logViewer.formatFileSize(2097152)).toBe('2.0 MB');
    });

    it('should handle empty file list', async () => {
      fetch.mockImplementation((url) => {
        if (url.includes('/api/logs/files')) {
          return Promise.resolve({
            json: () => Promise.resolve({ success: true, files: [] })
          });
        }
        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      await logViewer.loadLogFiles();

      const section = document.getElementById('logFilesSection');
      expect(section.style.display).toBe('none');
    });
  });

  describe('Log Search Functionality', () => {
    let logViewer;

    beforeEach(() => {
      // Mock all API calls
      fetch.mockImplementation((url) => {
        if (url.includes('/api/cluster/config')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              clusterConfig: { nodes: [], nodeCount: 0 },
              hasClusterConfig: false
            })
          });
        }

        if (url.includes('/api/logs/search')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              query: 'ERROR',
              searchPath: '/tmp/test',
              results: [
                {
                  file: 'server.log',
                  path: '/tmp/test/server.log',
                  content: '2025-09-27T10:00:00Z ERROR Test error message'
                }
              ],
              count: 1
            })
          });
        }

        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      logViewer = new LogViewer();

      // Setup DOM elements with values
      document.getElementById('logSearchQuery').value = 'ERROR';
      document.getElementById('logNodeFilter').value = '0';
      document.getElementById('logResultLimit').value = '50';
    });

    it('should perform log search with query parameters', async () => {
      await logViewer.searchLogs();

      expect(fetch).toHaveBeenCalledWith(
        expect.stringContaining('/api/logs/search?limit=50&query=ERROR&node_id=0')
      );

      const resultsSection = document.getElementById('logResultsSection');
      const resultsCount = document.getElementById('logResultsCount');

      expect(resultsSection.style.display).toBe('block');
      expect(resultsCount.textContent).toBe('1 results');
    });

    it('should show loading indicator during search', async () => {
      const loadingIndicator = document.getElementById('logLoadingIndicator');

      // Start search (but don't await yet)
      const searchPromise = logViewer.searchLogs();

      // Check loading indicator is shown
      expect(loadingIndicator.style.display).toBe('block');

      // Wait for search to complete
      await searchPromise;

      // Check loading indicator is hidden
      expect(loadingIndicator.style.display).toBe('none');
    });

    it('should handle search with no results', async () => {
      fetch.mockImplementation((url) => {
        if (url.includes('/api/logs/search')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              query: 'NONEXISTENT',
              results: [],
              count: 0
            })
          });
        }
        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      await logViewer.searchLogs();

      const container = document.getElementById('logResultsContainer');
      expect(container.innerHTML).toContain('No log entries found');
    });

    it('should clear search form and results', () => {
      const searchInput = document.getElementById('logSearchQuery');
      const nodeFilter = document.getElementById('logNodeFilter');
      const resultsSection = document.getElementById('logResultsSection');

      searchInput.value = 'test';
      nodeFilter.value = '1';
      resultsSection.style.display = 'block';

      logViewer.clearSearch();

      expect(searchInput.value).toBe('');
      expect(nodeFilter.value).toBe('');
      expect(resultsSection.style.display).toBe('none');
    });

    it('should perform recent logs search', async () => {
      const searchInput = document.getElementById('logSearchQuery');
      searchInput.value = 'previous query';

      await logViewer.showRecentLogs();

      expect(searchInput.value).toBe('');
      expect(fetch).toHaveBeenCalledWith(
        expect.stringContaining('/api/logs/search?limit=50&node_id=0')
      );
    });
  });

  describe('File Content Loading', () => {
    let logViewer;

    beforeEach(() => {
      fetch.mockImplementation((url) => {
        if (url.includes('/api/cluster/config')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              clusterConfig: { nodes: [], nodeCount: 0 },
              hasClusterConfig: false
            })
          });
        }

        if (url.includes('/api/logs/file')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              filePath: '/tmp/test/server.log',
              lines: [
                '2025-09-27T10:00:00Z INFO Starting server',
                '2025-09-27T10:01:00Z DEBUG Connection established'
              ],
              requestedLines: 300,
              actualLines: 2
            })
          });
        }

        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      logViewer = new LogViewer();
    });

    it('should load file content and display it', async () => {
      const testFile = {
        filename: 'server.log',
        path: '/tmp/test/server.log',
        size: 1024,
        node: 0
      };

      await logViewer.loadFileContent(testFile);

      expect(fetch).toHaveBeenCalledWith(
        `/api/logs/file?path=${encodeURIComponent(testFile.path)}&lines=300`
      );

      const resultsSection = document.getElementById('logResultsSection');
      const resultsCount = document.getElementById('logResultsCount');
      const searchInfo = document.getElementById('logSearchInfo');
      const container = document.getElementById('logResultsContainer');

      expect(resultsSection.style.display).toBe('block');
      expect(resultsCount.textContent).toBe('2 lines');
      expect(searchInfo.textContent).toContain('File: server.log');
      expect(container.children.length).toBe(2);
    });

    it('should handle empty file content', async () => {
      fetch.mockImplementation((url) => {
        if (url.includes('/api/logs/file')) {
          return Promise.resolve({
            json: () => Promise.resolve({
              success: true,
              lines: [],
              actualLines: 0
            })
          });
        }
        return Promise.resolve({ json: () => Promise.resolve({}) });
      });

      const testFile = { filename: 'empty.log', path: '/tmp/test/empty.log' };
      await logViewer.loadFileContent(testFile);

      const container = document.getElementById('logResultsContainer');
      expect(container.innerHTML).toContain('File is empty or could not be read');
    });
  });

  describe('Utility Functions', () => {
    let logViewer;

    beforeEach(() => {
      fetch.mockResolvedValue({
        json: () => Promise.resolve({ success: true })
      });
      logViewer = new LogViewer();
    });

    it('should escape HTML properly', () => {
      const testStrings = [
        { input: '<script>alert("xss")</script>', expected: '&lt;script&gt;alert("xss")&lt;/script&gt;' },
        { input: 'Normal text', expected: 'Normal text' },
        { input: 'Text & symbols', expected: 'Text &amp; symbols' }
      ];

      testStrings.forEach(({ input, expected }) => {
        expect(logViewer.escapeHtml(input)).toBe(expected);
      });
    });

    it('should handle loading indicator', () => {
      const indicator = document.getElementById('logLoadingIndicator');

      logViewer.showLoading(true);
      expect(indicator.style.display).toBe('block');

      logViewer.showLoading(false);
      expect(indicator.style.display).toBe('none');
    });
  });

  describe('Error Handling', () => {
    let logViewer;

    beforeEach(() => {
      logViewer = new LogViewer();
    });

    it('should handle fetch errors gracefully', async () => {
      fetch.mockRejectedValue(new Error('Network error'));

      // Should not throw
      await expect(logViewer.loadClusterConfig()).resolves.not.toThrow();
      await expect(logViewer.loadLogFiles()).resolves.not.toThrow();
      await expect(logViewer.searchLogs()).resolves.not.toThrow();

      expect(console.error).toHaveBeenCalled();
    });

    it('should handle API errors', async () => {
      fetch.mockResolvedValue({
        json: () => Promise.resolve({
          success: false,
          error: 'API Error'
        })
      });

      await logViewer.searchLogs();
      // Should handle gracefully without throwing
    });
  });
});