// Log viewer functionality
class LogViewer {
    constructor() {
        console.log('LogViewer constructor called');
        this.clusterConfig = null;
        this.logFiles = [];
        this.initializeEventListeners();
        this.loadClusterConfig();
    }

    initializeEventListeners() {
        console.log('Initializing LogViewer event listeners');
        const searchBtn = document.getElementById('searchLogsBtn');
        const clearBtn = document.getElementById('clearLogSearchBtn');
        const recentBtn = document.getElementById('recentLogsBtn');
        const refreshBtn = document.getElementById('refreshLogFilesBtn');

        console.log('Button elements found:', { searchBtn, clearBtn, recentBtn, refreshBtn });

        if (searchBtn) searchBtn.addEventListener('click', () => this.searchLogs());
        if (clearBtn) clearBtn.addEventListener('click', () => this.clearSearch());
        if (recentBtn) recentBtn.addEventListener('click', () => this.showRecentLogs());
        if (refreshBtn) refreshBtn.addEventListener('click', () => this.loadLogFiles());

        // Enter key support for search
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
        console.log('Loading cluster config...');
        try {
            const response = await fetch('/api/cluster/config');
            const data = await response.json();

            if (data.success) {
                this.clusterConfig = data.clusterConfig;
                this.updateClusterStatus(data);
                this.updateNodeFilter();
                await this.loadLogFiles();
            } else {
                this.showMessage('Cluster configuration not available', 'warning');
            }
        } catch (error) {
            console.error('Error loading cluster config:', error);
            this.showMessage('Failed to load cluster configuration', 'error');
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
        } else if (statusSection) {
            statusSection.style.display = 'none';
        }
    }

    updateNodeFilter() {
        const nodeFilter = document.getElementById('logNodeFilter');
        if (!nodeFilter) return;

        // Clear existing options except "All Nodes"
        while (nodeFilter.children.length > 1) {
            nodeFilter.removeChild(nodeFilter.lastChild);
        }

        if (this.clusterConfig?.nodes) {
            this.clusterConfig.nodes.forEach(node => {
                const option = document.createElement('option');
                option.value = node.id;
                option.textContent = `Node ${node.id}`;
                nodeFilter.appendChild(option);
            });
        }
    }

    async loadLogFiles() {
        console.log('Loading log files...');
        try {
            const response = await fetch('/api/logs/files');
            const data = await response.json();

            if (data.success) {
                this.logFiles = data.files;
                this.updateLogFilesTable(data.files);
            } else {
                this.showMessage('Failed to load log files: ' + data.error, 'error');
            }
        } catch (error) {
            console.error('Error loading log files:', error);
            this.showMessage('Failed to load log files', 'error');
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
            row.innerHTML = `
                <td>Node ${file.node}</td>
                <td><code>${file.filename}</code></td>
                <td>${this.formatFileSize(file.size)}</td>
                <td>${new Date(file.modified).toLocaleString()}</td>
            `;

            // Add click handler to load file content
            row.addEventListener('click', () => this.loadFileContent(file));
            row.addEventListener('mouseenter', () => row.style.backgroundColor = '#f8f9fa');
            row.addEventListener('mouseleave', () => row.style.backgroundColor = '');

            tbody.appendChild(row);
        });
    }

    formatFileSize(bytes) {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
    }

    async loadFileContent(file) {
        console.log('Loading file content for:', file.filename);
        this.showLoading(true);

        try {
            // Send file path as query parameter to avoid URL encoding issues
            const response = await fetch(`/api/logs/file?path=${encodeURIComponent(file.path)}&lines=300`);
            const data = await response.json();

            if (data.success) {
                this.displayFileContent(data, file);
            } else {
                this.showMessage('Failed to load file: ' + data.error, 'error');
            }
        } catch (error) {
            console.error('Error loading file:', error);
            this.showMessage('Failed to load file: ' + error.message, 'error');
        } finally {
            this.showLoading(false);
        }
    }

    displayFileContent(data, file) {
        const container = document.getElementById('logResultsContainer');
        const section = document.getElementById('logResultsSection');
        const countBadge = document.getElementById('logResultsCount');
        const searchInfo = document.getElementById('logSearchInfo');

        if (!container || !section) return;

        section.style.display = 'block';
        if (countBadge) countBadge.textContent = `${data.lines.length} lines`;
        if (searchInfo) searchInfo.textContent = `File: ${file.filename} (last ${data.lines.length} lines)`;

        if (data.lines.length === 0) {
            container.innerHTML = '<div class="no-logs-message">File is empty or could not be read.</div>';
            return;
        }

        container.innerHTML = '';
        data.lines.forEach(line => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';

            // Strip ANSI escape codes and show raw content
            const rawContent = line.replace(/\x1b\[[0-9;]*m/g, '');
            entry.innerHTML = `<div class="log-content">${this.escapeHtml(rawContent)}</div>`;

            container.appendChild(entry);
        });
    }

    async searchLogs() {
        console.log('SearchLogs button clicked');
        const query = document.getElementById('logSearchQuery')?.value.trim() || '';
        const nodeId = document.getElementById('logNodeFilter')?.value || '';
        const limit = document.getElementById('logResultLimit')?.value || '100';

        console.log('Search params:', { query, nodeId, limit });
        this.showLoading(true);

        try {
            let url = `/api/logs/search?limit=${limit}`;
            if (query) url += `&query=${encodeURIComponent(query)}`;
            if (nodeId) url += `&node_id=${nodeId}`;

            console.log('Fetching:', url);
            const response = await fetch(url);
            const data = await response.json();

            console.log('Search response:', data);

            if (data.success) {
                this.displaySearchResults(data);
            } else {
                this.showMessage('Search failed: ' + data.error, 'error');
            }
        } catch (error) {
            console.error('Error searching logs:', error);
            this.showMessage('Search failed: ' + error.message, 'error');
        } finally {
            this.showLoading(false);
        }
    }

    async showRecentLogs() {
        console.log('Recent logs button clicked');
        // Clear search query and search for recent logs
        const searchInput = document.getElementById('logSearchQuery');
        if (searchInput) searchInput.value = '';
        await this.searchLogs();
    }

    displaySearchResults(data) {
        const container = document.getElementById('logResultsContainer');
        const section = document.getElementById('logResultsSection');
        const countBadge = document.getElementById('logResultsCount');
        const searchInfo = document.getElementById('logSearchInfo');

        if (!container || !section) return;

        section.style.display = 'block';
        if (countBadge) countBadge.textContent = `${data.count} results`;
        if (searchInfo) searchInfo.textContent = `Query: "${data.query}" | Path: ${data.searchPath}`;

        if (data.results.length === 0) {
            container.innerHTML = '<div class="no-logs-message">No log entries found matching your search criteria.</div>';
            return;
        }

        container.innerHTML = '';
        data.results.forEach(result => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';

            // Strip ANSI escape codes and show raw content
            const rawContent = result.content.replace(/\x1b\[[0-9;]*m/g, '');
            entry.innerHTML = `<div class="log-content">${this.escapeHtml(rawContent)}</div>`;

            container.appendChild(entry);
        });
    }

    getLogEntryClass(content) {
        const upperContent = content.toUpperCase();
        if (upperContent.includes('ERROR')) return 'log-entry-error';
        if (upperContent.includes('WARN')) return 'log-entry-warn';
        if (upperContent.includes('INFO')) return 'log-entry-info';
        if (upperContent.includes('DEBUG')) return 'log-entry-debug';
        return '';
    }

    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
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

    showMessage(message, type) {
        console.log(`${type.toUpperCase()}: ${message}`);
        alert(`${message}`); // Simple fallback
    }
}

// Initialize log viewer when this script loads
function initializeLogViewer() {
    console.log('initializeLogViewer called');
    if (typeof window.logViewer === 'undefined') {
        console.log('Creating new LogViewer instance');
        window.logViewer = new LogViewer();
    } else {
        console.log('LogViewer already exists');
    }
}

// Make the function globally available
window.initializeLogViewer = initializeLogViewer;