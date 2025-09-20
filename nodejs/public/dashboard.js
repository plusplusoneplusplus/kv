// Global variables for dashboard features
let dashboardSocket;
let clusterRefreshInterval;
let nodeRefreshInterval;
let replicationRefreshInterval;
let transactionChart;
let latencyChart;
let lagTrendsChart;
let replicationHealthChart;
let currentNodeId = 1; // Default node for Node Details tab

// Tab switching functionality
function showTab(tabName) {
    // Hide all tab contents
    const tabs = document.querySelectorAll('.tab-content');
    tabs.forEach(tab => tab.classList.remove('active'));

    // Remove active class from all tab buttons
    const buttons = document.querySelectorAll('.tab-button');
    buttons.forEach(button => button.classList.remove('active'));

    // Show selected tab content
    let tabElement;
    switch(tabName) {
        case 'browse':
            tabElement = document.getElementById('browseTab');
            break;
        case 'clear':
            tabElement = document.getElementById('clearTab');
            break;
        case 'settings':
            tabElement = document.getElementById('settingsTab');
            break;
        case 'cluster':
            tabElement = document.getElementById('clusterTab');
            initializeClusterDashboard();
            break;
        case 'nodeDetails':
            tabElement = document.getElementById('nodeDetailsTab');
            initializeNodeDetails();
            break;
        case 'replication':
            tabElement = document.getElementById('replicationTab');
            initializeReplicationMonitor();
            break;
    }

    if (tabElement) {
        tabElement.classList.add('active');
    }

    // Add active class to clicked tab button
    const clickedButton = document.querySelector(`[onclick="showTab('${tabName}')"]`);
    if (clickedButton) {
        clickedButton.classList.add('active');
    }
}

// Initialize cluster dashboard
function initializeClusterDashboard() {
    if (!dashboardSocket) {
        dashboardSocket = io();
        dashboardSocket.on('connect', () => {
            console.log('Connected to dashboard server');
        });
        dashboardSocket.on('cluster-update', (data) => {
            updateClusterDashboard(data);
        });
    }

    refreshClusterDashboard();
    if (clusterRefreshInterval) clearInterval(clusterRefreshInterval);
    clusterRefreshInterval = setInterval(refreshClusterDashboard, 10000);
}

async function refreshClusterDashboard() {
    try {
        const [healthResponse, statsResponse] = await Promise.all([
            fetch('/api/cluster/health'),
            fetch('/api/cluster/stats?detailed=true')
        ]);

        const healthData = await healthResponse.json();
        const statsData = await statsResponse.json();

        if (healthData.success) {
            updateClusterDashboard(healthData.data);
        }

        if (statsData.success) {
            updateClusterDatabaseStats(statsData.data);
        }

        document.getElementById('cluster-last-updated').textContent = new Date().toLocaleTimeString();
    } catch (error) {
        console.error('Error refreshing cluster dashboard:', error);
        showClusterAlert('Failed to refresh dashboard data', 'danger');
    }
}

function updateClusterDashboard(data) {
    // Update overview metrics
    document.getElementById('cluster-total-nodes').textContent = data.total_nodes_count || 0;
    document.getElementById('cluster-healthy-nodes').textContent = data.healthy_nodes_count || 0;
    document.getElementById('cluster-current-leader').textContent = data.current_leader_id !== null ? `Node ${data.current_leader_id}` : 'None';
    document.getElementById('cluster-current-term').textContent = data.current_term || 0;

    // Update cluster health
    const healthElement = document.getElementById('cluster-health');
    const healthCard = document.getElementById('cluster-health-card');
    const healthIcon = document.getElementById('cluster-health-icon');
    const status = data.cluster_status || 'unknown';

    healthElement.textContent = status.charAt(0).toUpperCase() + status.slice(1);

    // Update health card styling
    healthCard.className = 'card dashboard-card';
    healthIcon.className = 'fas fa-2x';

    switch (status) {
        case 'healthy':
            healthCard.classList.add('bg-success', 'text-white');
            healthIcon.classList.add('fa-shield-alt', 'opacity-75');
            break;
        case 'degraded':
            healthCard.classList.add('bg-warning', 'text-dark');
            healthIcon.classList.add('fa-exclamation-triangle', 'opacity-75');
            break;
        case 'unhealthy':
            healthCard.classList.add('bg-danger', 'text-white');
            healthIcon.classList.add('fa-times-circle', 'opacity-75');
            break;
        default:
            healthCard.classList.add('bg-secondary', 'text-white');
            healthIcon.classList.add('fa-question-circle', 'opacity-75');
    }

    // Update nodes grid
    updateClusterNodesGrid(data.nodes || []);
}

function updateClusterNodesGrid(nodes) {
    const grid = document.getElementById('cluster-nodes-grid');
    grid.innerHTML = '';

    nodes.forEach(node => {
        const nodeCard = document.createElement('div');
        nodeCard.className = 'col-md-4 col-lg-3 mb-3';

        const statusClass = getStatusClass(node.status);
        const leaderBadge = node.is_leader ? '<span class="leader-badge">LEADER</span>' : '';

        nodeCard.innerHTML = `
            <div class="card node-card h-100" onclick="viewNodeDetails(${node.node_id})">
                <div class="card-body text-center position-relative">
                    ${leaderBadge}
                    <i class="fas fa-server fa-2x mb-2 text-primary"></i>
                    <h6 class="card-title">Node ${node.node_id}</h6>
                    <p class="card-text">
                        <span class="status-indicator ${statusClass}"></span>
                        ${node.status.charAt(0).toUpperCase() + node.status.slice(1)}
                    </p>
                    <small class="text-muted">${node.endpoint || 'N/A'}</small>
                    ${node.status === 'unreachable' ? '<div class="alert-badge">!</div>' : ''}
                </div>
            </div>
        `;

        grid.appendChild(nodeCard);
    });
}

async function updateClusterDatabaseStats(stats) {
    document.getElementById('cluster-total-keys').textContent = stats.total_keys?.toLocaleString() || '0';
    document.getElementById('cluster-total-size').textContent = formatBytes(stats.total_size_bytes || 0);
    document.getElementById('cluster-active-transactions').textContent = stats.active_transactions || 0;
    document.getElementById('cluster-cache-hit-rate').textContent = stats.cache_hit_rate_percent ? `${stats.cache_hit_rate_percent}%` : 'N/A';
    document.getElementById('cluster-avg-response-time').textContent = stats.average_response_time_ms ? `${stats.average_response_time_ms} ms` : 'N/A';
    document.getElementById('cluster-compaction-pending').textContent = formatBytes(stats.compaction_pending_bytes || 0);
}

function getStatusClass(status) {
    switch (status) {
        case 'healthy': return 'status-healthy';
        case 'degraded': return 'status-degraded';
        case 'unhealthy': return 'status-unhealthy';
        case 'unreachable': return 'status-unreachable';
        default: return 'status-unreachable';
    }
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function viewNodeDetails(nodeId) {
    currentNodeId = nodeId;
    showTab('nodeDetails');
}

function showClusterAlert(message, type) {
    const alertContainer = document.getElementById('cluster-alerts-container');
    const alertId = 'alert-' + Date.now();

    const alert = document.createElement('div');
    alert.id = alertId;
    alert.className = `alert alert-${type} alert-dismissible fade show`;
    alert.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;

    alertContainer.appendChild(alert);

    // Auto-dismiss after 5 seconds
    setTimeout(() => {
        const alertElement = document.getElementById(alertId);
        if (alertElement && alertElement.parentNode) {
            alertElement.remove();
        }
    }, 5000);
}

function showClusterDiagnostics() {
    alert('Cluster diagnostic details would be shown here. This feature can be expanded to show detailed cluster diagnostics.');
}

// Initialize node details
function initializeNodeDetails() {
    document.getElementById('node-name').textContent = `Node ${currentNodeId}`;
    document.getElementById('node-id-detail').textContent = currentNodeId;

    initializeNodeCharts();
    loadNodeData();

    if (nodeRefreshInterval) clearInterval(nodeRefreshInterval);
    nodeRefreshInterval = setInterval(loadNodeData, 10000);
}

function initializeNodeCharts() {
    // Transaction History Chart
    const transactionCtx = document.getElementById('nodeTransactionChart').getContext('2d');
    transactionChart = new Chart(transactionCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Transactions/sec',
                data: [],
                borderColor: '#007bff',
                backgroundColor: 'rgba(0, 123, 255, 0.1)',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });

    // Latency Chart
    const latencyCtx = document.getElementById('nodeLatencyChart').getContext('2d');
    latencyChart = new Chart(latencyCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Latency (ms)',
                data: [],
                borderColor: '#28a745',
                backgroundColor: 'rgba(40, 167, 69, 0.1)',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

async function loadNodeData() {
    try {
        const [nodeResponse, healthResponse] = await Promise.all([
            fetch(`/api/cluster/nodes/${currentNodeId}`),
            fetch('/api/cluster/health')
        ]);

        const nodeData = await nodeResponse.json();
        const healthData = await healthResponse.json();

        if (nodeData.success) {
            updateNodeData(nodeData.data);
        }

        if (healthData.success) {
            // Find this node in the cluster health data
            const thisNode = healthData.data.nodes?.find(n => n.node_id == currentNodeId);
            if (thisNode) {
                updateNodeStatus(thisNode);
            }
        }

        document.getElementById('node-loading-state').classList.add('d-none');
        document.getElementById('node-content').classList.remove('d-none');
        document.getElementById('node-last-updated').textContent = new Date().toLocaleTimeString();

    } catch (error) {
        console.error('Error loading node data:', error);
        alert('Failed to load node data');
    }
}

function updateNodeData(data) {
    // Update basic info
    document.getElementById('node-endpoint').textContent = data.endpoint || 'N/A';

    // Update metrics with mock data
    updateNodeMetrics();
    updateNodeConnectionPool();
    updateNodeRecentTransactions();
    updateNodeActivityTimeline();
    updateNodeCharts();
}

function updateNodeStatus(nodeData) {
    const statusBadge = document.getElementById('node-status-badge');
    const leaderBadge = document.getElementById('node-leader-badge');

    // Update status
    const status = nodeData.status || 'unknown';
    statusBadge.textContent = status.charAt(0).toUpperCase() + status.slice(1);
    statusBadge.className = `node-status-badge status-${status}`;

    // Update leader indicator
    if (nodeData.is_leader) {
        leaderBadge.classList.remove('d-none');
        document.getElementById('node-role').textContent = 'Leader';
    } else {
        leaderBadge.classList.add('d-none');
        document.getElementById('node-role').textContent = 'Follower';
    }

    // Update other node info
    document.getElementById('node-term').textContent = nodeData.term || '-';
    document.getElementById('node-last-heartbeat').textContent = new Date().toLocaleString();
    document.getElementById('node-join-time').textContent = new Date(Date.now() - Math.random() * 24 * 60 * 60 * 1000).toLocaleString();
}

function updateNodeMetrics() {
    // Mock data for demonstration
    document.getElementById('node-uptime-value').textContent = Math.floor(Math.random() * 30) + 'd';
    document.getElementById('node-connections-value').textContent = Math.floor(Math.random() * 100) + 10;
    document.getElementById('node-transactions-value').textContent = Math.floor(Math.random() * 1000) + 100;
    document.getElementById('node-latency-value').textContent = (Math.random() * 10 + 1).toFixed(1);
}

function updateNodeConnectionPool() {
    const active = Math.floor(Math.random() * 50) + 10;
    const max = 100;
    const idle = Math.floor(Math.random() * 20) + 5;
    const errors = Math.floor(Math.random() * 5);
    const usage = Math.floor((active / max) * 100);

    document.getElementById('node-active-connections').textContent = active;
    document.getElementById('node-max-pool-size').textContent = max;
    document.getElementById('node-idle-connections').textContent = idle;
    document.getElementById('node-connection-errors').textContent = errors;
    document.getElementById('node-pool-usage-percent').textContent = `${usage}%`;
    document.getElementById('node-pool-usage-bar').style.width = `${usage}%`;

    // Update progress bar color based on usage
    const progressBar = document.getElementById('node-pool-usage-bar');
    progressBar.className = 'progress-bar';
    if (usage > 80) {
        progressBar.classList.add('bg-danger');
    } else if (usage > 60) {
        progressBar.classList.add('bg-warning');
    } else {
        progressBar.classList.add('bg-success');
    }
}

function updateNodeRecentTransactions() {
    const container = document.getElementById('node-recent-transactions');
    const transactions = [];

    // Generate mock transactions
    for (let i = 0; i < 10; i++) {
        const types = ['GET', 'PUT', 'DELETE', 'RANGE'];
        const type = types[Math.floor(Math.random() * types.length)];
        const timestamp = new Date(Date.now() - Math.random() * 60000);
        const latency = (Math.random() * 50 + 1).toFixed(1);

        transactions.push({
            type,
            timestamp,
            latency,
            key: `key_${Math.floor(Math.random() * 1000)}`
        });
    }

    container.innerHTML = transactions.map(tx => `
        <div class="transaction-item">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${tx.type}</strong> ${tx.key}
                </div>
                <div class="text-end">
                    <small class="text-muted">${tx.latency}ms</small><br>
                    <small class="text-muted">${tx.timestamp.toLocaleTimeString()}</small>
                </div>
            </div>
        </div>
    `).join('');
}

function updateNodeActivityTimeline() {
    const container = document.getElementById('node-activity-timeline');
    const activities = [
        { time: new Date(), event: 'Node started successfully', type: 'success' },
        { time: new Date(Date.now() - 300000), event: 'Elected as cluster leader', type: 'info' },
        { time: new Date(Date.now() - 600000), event: 'Joined cluster', type: 'info' },
        { time: new Date(Date.now() - 900000), event: 'Connection pool initialized', type: 'success' },
        { time: new Date(Date.now() - 1200000), event: 'Database connection established', type: 'success' }
    ];

    container.innerHTML = activities.map(activity => `
        <div class="timeline-item">
            <div class="fw-bold">${activity.event}</div>
            <small class="text-muted">${activity.time.toLocaleString()}</small>
        </div>
    `).join('');
}

function updateNodeCharts() {
    const now = new Date();
    const timeLabels = [];
    const transactionData = [];
    const latencyData = [];

    // Generate mock chart data
    for (let i = 9; i >= 0; i--) {
        const time = new Date(now.getTime() - i * 30000);
        timeLabels.push(time.toLocaleTimeString());
        transactionData.push(Math.floor(Math.random() * 100) + 50);
        latencyData.push(Math.random() * 20 + 5);
    }

    // Update transaction chart
    if (transactionChart) {
        transactionChart.data.labels = timeLabels;
        transactionChart.data.datasets[0].data = transactionData;
        transactionChart.update('none');
    }

    // Update latency chart
    if (latencyChart) {
        latencyChart.data.labels = timeLabels;
        latencyChart.data.datasets[0].data = latencyData;
        latencyChart.update('none');
    }
}

function refreshNodeData() {
    loadNodeData();
}

function showNodeConnectionDetails() {
    alert('Connection details would be shown here. This feature can be expanded to show detailed connection information.');
}

function showNodeDiagnostics() {
    alert('Node diagnostics would be run here. This feature can be expanded to show detailed diagnostic information.');
}

function exportNodeReport() {
    alert('Node report export would be implemented here. This feature can be expanded to generate downloadable reports.');
}

// Initialize replication monitor
function initializeReplicationMonitor() {
    initializeReplicationCharts();
    loadReplicationData();

    if (replicationRefreshInterval) clearInterval(replicationRefreshInterval);
    replicationRefreshInterval = setInterval(loadReplicationData, 5000);
}

function initializeReplicationCharts() {
    // Lag Trends Chart
    const lagCtx = document.getElementById('lagTrendsChart').getContext('2d');
    lagTrendsChart = new Chart(lagCtx, {
        type: 'line',
        data: {
            labels: [],
            datasets: []
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Lag (ms)'
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'top'
                }
            }
        }
    });

    // Replication Health Chart (Doughnut)
    const healthCtx = document.getElementById('replicationHealthChart').getContext('2d');
    replicationHealthChart = new Chart(healthCtx, {
        type: 'doughnut',
        data: {
            labels: ['Synced', 'Syncing', 'Failed'],
            datasets: [{
                data: [0, 0, 0],
                backgroundColor: ['#28a745', '#ffc107', '#dc3545'],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                }
            }
        }
    });
}

async function loadReplicationData() {
    try {
        const response = await fetch('/api/cluster/replication');
        const data = await response.json();

        if (data.success) {
            updateReplicationData(data.data);
        } else {
            // Mock data for demonstration
            const mockData = {
                consensusActive: true,
                currentTerm: 5,
                nodeStates: {
                    '0': 'leader',
                    '1': 'follower',
                    '2': 'follower'
                },
                replicationLag: {
                    '1': 15,
                    '2': 8
                }
            };
            updateReplicationData(mockData);
        }

        document.getElementById('repl-last-updated').textContent = new Date().toLocaleTimeString();

    } catch (error) {
        console.error('Error loading replication data:', error);
        // Use mock data on error
        const mockData = {
            consensusActive: true,
            currentTerm: 5,
            nodeStates: {
                '0': 'leader',
                '1': 'follower',
                '2': 'follower'
            },
            replicationLag: {
                '1': 15,
                '2': 8
            }
        };
        updateReplicationData(mockData);
        document.getElementById('repl-last-updated').textContent = new Date().toLocaleTimeString();
    }
}

function updateReplicationData(data) {
    updateReplicationOverview(data);
    updateReplicationNodesGrid(data);
    updateReplicationStatistics(data);
    updateReplicationCharts(data);
    updateReplicationPendingOperations();
    updateReplicationFailoverHistory();
}

function updateReplicationOverview(data) {
    // Consensus status
    const consensusElement = document.getElementById('repl-consensus-status');
    if (data.consensusActive) {
        consensusElement.className = 'consensus-indicator consensus-active';
        consensusElement.innerHTML = '<i class="fas fa-check-circle"></i><br>Consensus Active';
    } else {
        consensusElement.className = 'consensus-indicator consensus-inactive';
        consensusElement.innerHTML = '<i class="fas fa-times-circle"></i><br>Consensus Inactive';
    }

    // Current term
    document.getElementById('repl-current-term').textContent = `Term: ${data.currentTerm || 0}`;

    // Count active followers
    const followers = Object.values(data.nodeStates || {}).filter(state => state === 'follower').length;
    document.getElementById('repl-active-followers').textContent = followers;

    // Calculate average lag
    const lagValues = Object.values(data.replicationLag || {});
    const avgLag = lagValues.length > 0 ?
        Math.round(lagValues.reduce((a, b) => a + b, 0) / lagValues.length) : 0;
    document.getElementById('repl-replication-lag').textContent = `${avgLag}ms`;
}

function updateReplicationNodesGrid(data) {
    const grid = document.getElementById('repl-nodes-replication-grid');
    grid.innerHTML = '';

    const nodeStates = data.nodeStates || {};
    const replicationLag = data.replicationLag || {};

    Object.entries(nodeStates).forEach(([nodeId, state], index) => {
        const lag = replicationLag[nodeId] || 0;
        const lagClass = lag < 10 ? 'lag-low' : lag < 50 ? 'lag-medium' : 'lag-high';
        const syncClass = lag < 10 ? 'sync-synced' : lag < 50 ? 'sync-syncing' : 'sync-failed';

        const cardClass = state === 'leader' ? 'leader' :
                        state === 'follower' ? 'follower' : 'unreachable';

        const nodeCard = document.createElement('div');
        nodeCard.className = 'col-md-4 mb-3';

        const isLast = index === Object.keys(nodeStates).length - 1;
        const arrow = !isLast ? '<i class="fas fa-arrow-right replication-arrow"></i>' : '';

        nodeCard.innerHTML = `
            <div class="node-replication-card ${cardClass} p-3 position-relative">
                <div class="text-center">
                    <h6 class="mb-2">
                        <i class="fas fa-server"></i> Node ${nodeId}
                        ${state === 'leader' ? '<i class="fas fa-crown text-warning ms-1"></i>' : ''}
                    </h6>
                    <div class="mb-2">
                        <span class="sync-status ${syncClass}"></span>
                        <span class="text-capitalize">${state}</span>
                    </div>
                    <div class="lag-indicator ${lagClass}">
                        Lag: ${lag}ms
                    </div>
                </div>
                ${arrow}
            </div>
        `;

        grid.appendChild(nodeCard);
    });
}

function updateReplicationStatistics(data) {
    // Mock statistics for demonstration
    document.getElementById('repl-ops-replicated').textContent = (Math.floor(Math.random() * 10000) + 1000).toLocaleString();
    document.getElementById('repl-replication-rate').textContent = Math.floor(Math.random() * 100) + 50;
    document.getElementById('repl-pending-count').textContent = Math.floor(Math.random() * 20);

    const nodeStates = data.nodeStates || {};
    const syncedNodes = Object.values(nodeStates).filter(state => state !== 'unreachable').length;
    document.getElementById('repl-sync-nodes').textContent = syncedNodes;

    document.getElementById('repl-last-commit').textContent = new Date(Date.now() - Math.random() * 60000).toLocaleTimeString();
    document.getElementById('repl-commit-rate').textContent = `${Math.floor(Math.random() * 20) + 80}%`;
}

function updateReplicationCharts(data) {
    updateReplicationLagTrendsChart(data);
    updateReplicationHealthChart(data);
}

function updateReplicationLagTrendsChart(data) {
    const now = new Date();
    const timeLabels = [];

    // Generate time labels (last 10 data points)
    for (let i = 9; i >= 0; i--) {
        const time = new Date(now.getTime() - i * 30000);
        timeLabels.push(time.toLocaleTimeString());
    }

    const datasets = [];
    const nodeStates = data.nodeStates || {};
    const colors = ['#007bff', '#28a745', '#dc3545', '#ffc107', '#17a2b8'];

    Object.entries(nodeStates).forEach(([nodeId, state], index) => {
        if (state !== 'leader') {
            const lagData = [];
            for (let i = 0; i < 10; i++) {
                lagData.push(Math.random() * 50 + 5);
            }

            datasets.push({
                label: `Node ${nodeId}`,
                data: lagData,
                borderColor: colors[index % colors.length],
                backgroundColor: colors[index % colors.length] + '20',
                tension: 0.4
            });
        }
    });

    if (lagTrendsChart) {
        lagTrendsChart.data.labels = timeLabels;
        lagTrendsChart.data.datasets = datasets;
        lagTrendsChart.update('none');
    }
}

function updateReplicationHealthChart(data) {
    const nodeStates = data.nodeStates || {};
    const lagData = data.replicationLag || {};

    let synced = 0, syncing = 0, failed = 0;

    Object.entries(nodeStates).forEach(([nodeId, state]) => {
        if (state === 'unreachable') {
            failed++;
        } else {
            const lag = lagData[nodeId] || 0;
            if (lag < 10) {
                synced++;
            } else {
                syncing++;
            }
        }
    });

    if (replicationHealthChart) {
        replicationHealthChart.data.datasets[0].data = [synced, syncing, failed];
        replicationHealthChart.update('none');
    }
}

function updateReplicationPendingOperations() {
    const container = document.getElementById('repl-pending-operations');
    const operations = [];

    // Generate mock pending operations
    const operationTypes = ['PUT', 'DELETE', 'SET'];
    for (let i = 0; i < Math.floor(Math.random() * 10) + 3; i++) {
        operations.push({
            id: Math.floor(Math.random() * 10000),
            type: operationTypes[Math.floor(Math.random() * operationTypes.length)],
            key: `key_${Math.floor(Math.random() * 1000)}`,
            timestamp: new Date(Date.now() - Math.random() * 300000),
            status: Math.random() > 0.7 ? 'pending' : 'committed'
        });
    }

    container.innerHTML = operations.map(op => `
        <div class="operation-item operation-${op.status}">
            <div class="d-flex justify-content-between align-items-center">
                <div>
                    <strong>${op.type}</strong> ${op.key}
                    <br><small class="text-muted">ID: ${op.id}</small>
                </div>
                <div class="text-end">
                    <span class="badge bg-${op.status === 'pending' ? 'warning' : 'success'}">${op.status}</span>
                    <br><small class="text-muted">${op.timestamp.toLocaleTimeString()}</small>
                </div>
            </div>
        </div>
    `).join('');
}

function updateReplicationFailoverHistory() {
    const container = document.getElementById('repl-failover-timeline');
    const events = [
        {
            time: new Date(),
            event: 'Replication monitoring active',
            type: 'success',
            details: 'All nodes synchronized successfully'
        },
        {
            time: new Date(Date.now() - 1800000),
            event: 'Node 2 rejoined cluster',
            type: 'success',
            details: 'Node 2 completed catch-up replication'
        },
        {
            time: new Date(Date.now() - 3600000),
            event: 'Failover initiated',
            type: 'critical',
            details: 'Node 1 became unreachable, Node 0 elected as leader'
        },
        {
            time: new Date(Date.now() - 7200000),
            event: 'Consensus term updated',
            type: 'info',
            details: 'Election term incremented to current value'
        }
    ];

    container.innerHTML = events.map(event => `
        <div class="failover-item ${event.type}">
            <div class="fw-bold">${event.event}</div>
            <div class="text-muted mb-1">${event.details}</div>
            <small class="text-muted">${event.time.toLocaleString()}</small>
        </div>
    `).join('');
}

function refreshReplicationData() {
    loadReplicationData();
}

function showReplicationDetails() {
    alert('Detailed replication view would be shown here. This feature can be expanded to show comprehensive replication metrics.');
}

function pauseReplication() {
    if (confirm('Are you sure you want to pause replication? This may affect data consistency.')) {
        alert('Replication pause functionality would be implemented here.');
    }
}

function resumeReplication() {
    alert('Replication resume functionality would be implemented here.');
}

function triggerFailover() {
    if (confirm('Are you sure you want to trigger a manual failover? This will change the cluster leader.')) {
        alert('Manual failover functionality would be implemented here.');
    }
}

function exportReplicationReport() {
    alert('Replication report export would be implemented here. This feature can generate detailed replication reports.');
}

// Cleanup intervals on page unload
window.addEventListener('beforeunload', () => {
    if (clusterRefreshInterval) clearInterval(clusterRefreshInterval);
    if (nodeRefreshInterval) clearInterval(nodeRefreshInterval);
    if (replicationRefreshInterval) clearInterval(replicationRefreshInterval);
});

// Initialize the first tab on page load
document.addEventListener('DOMContentLoaded', function() {
    // The browse tab is already active by default
});
