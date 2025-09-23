// Cluster Dashboard - Enhanced UI
let dashboardSocket;
let clusterRefreshInterval;

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

        const lastUpdatedElement = document.getElementById('cluster-last-updated');
        if (lastUpdatedElement) {
            lastUpdatedElement.textContent = new Date().toLocaleTimeString();
        }
    } catch (error) {
        console.error('Error refreshing cluster dashboard:', error);
        showClusterAlert('Failed to refresh dashboard data', 'danger');
    }
}

function updateClusterDashboard(data) {
    // Update overview metrics with null checks
    const totalNodesElement = document.getElementById('cluster-total-nodes');
    if (totalNodesElement) {
        totalNodesElement.textContent = data.total_nodes_count || 0;
    }

    const healthyNodesElement = document.getElementById('cluster-healthy-nodes');
    if (healthyNodesElement) {
        healthyNodesElement.textContent = data.healthy_nodes_count || 0;
    }

    const leaderElement = document.getElementById('cluster-current-leader');
    if (leaderElement) {
        leaderElement.textContent = data.current_leader_id !== null ? `Node ${data.current_leader_id}` : 'None';
    }

    const termElement = document.getElementById('cluster-current-term');
    if (termElement) {
        termElement.textContent = data.current_term || 0;
    }

    // Update cluster health with enhanced status
    const healthElement = document.getElementById('cluster-health');
    if (healthElement) {
        let healthText = 'Unknown';
        let healthClass = 'text-secondary';

        if (data.healthy_nodes_count === data.total_nodes_count && data.total_nodes_count > 0) {
            healthText = 'Healthy';
            healthClass = 'text-success';
        } else if (data.healthy_nodes_count > data.total_nodes_count / 2) {
            healthText = 'Degraded';
            healthClass = 'text-warning';
        } else {
            healthText = 'Critical';
            healthClass = 'text-danger';
        }

        healthElement.innerHTML = `<span class="pulse-dot ${healthClass}"></span> ${healthText}`;
        healthElement.className = `fw-bold ${healthClass}`;
    }

    // Update nodes grid with enhanced UI
    if (data.nodes) {
        updateClusterNodesGrid(data.nodes);
    }
}

function updateClusterNodesGrid(nodes) {
    const grid = document.getElementById('cluster-nodes-grid');
    if (!grid) return;

    grid.innerHTML = nodes.map(node => {
        const statusInfo = getNodeStatusInfo(node.status, node.is_leader);

        return `
            <div class="col-lg-4 col-md-6 mb-3">
                <div class="card h-100 ${statusInfo.cardClass} shadow-sm">
                    <div class="card-body text-center position-relative">
                        ${node.is_leader ? '<div class="leader-indicator"><i class="fas fa-crown"></i> LEADER</div>' : ''}

                        <div class="d-flex justify-content-center align-items-center mb-3">
                            <div class="node-icon-wrapper">
                                <i class="fas fa-server fa-2x ${statusInfo.iconColor}"></i>
                                <div class="status-dot ${statusInfo.statusClass} ${statusInfo.animation}"></div>
                            </div>
                        </div>

                        <h5 class="card-title mb-1">Node ${node.node_id}</h5>
                        <p class="node-endpoint text-muted small mb-2">${node.endpoint || 'localhost:909' + node.node_id}</p>

                        <div class="status-badge ${statusInfo.badgeClass} mb-2">
                            <i class="fas ${statusInfo.statusIcon}"></i>
                            ${statusInfo.statusText}
                        </div>

                        ${node.last_heartbeat ? `
                            <div class="last-seen text-muted small">
                                <i class="fas fa-clock"></i>
                                Last seen: ${formatLastSeen(node.last_heartbeat)}
                            </div>
                        ` : ''}

                        ${node.status === 'unreachable' ?
                            '<div class="alert-pulse"><i class="fas fa-exclamation-triangle"></i></div>' : ''
                        }
                    </div>
                </div>
            </div>
        `;
    }).join('');
}

function getNodeStatusInfo(status, isLeader) {
    const baseInfo = {
        healthy: {
            statusText: 'Healthy',
            statusClass: 'status-dot-healthy',
            cardClass: 'border-success',
            iconColor: 'text-success',
            badgeClass: 'badge bg-success',
            statusIcon: 'fa-check-circle',
            animation: 'pulse-healthy'
        },
        degraded: {
            statusText: 'Degraded',
            statusClass: 'status-dot-warning',
            cardClass: 'border-warning',
            iconColor: 'text-warning',
            badgeClass: 'badge bg-warning text-dark',
            statusIcon: 'fa-exclamation-triangle',
            animation: 'pulse-warning'
        },
        unreachable: {
            statusText: 'Unreachable',
            statusClass: 'status-dot-danger',
            cardClass: 'border-danger',
            iconColor: 'text-danger',
            badgeClass: 'badge bg-danger',
            statusIcon: 'fa-times-circle',
            animation: 'pulse-danger'
        }
    };

    const info = baseInfo[status] || baseInfo.unreachable;

    if (isLeader) {
        info.cardClass += ' leader-glow';
    }

    return info;
}

function updateClusterDatabaseStats(stats) {
    const elements = [
        { id: 'cluster-total-keys', value: stats.total_keys?.toLocaleString() || '0' },
        { id: 'cluster-db-size', value: formatBytes(stats.total_size_bytes || 0) },
        { id: 'cluster-active-transactions', value: stats.active_transactions || 0 },
        { id: 'cluster-cache-hit-rate', value: stats.cache_hit_rate_percent ? `${stats.cache_hit_rate_percent}%` : 'N/A' },
        { id: 'cluster-avg-response-time', value: stats.average_response_time_ms ? `${stats.average_response_time_ms} ms` : 'N/A' },
        { id: 'cluster-compaction-pending', value: formatBytes(stats.compaction_pending_bytes || 0) }
    ];

    elements.forEach(({id, value}) => {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
        }
    });
}

function formatLastSeen(timestamp) {
    if (!timestamp) return 'Unknown';
    const now = new Date();
    const lastSeen = new Date(timestamp);
    const diffMs = now - lastSeen;
    const diffSecs = Math.floor(diffMs / 1000);

    if (diffSecs < 60) return `${diffSecs}s ago`;
    if (diffSecs < 3600) return `${Math.floor(diffSecs / 60)}m ago`;
    return `${Math.floor(diffSecs / 3600)}h ago`;
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function showClusterAlert(message, type = 'info') {
    // Create alert element if it doesn't exist
    let alertContainer = document.getElementById('cluster-alerts');
    if (!alertContainer) {
        alertContainer = document.createElement('div');
        alertContainer.id = 'cluster-alerts';
        alertContainer.className = 'position-fixed top-0 end-0 p-3';
        alertContainer.style.zIndex = '1050';
        document.body.appendChild(alertContainer);
    }

    const alertId = 'alert-' + Date.now();
    const alertDiv = document.createElement('div');
    alertDiv.id = alertId;
    alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
    alertDiv.innerHTML = `
        ${message}
        <button type="button" class="btn-close" onclick="dismissAlert('${alertId}')"></button>
    `;

    alertContainer.appendChild(alertDiv);

    // Auto-dismiss after 5 seconds
    setTimeout(() => dismissAlert(alertId), 5000);
}

function dismissAlert(alertId) {
    const alertElement = document.getElementById(alertId);
    if (alertElement) {
        alertElement.remove();
    }
}