// KV Store Viewer - Main Application Logic

// Global state
let currentData = [];
let filteredData = [];
let dataStats = {};
let currentPage = 1;
let itemsPerPage = 50;

// Helper function to create a display-safe string from binary data
function createDisplayString(bytes) {
    let result = '';
    for (let i = 0; i < bytes.length; i++) {
        const byte = bytes[i];
        // Check if byte represents printable ASCII (32-126)
        if (byte >= 32 && byte <= 126) {
            result += String.fromCharCode(byte);
        } else if (byte === 9) {
            result += '\\t';
        } else if (byte === 10) {
            result += '\\n';
        } else if (byte === 13) {
            result += '\\r';
        } else {
            // Non-printable characters - show as hex
            result += '\\x' + byte.toString(16).padStart(2, '0').toUpperCase();
        }
    }
    return result;
}

// Process base64 encoded data from server into displayable format
function processRawData(rawKeyValues) {
    return rawKeyValues.map(kv => {
        // Handle key
        let keyString, keyBytes;
        if (kv.keyIsBuffer && typeof kv.key === 'string') {
            // Decode base64 to bytes
            keyBytes = Uint8Array.from(atob(kv.key), c => c.charCodeAt(0));
            // Use the original base64 as unique identifier, but create display string
            keyString = createDisplayString(keyBytes);
            // Store the original base64 for uniqueness
            keyString._originalBase64 = kv.key;
        } else {
            keyString = kv.key || '';
            keyBytes = new TextEncoder().encode(keyString);
        }
        
        // Handle value
        let valueString, valueBytes;
        if (kv.valueIsBuffer && typeof kv.value === 'string') {
            // Decode base64 to bytes
            valueBytes = Uint8Array.from(atob(kv.value), c => c.charCodeAt(0));
            valueString = new TextDecoder('utf-8').decode(valueBytes);
        } else {
            valueString = kv.value || '';
            valueBytes = new TextEncoder().encode(valueString);
        }
        
        // Calculate metadata
        const valueLength = valueBytes.length;
        const hexValue = Array.from(valueBytes).map(b => b.toString(16).padStart(2, '0')).join('');
        const isAscii = /^[\x20-\x7E]*$/.test(valueString);
        const hasBinary = Array.from(valueBytes).some(byte => byte < 32 && byte !== 9 && byte !== 10 && byte !== 13);
        
        return {
            key: keyString,
            originalKey: kv.keyIsBuffer ? kv.key : keyString, // Use original base64 for uniqueness
            value: valueString,
            valueLength: valueLength,
            hexValue: hexValue,
            isAscii: isAscii,
            hasBinary: hasBinary
        };
    });
}


// Connection management
async function testConnection() {
    try {
        const response = await fetch('/api/ping');
        const result = await response.json();

        if (result.success) {
            document.getElementById('statusIndicator').classList.add('connected');

            // Get current node info for display
            const currentNode = getCurrentNodeInfo();
            const nodeInfo = currentNode ? ` - ${currentNode}` : '';

            document.getElementById('statusText').textContent = `Connected (${result.roundTripTime}ms)${nodeInfo}`;
            if (document.getElementById('connectionStats')) {
                document.getElementById('connectionStats').textContent = 'Connected';
            }
            return true;
        } else {
            throw new Error('Ping failed');
        }
    } catch (error) {
        document.getElementById('statusIndicator').classList.remove('connected');
        document.getElementById('statusText').textContent = 'Connection failed';
        if (document.getElementById('connectionStats')) {
            document.getElementById('connectionStats').textContent = 'Disconnected';
        }
        console.error('Connection test failed:', error);
        return false;
    }
}

// Data loading
async function loadKeys() {
    const startKey = document.getElementById('startKey').value;
    const limit = document.getElementById('limitInput').value;
    const prefix = document.getElementById('prefixFilter').value;
    
    showLoading(true);
    hideMessages();
    
    try {
        const params = new URLSearchParams();
        if (startKey) params.append('startKey', startKey);
        if (limit) params.append('limit', limit);
        if (prefix) params.append('prefix', prefix);
        
        const response = await fetch(`/api/keys?${params}`);
        const result = await response.json();
        
        if (result.success) {
            currentData = processRawData(result.keyValues);
            analyzeData(currentData);
            applyFilter();
            document.getElementById('countStats').textContent = `${result.count} items`;
        } else {
            throw new Error(result.error || 'Failed to load keys');
        }
    } catch (error) {
        showError(`Failed to load keys: ${error.message}`);
        console.error('Load keys error:', error);
    } finally {
        showLoading(false);
    }
}

async function searchKey() {
    const key = document.getElementById('searchKey').value;
    if (!key) {
        showError('Please enter a key to search for');
        return;
    }
    
    showLoading(true);
    hideMessages();
    
    try {
        const response = await fetch(`/api/key/${encodeURIComponent(key)}`);
        const result = await response.json();
        
        if (result.found) {
            currentData = processRawData([{
                key: result.key,
                value: result.value,
                keyIsBuffer: false, // Individual key search uses string key
                valueIsBuffer: result.valueIsBuffer || false
            }]);
            analyzeData(currentData);
            applyFilter();
            document.getElementById('countStats').textContent = '1 item (search result)';
        } else {
            showError(`Key '${key}' not found`);
            currentData = [];
            analyzeData(currentData);
            applyFilter();
            document.getElementById('countStats').textContent = '0 items';
        }
    } catch (error) {
        showError(`Failed to search key: ${error.message}`);
        console.error('Search key error:', error);
    } finally {
        showLoading(false);
    }
}

// Key operations
async function deleteKey(key) {
    if (!confirm(`Are you sure you want to delete key '${key}'?`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/key/${encodeURIComponent(key)}`, {
            method: 'DELETE'
        });
        const result = await response.json();
        
        if (result.success) {
            showSuccess(`Key '${key}' deleted successfully`);
            loadKeys(); // Refresh the list
        } else {
            throw new Error(result.error || 'Delete failed');
        }
    } catch (error) {
        showError(`Failed to delete key: ${error.message}`);
        console.error('Delete key error:', error);
    }
}

async function saveKeyValue() {
    const key = document.getElementById('modalKey').value;
    const value = document.getElementById('modalValue').value;
    
    if (!key || !value) {
        alert('Both key and value are required');
        return;
    }
    
    try {
        const response = await fetch('/api/key', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ key, value })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showSuccess(`Key '${key}' saved successfully`);
            closeModal();
            loadKeys(); // Refresh the list
        } else {
            throw new Error(result.error || 'Save failed');
        }
    } catch (error) {
        showError(`Failed to save key: ${error.message}`);
        console.error('Save key error:', error);
    }
}

// UI state management
function showLoading(show) {
    document.getElementById('loadingIndicator').style.display = show ? 'block' : 'none';
}

function showError(message) {
    const errorDiv = document.getElementById('errorDisplay');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}

function showSuccess(message) {
    const successDiv = document.getElementById('successDisplay');
    successDiv.textContent = message;
    successDiv.style.display = 'block';
    setTimeout(() => {
        successDiv.style.display = 'none';
    }, 5000);
}

function hideMessages() {
    document.getElementById('errorDisplay').style.display = 'none';
    document.getElementById('successDisplay').style.display = 'none';
}

// Tab management
function showTab(tabName, updateHistory = true) {
    // Update URL path if requested
    if (updateHistory) {
        const newPath = tabName === 'browse' ? '/' : `/${tabName}`;
        window.history.pushState({ tab: tabName }, '', newPath);
    }

    // Hide all tab contents
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });

    // Remove active class from all tab buttons
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.remove('active');
    });

    // Show selected tab
    document.getElementById(tabName + 'Tab').classList.add('active');

    // Add active class to correct button
    const targetButton = document.querySelector(`[data-tab="${tabName}"]`);
    if (targetButton) {
        targetButton.classList.add('active');
    }

    // Initialize tab-specific functionality
    if (tabName === 'cluster') {
        // Initialize cluster dashboard when cluster tab is shown
        if (typeof initializeClusterDashboard === 'function') {
            initializeClusterDashboard();
        }
    } else if (tabName === 'logs') {
        // Initialize log viewer when logs tab is shown
        console.log('Initializing logs tab...');
        if (typeof initializeLogViewer === 'function') {
            console.log('Calling initializeLogViewer');
            initializeLogViewer();
        } else {
            console.error('initializeLogViewer function not found');
        }
    }
}

// Settings management functions
function updateEndpoint() {
    const hostInput = document.getElementById('thriftHost');
    const portInput = document.getElementById('thriftPort');
    const statusDiv = document.getElementById('endpointStatus');

    const host = hostInput.value.trim() || 'localhost';
    const port = parseInt(portInput.value) || 9090;

    // Validate port range
    if (port < 1 || port > 65535) {
        showEndpointStatus('Port must be between 1 and 65535', 'error');
        return;
    }

    // Validate host (basic check)
    if (!host || host.includes(' ')) {
        showEndpointStatus('Please enter a valid hostname or IP address', 'error');
        return;
    }

    // Send update request to server
    fetch('/api/admin/update-endpoint', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            host: host,
            port: port
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            document.getElementById('currentEndpoint').textContent = `${host}:${port}`;
            updateCurrentNodeDisplay(host, port);
            updateNodeSelection();
            showEndpointStatus(`Endpoint updated to ${host}:${port}`, 'success');
            // Refresh connection status
            setTimeout(testConnection, 1000);
        } else {
            showEndpointStatus(data.error || 'Failed to update endpoint', 'error');
        }
    })
    .catch(error => {
        showEndpointStatus('Failed to update endpoint: ' + error.message, 'error');
    });
}

function testEndpointConnection() {
    const statusDiv = document.getElementById('endpointStatus');
    showEndpointStatus('Testing connection...', 'info');

    fetch('/api/ping')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                showEndpointStatus(`Connection successful! Round-trip: ${data.roundTripTime}ms`, 'success');
            } else {
                showEndpointStatus('Connection failed: ' + (data.error || 'Unknown error'), 'error');
            }
        })
        .catch(error => {
            showEndpointStatus('Connection test failed: ' + error.message, 'error');
        });
}

function resetToDefaults() {
    document.getElementById('thriftHost').value = 'localhost';
    document.getElementById('thriftPort').value = '9090';
    showEndpointStatus('Values reset to defaults. Click "Update Endpoint" to apply.', 'info');
}

function showEndpointStatus(message, type) {
    const statusDiv = document.getElementById('endpointStatus');
    statusDiv.textContent = message;
    statusDiv.className = `status-message ${type}`;
    statusDiv.style.display = 'block';

    if (type === 'success' || type === 'info') {
        setTimeout(() => {
            statusDiv.style.display = 'none';
        }, 5000);
    }
}

// Global variable to store nodes configuration
let availableNodes = [];

// Helper function to get current node info for display
function getCurrentNodeInfo() {
    const hostInput = document.getElementById('thriftHost');
    const portInput = document.getElementById('thriftPort');

    if (!hostInput || !portInput) return null;

    const currentHost = hostInput.value;
    const currentPort = parseInt(portInput.value);

    // Find matching node
    const matchingNode = availableNodes.find(node =>
        node.host === currentHost && node.port === currentPort
    );

    if (matchingNode) {
        return matchingNode.name;
    } else {
        return `${currentHost}:${currentPort}`;
    }
}

// UI State Management for Node Switching
function showNodeSwitchingOverlay(targetHost, targetPort) {
    const overlay = document.getElementById('nodeSwitchingOverlay');
    const details = document.getElementById('switchingDetails');

    if (overlay && details) {
        details.textContent = `Connecting to ${targetHost}:${targetPort}...`;
        overlay.style.display = 'flex';

        // Freeze main UI elements
        freezeUI();
    }
}

function hideNodeSwitchingOverlay() {
    const overlay = document.getElementById('nodeSwitchingOverlay');

    if (overlay) {
        overlay.style.display = 'none';

        // Unfreeze main UI elements
        unfreezeUI();

        // Update tab highlighting to reflect current node
        updateHeaderNodeSelection();

        // Refresh key-value data from the new node
        loadKeys();

        // Reset switching flag
        isSwitchingNode = false;
    }
}

function freezeUI() {
    // Freeze tabs
    const nodeTabs = document.getElementById('nodeTabs');
    if (nodeTabs) {
        nodeTabs.classList.add('switching');
    }

    // Freeze other main UI elements
    const tabNav = document.querySelector('.tab-nav');
    const tabContainers = document.querySelectorAll('[id$="-tab-container"]');

    if (tabNav) tabNav.classList.add('ui-frozen');
    tabContainers.forEach(container => container.classList.add('ui-frozen'));
}

function unfreezeUI() {
    // Unfreeze tabs
    const nodeTabs = document.getElementById('nodeTabs');
    if (nodeTabs) {
        nodeTabs.classList.remove('switching');
    }

    // Remove switching-from class from all tabs
    const allTabs = document.querySelectorAll('.node-tab');
    allTabs.forEach(tab => tab.classList.remove('switching-from'));

    // Unfreeze other UI elements
    const tabNav = document.querySelector('.tab-nav');
    const tabContainers = document.querySelectorAll('[id$="-tab-container"]');

    if (tabNav) tabNav.classList.remove('ui-frozen');
    tabContainers.forEach(container => container.classList.remove('ui-frozen'));
}

// Initialize settings when page loads
function initializeSettings() {
    // Load available nodes
    loadAvailableNodes();

    // Get current endpoint from server
    fetch('/api/admin/current-endpoint')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById('thriftHost').value = data.host;
                document.getElementById('thriftPort').value = data.port;
                document.getElementById('currentEndpoint').textContent = `${data.host}:${data.port}`;
                updateCurrentNodeDisplay(data.host, data.port);
            }
        })
        .catch(error => {
            console.log('Could not fetch current endpoint, using defaults');
        });
}

// Load available nodes from server
function loadAvailableNodes() {
    fetch('/api/admin/nodes')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                availableNodes = data.nodes;
                populateNodeSelector(data.nodes, data.defaultNode);
                populateHeaderNodeSelector(data.nodes, data.defaultNode);
                updateCurrentNodeDisplay(data.currentNode.host, data.currentNode.port);
            }
        })
        .catch(error => {
            console.error('Could not fetch available nodes:', error);
            populateNodeSelector([], null);
            populateHeaderNodeSelector([], null);
        });
}

// Populate node selector dropdown
function populateNodeSelector(nodes, defaultNode) {
    const selector = document.getElementById('nodeSelector');
    if (!selector) return;

    selector.innerHTML = '';

    // Add nodes from configuration
    nodes.forEach(node => {
        const option = document.createElement('option');
        option.value = node.id;
        option.textContent = node.name;
        option.dataset.host = node.host;
        option.dataset.port = node.port;
        option.dataset.description = node.description;
        selector.appendChild(option);
    });

    // Add custom option
    const customOption = document.createElement('option');
    customOption.value = 'custom';
    customOption.textContent = 'Custom Configuration';
    selector.appendChild(customOption);

    // Set current selection
    updateNodeSelection();
}

// Populate header node tabs
function populateHeaderNodeSelector(nodes, defaultNode) {
    const tabsContainer = document.getElementById('nodeTabs');
    if (!tabsContainer) return;

    tabsContainer.innerHTML = '';

    // Add nodes from configuration
    nodes.forEach(node => {
        const tab = document.createElement('div');
        tab.className = 'node-tab';
        tab.dataset.nodeId = node.id;
        tab.dataset.host = node.host;
        tab.dataset.port = node.port;
        tab.onclick = () => onNodeTabClick(node.id);

        // Determine label (Primary/Secondary)
        const isFirstNode = node.id === 'node-0';
        const label = isFirstNode ? 'Primary' : 'Secondary';

        tab.innerHTML = `
            <span class="tab-label">${label}</span>
            <span class="tab-port">${node.port}</span>
        `;

        tabsContainer.appendChild(tab);
    });

    // Add custom tab
    const customTab = document.createElement('div');
    customTab.className = 'node-tab custom';
    customTab.dataset.nodeId = 'custom';
    customTab.onclick = () => onNodeTabClick('custom');
    customTab.innerHTML = `
        <span class="tab-label">Custom</span>
        <span class="tab-port">⚙️</span>
    `;
    tabsContainer.appendChild(customTab);

    // Set current selection
    updateHeaderNodeSelection();
}

// Update current node display
function updateCurrentNodeDisplay(host, port) {
    const currentEndpoint = document.getElementById('currentEndpoint');
    const currentNode = document.getElementById('currentNode');

    if (currentEndpoint) {
        currentEndpoint.textContent = `${host}:${port}`;
    }

    if (currentNode) {
        // Find matching node
        const matchingNode = availableNodes.find(node =>
            node.host === host && node.port == port
        );

        if (matchingNode) {
            currentNode.textContent = matchingNode.name;
        } else {
            currentNode.textContent = `Custom (${host}:${port})`;
        }
    }
}

// Update node selector based on current endpoint
function updateNodeSelection() {
    const selector = document.getElementById('nodeSelector');
    const hostInput = document.getElementById('thriftHost');
    const portInput = document.getElementById('thriftPort');

    if (!selector || !hostInput || !portInput) return;

    const currentHost = hostInput.value;
    const currentPort = parseInt(portInput.value);

    // Find matching node
    const matchingNode = availableNodes.find(node =>
        node.host === currentHost && node.port === currentPort
    );

    if (matchingNode) {
        selector.value = matchingNode.id;
    } else {
        selector.value = 'custom';
    }
}

// Handle node selection change
function onNodeSelectionChange() {
    const selector = document.getElementById('nodeSelector');
    const hostInput = document.getElementById('thriftHost');
    const portInput = document.getElementById('thriftPort');

    if (!selector || !hostInput || !portInput) return;

    const selectedValue = selector.value;

    if (selectedValue === 'custom') {
        // User selected custom, don't change the inputs
        return;
    }

    // Find the selected node
    const selectedNode = availableNodes.find(node => node.id === selectedValue);
    if (selectedNode) {
        hostInput.value = selectedNode.host;
        portInput.value = selectedNode.port;

        // Show description in status
        if (selectedNode.description) {
            showEndpointStatus(selectedNode.description, 'info');
        }
    }
}

// Update header node selection based on current endpoint
function updateHeaderNodeSelection() {
    const tabsContainer = document.getElementById('nodeTabs');
    const hostInput = document.getElementById('thriftHost');
    const portInput = document.getElementById('thriftPort');

    if (!tabsContainer) return;

    // Try to get current host/port from inputs, or use current endpoint
    let currentHost, currentPort;
    if (hostInput && portInput) {
        currentHost = hostInput.value;
        currentPort = parseInt(portInput.value);
    } else {
        // Extract from current endpoint display if inputs not available
        const currentEndpoint = document.getElementById('currentEndpoint');
        if (currentEndpoint) {
            const parts = currentEndpoint.textContent.split(':');
            currentHost = parts[0];
            currentPort = parseInt(parts[1]);
        }
    }

    if (!currentHost || !currentPort) return;

    // Remove active class from all tabs
    const allTabs = tabsContainer.querySelectorAll('.node-tab');
    allTabs.forEach(tab => tab.classList.remove('active'));

    // Find matching node and set active
    const matchingNode = availableNodes.find(node =>
        node.host === currentHost && node.port === currentPort
    );

    if (matchingNode) {
        const matchingTab = tabsContainer.querySelector(`[data-node-id="${matchingNode.id}"]`);
        if (matchingTab) {
            matchingTab.classList.add('active');
        }
    } else {
        // Set custom tab as active
        const customTab = tabsContainer.querySelector('[data-node-id="custom"]');
        if (customTab) {
            customTab.classList.add('active');
        }
    }
}

// Global flag to prevent double-clicking during switching
let isSwitchingNode = false;

// Handle node tab click
function onNodeTabClick(nodeId) {
    // Prevent double-clicks and clicks during switching
    if (isSwitchingNode) {
        console.log('Node switching in progress, ignoring click');
        return;
    }

    if (nodeId === 'custom') {
        // For custom tab, just highlight it and let user use settings
        updateHeaderNodeSelection();
        return;
    }

    // Find the selected node
    const selectedNode = availableNodes.find(node => node.id === nodeId);
    if (selectedNode) {
        // Check if we're already on this node
        const currentHost = document.getElementById('currentEndpoint')?.textContent?.split(':')[0];
        const currentPort = parseInt(document.getElementById('currentEndpoint')?.textContent?.split(':')[1]);

        if (selectedNode.host === currentHost && selectedNode.port === currentPort) {
            console.log('Already on this node, ignoring click');
            return;
        }

        // Set switching flag
        isSwitchingNode = true;

        // Mark the current tab as switching-from
        const currentActiveTab = document.querySelector('.node-tab.active');
        if (currentActiveTab) {
            currentActiveTab.classList.add('switching-from');
        }

        // Show loading overlay and update endpoint
        showNodeSwitchingOverlay(selectedNode.host, selectedNode.port);
        updateEndpointDirectWithLoading(selectedNode.host, selectedNode.port);
    }
}

// Direct endpoint update function (legacy - for non-loading contexts)
function updateEndpointDirect(host, port) {
    updateEndpointDirectWithLoading(host, port);
}

// Direct endpoint update function with loading states
function updateEndpointDirectWithLoading(host, port) {
    // Send update request to server
    fetch('/api/admin/update-endpoint', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            host: host,
            port: port
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Update all displays
            updateCurrentNodeDisplay(host, port);
            updateHeaderNodeSelection();

            // Update settings inputs if they exist
            const hostInput = document.getElementById('thriftHost');
            const portInput = document.getElementById('thriftPort');
            if (hostInput && portInput) {
                hostInput.value = host;
                portInput.value = port;
                updateNodeSelection();
            }

            // Test connection and hide overlay after response
            setTimeout(() => {
                testConnection().finally(() => {
                    // Hide loading overlay after connection test completes
                    setTimeout(hideNodeSwitchingOverlay, 300);
                });
            }, 200);
        } else {
            console.error('Failed to update endpoint:', data.error);
            // Hide overlay on error and reset flag
            setTimeout(() => {
                hideNodeSwitchingOverlay();
                isSwitchingNode = false;
            }, 500);
        }
    })
    .catch(error => {
        console.error('Failed to update endpoint:', error);
        // Hide overlay on error and reset flag
        setTimeout(() => {
            hideNodeSwitchingOverlay();
            isSwitchingNode = false;
        }, 500);
    });
}

// Close modal when clicking outside
window.onclick = function(event) {
    const addModal = document.getElementById('addModal');
    const viewModal = document.getElementById('viewModal');
    const clearAllModal = document.getElementById('clearAllModal');

    if (event.target === addModal) {
        closeModal();
    } else if (event.target === viewModal) {
        closeViewModal();
    } else if (event.target === clearAllModal) {
        closeClearAllModal();
    }
}

// Initialize tab navigation from URL
function initializeTabFromURL() {
    const path = window.location.pathname;
    let tabName = 'browse'; // default

    if (path === '/clear') tabName = 'clear';
    else if (path === '/settings') tabName = 'settings';
    else if (path === '/cluster') tabName = 'cluster';

    showTab(tabName, false); // don't update history on init
}

// Setup tab navigation event listeners
function setupTabNavigation() {
    document.querySelectorAll('.tab-button').forEach(button => {
        button.addEventListener('click', (e) => {
            e.preventDefault();
            const tabName = button.getAttribute('data-tab');
            showTab(tabName);
        });
    });
}

// Handle browser back/forward buttons
window.addEventListener('popstate', (event) => {
    if (event.state && event.state.tab) {
        showTab(event.state.tab, false);
    } else {
        initializeTabFromURL();
    }
});

// Initialize tab based on current URL
function initializeTabFromURL() {
    const path = window.location.pathname;
    let tabName = 'browse'; // default tab

    // Extract tab name from URL path
    if (path === '/clear') {
        tabName = 'clear';
    } else if (path === '/settings') {
        tabName = 'settings';
    } else if (path === '/cluster') {
        tabName = 'cluster';
    } else if (path === '/logs') {
        tabName = 'logs';
    } else if (path === '/dashboard') {
        tabName = 'browse'; // dashboard redirects to browse
    }

    // Show the appropriate tab without updating history
    showTab(tabName, false);
}

// Initialize the application
function initializeApp() {
    setupTabNavigation();
    initializeTabFromURL();

    // Load available nodes for header selector (needs to be done early)
    loadAvailableNodes();

    testConnection();
    loadKeys();
    initializeSettings();

    // Setup confirmation input listener
    const confirmationInput = document.getElementById('clearAllConfirmation');
    if (confirmationInput) {
        confirmationInput.addEventListener('input', function() {
            const executeButton = document.getElementById('clearAllExecute');
            const statusText = document.getElementById('clearAllStatus');

            if (this.value === 'DELETE ALL DATA') {
                executeButton.disabled = false;
                statusText.textContent = 'Ready to execute';
                statusText.style.color = '#e53e3e';
            } else {
                executeButton.disabled = true;
                statusText.textContent = this.value ? 'Incorrect confirmation phrase' : '';
                statusText.style.color = '#666';
            }
        });
    }
}

// This initialization will be called by component-loader.js after components are loaded