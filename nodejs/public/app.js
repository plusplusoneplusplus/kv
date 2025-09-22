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

// Initialize the page after components are loaded
function initializeApp() {
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

// Connection management
async function testConnection() {
    try {
        const response = await fetch('/api/ping');
        const result = await response.json();
        
        if (result.success) {
            document.getElementById('statusIndicator').classList.add('connected');
            document.getElementById('statusText').textContent = `Connected (${result.roundTripTime}ms)`;
            document.getElementById('connectionStats').textContent = 'Connected';
        } else {
            throw new Error('Ping failed');
        }
    } catch (error) {
        document.getElementById('statusIndicator').classList.remove('connected');
        document.getElementById('statusText').textContent = 'Connection failed';
        document.getElementById('connectionStats').textContent = 'Disconnected';
        console.error('Connection test failed:', error);
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
function showTab(tabName) {
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
    
    // Add active class to clicked button
    event.target.classList.add('active');
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

// Initialize settings when page loads
function initializeSettings() {
    // Get current endpoint from server
    fetch('/api/admin/current-endpoint')
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById('thriftHost').value = data.host;
                document.getElementById('thriftPort').value = data.port;
                document.getElementById('currentEndpoint').textContent = `${data.host}:${data.port}`;
            }
        })
        .catch(error => {
            console.log('Could not fetch current endpoint, using defaults');
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

// This initialization will be called by component-loader.js after components are loaded