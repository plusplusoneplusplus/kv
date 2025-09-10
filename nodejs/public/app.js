// KV Store Viewer - Main Application Logic

// Global state
let currentData = [];
let filteredData = [];
let dataStats = {};
let currentPage = 1;
let itemsPerPage = 50;

// Initialize the page
document.addEventListener('DOMContentLoaded', function() {
    testConnection();
    loadKeys();
    
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
});

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
    
    showLoading(true);
    hideMessages();
    
    try {
        const params = new URLSearchParams();
        if (startKey) params.append('startKey', startKey);
        if (limit) params.append('limit', limit);
        
        const response = await fetch(`/api/keys?${params}`);
        const result = await response.json();
        
        if (result.success) {
            currentData = result.keyValues;
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
            currentData = [{
                key: result.key,
                value: result.value,
                valueLength: result.valueLength,
                hexValue: result.hexValue,
                isAscii: result.isAscii,
                hasBinary: result.hasBinary
            }];
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