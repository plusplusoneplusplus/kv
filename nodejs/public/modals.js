// KV Store Viewer - Modal Management Module

// Add/Edit Modal Functions
function openAddModal() {
    document.getElementById('modalTitle').textContent = 'Add Key-Value Pair';
    document.getElementById('modalKey').value = '';
    document.getElementById('modalValue').value = '';
    document.getElementById('modalKey').readOnly = false;
    document.getElementById('addModal').style.display = 'block';
}

function editKey(key, value) {
    document.getElementById('modalTitle').textContent = 'Edit Key-Value Pair';
    document.getElementById('modalKey').value = key;
    document.getElementById('modalValue').value = value;
    document.getElementById('modalKey').readOnly = true;
    document.getElementById('addModal').style.display = 'block';
}

// Base64 wrapper functions for binary data
function editKeyB64(keyB64, valueB64) {
    const key = decodeURIComponent(escape(atob(keyB64)));
    const value = decodeURIComponent(escape(atob(valueB64)));
    editKey(key, value);
}

function closeModal() {
    document.getElementById('addModal').style.display = 'none';
}

// Value Viewer Modal Functions
function viewValue(key, value) {
    alert(`Key: ${key}\n\nValue:\n${value}`);
}

function viewValueAdvanced(key, value, hexValue, hasBinary) {
    document.getElementById('viewModalTitle').innerHTML = `View Value: ${formatKeyDisplay(key)}`;
    
    // Text view
    document.getElementById('textContent').textContent = value;
    
    // Hex view
    document.getElementById('hexContent').textContent = formatHexDump(hexValue);
    
    // Raw bytes view
    document.getElementById('rawContent').textContent = formatRawBytes(hexValue);
    
    // Show appropriate default tab
    if (hasBinary) {
        switchViewMode('hex');
    } else {
        switchViewMode('text');
    }
    
    document.getElementById('viewModal').style.display = 'block';
}

function viewValueAdvancedB64(keyB64, valueB64, hexB64, hasBinary) {
    const key = decodeURIComponent(escape(atob(keyB64)));
    const value = decodeURIComponent(escape(atob(valueB64)));
    const hexValue = atob(hexB64);
    viewValueAdvanced(key, value, hexValue, hasBinary);
}

function switchViewMode(mode) {
    // Update tabs
    document.querySelectorAll('.view-mode-tab').forEach(tab => {
        tab.classList.remove('active');
    });
    event.target.classList.add('active');
    
    // Update content
    document.querySelectorAll('.view-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(mode + 'View').classList.add('active');
}

function closeViewModal() {
    document.getElementById('viewModal').style.display = 'none';
}

// Clear All Modal Functions
function openClearAllModal() {
    document.getElementById('clearAllConfirmation').value = '';
    document.getElementById('clearAllStatus').textContent = '';
    document.getElementById('clearAllExecute').disabled = true;
    document.getElementById('clearAllModal').style.display = 'block';
}

function closeClearAllModal() {
    document.getElementById('clearAllModal').style.display = 'none';
}

async function executeClearAll() {
    const confirmationInput = document.getElementById('clearAllConfirmation');
    const statusText = document.getElementById('clearAllStatus');
    const executeButton = document.getElementById('clearAllExecute');
    
    if (confirmationInput.value !== 'DELETE ALL DATA') {
        statusText.textContent = 'Invalid confirmation phrase';
        statusText.style.color = '#e53e3e';
        return;
    }
    
    // Final confirmation dialog
    if (!confirm('FINAL WARNING: This will permanently delete ALL data. Are you absolutely sure?')) {
        return;
    }
    
    executeButton.disabled = true;
    executeButton.textContent = 'Deleting...';
    statusText.textContent = 'Executing deletion...';
    statusText.style.color = '#e53e3e';
    
    try {
        const response = await fetch('/api/admin/clear-all', {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ confirmation: confirmationInput.value })
        });
        
        const result = await response.json();
        
        if (result.success) {
            statusText.textContent = `Success: ${result.message}`;
            statusText.style.color = '#27ae60';
            showSuccess(result.message);
            
            // Close modal after 2 seconds and refresh data
            setTimeout(() => {
                closeClearAllModal();
                loadKeys();
            }, 2000);
        } else {
            throw new Error(result.error || 'Clear all failed');
        }
    } catch (error) {
        statusText.textContent = `Error: ${error.message}`;
        statusText.style.color = '#e53e3e';
        showError(`Failed to clear all data: ${error.message}`);
        console.error('Clear all error:', error);
    } finally {
        executeButton.disabled = false;
        executeButton.textContent = 'Clear All Data';
    }
}

// Hex formatting functions
function formatHexDump(hexString) {
    if (!hexString) return '';
    
    let result = '';
    let ascii = '';
    
    for (let i = 0; i < hexString.length; i += 2) {
        if (i > 0 && (i / 2) % 16 === 0) {
            result += '  |' + ascii + '|\n';
            ascii = '';
        }
        
        if ((i / 2) % 16 === 0) {
            result += (i / 2).toString(16).padStart(8, '0') + '  ';
        }
        
        const byte = hexString.substr(i, 2);
        result += byte + ' ';
        
        const byteValue = parseInt(byte, 16);
        ascii += (byteValue >= 32 && byteValue <= 126) ? String.fromCharCode(byteValue) : '.';
        
        if ((i / 2) % 8 === 7) {
            result += ' ';
        }
    }
    
    // Pad the last line
    const lastLineBytes = (hexString.length / 2) % 16;
    if (lastLineBytes > 0) {
        const padding = ' '.repeat((16 - lastLineBytes) * 3 + (lastLineBytes <= 8 ? 1 : 0));
        result += padding + '  |' + ascii.padEnd(16, ' ') + '|';
    }
    
    return result;
}

function formatRawBytes(hexString) {
    if (!hexString) return '';
    
    let result = '';
    for (let i = 0; i < hexString.length; i += 2) {
        const byte = parseInt(hexString.substr(i, 2), 16);
        result += byte.toString().padStart(3, ' ') + ' ';
        
        if ((i / 2) % 16 === 15) {
            result += '\n';
        }
    }
    return result;
}