// KV Store Viewer - Tab Management Module

let currentTab = 'browse';

function switchTab(tabName) {
    // Update tab navigation
    document.querySelectorAll('.tab-nav-item').forEach(tab => {
        tab.classList.remove('active');
    });
    document.getElementById(tabName + 'Tab').classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    document.getElementById(tabName + 'Content').classList.add('active');
    
    currentTab = tabName;
    
    // Perform tab-specific actions
    switch(tabName) {
        case 'browse':
            // Refresh data if needed
            if (currentData.length === 0) {
                loadKeys();
            }
            break;
        case 'analysis':
            // Update analysis data
            updateAnalysisTab();
            break;
        case 'search':
            // Focus on search input
            setTimeout(() => {
                const searchInput = document.getElementById('searchKeyAdvanced');
                if (searchInput) searchInput.focus();
            }, 100);
            break;
        case 'admin':
            // Update admin info
            updateAdminTab();
            break;
    }
}

function updateAnalysisTab() {
    // The analysis is already updated when data is loaded
    // But we can refresh the analysis badge
    updateAnalysisBadge();
}

function updateAnalysisBadge() {
    const badge = document.getElementById('analysisBadge');
    const suspiciousCount = dataStats.suspiciousDuplicates ? dataStats.suspiciousDuplicates.length : 0;
    
    if (suspiciousCount > 0) {
        badge.textContent = suspiciousCount;
        badge.style.display = 'inline';
    } else {
        badge.style.display = 'none';
    }
}

function updateAdminTab() {
    // Update admin statistics
    document.getElementById('adminTotalKeys').textContent = dataStats.totalKeys || 0;
    document.getElementById('adminDbSize').textContent = (dataStats.totalDataSize || 0) + ' KB';
    
    const connectionStatus = document.getElementById('statusIndicator').classList.contains('connected');
    document.getElementById('adminConnection').textContent = connectionStatus ? 'Connected' : 'Disconnected';
    document.getElementById('adminConnection').style.color = connectionStatus ? '#27ae60' : '#e74c3c';
}

// Advanced search functionality
async function searchKeyAdvanced() {
    const searchTerm = document.getElementById('searchKeyAdvanced').value;
    const searchType = document.getElementById('searchType').value;
    const resultsDiv = document.getElementById('searchResults');
    
    if (!searchTerm) {
        resultsDiv.innerHTML = '<p style="color: #e74c3c; text-align: center; padding: 20px;">Please enter a search term</p>';
        return;
    }
    
    resultsDiv.innerHTML = '<p style="color: #7f8c8d; text-align: center; padding: 20px;">Searching...</p>';
    
    try {
        let matches = [];
        
        // If we don't have current data, load it first
        if (currentData.length === 0) {
            await loadKeys();
        }
        
        // Perform search based on type
        switch(searchType) {
            case 'exact':
                matches = currentData.filter(item => item.key === searchTerm);
                break;
            case 'contains':
                matches = currentData.filter(item => item.key.includes(searchTerm));
                break;
            case 'prefix':
                matches = currentData.filter(item => item.key.startsWith(searchTerm));
                break;
            case 'suffix':
                matches = currentData.filter(item => item.key.endsWith(searchTerm));
                break;
            case 'regex':
                try {
                    const regex = new RegExp(searchTerm, 'i');
                    matches = currentData.filter(item => regex.test(item.key));
                } catch (e) {
                    resultsDiv.innerHTML = '<p style="color: #e74c3c; text-align: center; padding: 20px;">Invalid regular expression</p>';
                    return;
                }
                break;
        }
        
        // Display results
        if (matches.length === 0) {
            resultsDiv.innerHTML = '<p style="color: #f39c12; text-align: center; padding: 20px;">No matches found</p>';
        } else {
            displaySearchResults(matches, resultsDiv);
        }
        
    } catch (error) {
        resultsDiv.innerHTML = `<p style="color: #e74c3c; text-align: center; padding: 20px;">Search failed: ${error.message}</p>`;
        console.error('Search error:', error);
    }
}

function displaySearchResults(matches, container) {
    let html = `<h4 style="margin-bottom: 15px;">Found ${matches.length} match${matches.length !== 1 ? 'es' : ''}</h4>`;
    
    html += `
        <div class="table-container" style="max-height: 400px;">
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr>
                        <th>Key</th>
                        <th>Value</th>
                        <th>Length</th>
                        <th>Actions</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    matches.forEach(item => {
        const binaryIndicator = item.hasBinary ? '<span class="binary-indicator">BINARY</span>' : '';
        const displayValue = item.hasBinary ? 
            `[${item.valueLength} bytes]` : 
            escapeHtml(truncateString(item.value, 50));
        
        html += `
            <tr>
                <td class="key-cell">${formatKeyDisplay(item.key)}</td>
                <td class="value-cell">${displayValue}${binaryIndicator}</td>
                <td class="length-cell">${item.valueLength}</td>
                <td class="actions-cell">
                    <button class="btn-small" onclick="viewValueAdvanced('${escapeJs(item.key)}', '${escapeJs(item.value)}', '${escapeJs(item.hexValue)}', ${item.hasBinary})">View</button>
                    <button class="btn-small" onclick="editKey('${escapeJs(item.key)}', '${escapeJs(item.value)}')">Edit</button>
                    <button class="btn-small danger" onclick="deleteKey('${escapeJs(item.key)}')">Delete</button>
                </td>
            </tr>
        `;
    });
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    container.innerHTML = html;
}

// Add Enter key support for search
document.addEventListener('DOMContentLoaded', function() {
    const searchInput = document.getElementById('searchKeyAdvanced');
    if (searchInput) {
        searchInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter') {
                searchKeyAdvanced();
            }
        });
    }
});