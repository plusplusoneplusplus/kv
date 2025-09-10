// KV Store Viewer - Display and Formatting Module

function applySorting() {
    const sortBy = document.getElementById('sortBy').value;
    
    filteredData.sort((a, b) => {
        switch (sortBy) {
            case 'key':
                return a.key.localeCompare(b.key);
            case 'key-desc':
                return b.key.localeCompare(a.key);
            case 'size':
                return a.valueLength - b.valueLength;
            case 'size-desc':
                return b.valueLength - a.valueLength;
            case 'duplicates':
                const aCount = dataStats.keyGroups[a.key] ? dataStats.keyGroups[a.key].length : 1;
                const bCount = dataStats.keyGroups[b.key] ? dataStats.keyGroups[b.key].length : 1;
                if (aCount !== bCount) {
                    return bCount - aCount; // Show duplicates first
                }
                return a.key.localeCompare(b.key);
            default:
                return 0;
        }
    });
    
    displayCurrentData();
}

function applyFilter() {
    const filterType = document.getElementById('filterType').value;
    
    switch (filterType) {
        case 'all':
            filteredData = [...currentData];
            break;
        case 'duplicates':
            filteredData = currentData.filter(item => 
                dataStats.keyGroups[item.key] && dataStats.keyGroups[item.key].length > 1
            );
            break;
        case 'unique':
            filteredData = currentData.filter(item => 
                !dataStats.keyGroups[item.key] || dataStats.keyGroups[item.key].length === 1
            );
            break;
        case 'binary':
            filteredData = currentData.filter(item => item.hasBinary);
            break;
        case 'text':
            filteredData = currentData.filter(item => !item.hasBinary);
            break;
        default:
            filteredData = [...currentData];
    }
    
    applySorting();
}

function displayCurrentData() {
    const groupDuplicates = document.getElementById('groupDuplicates').checked;
    displayPaginatedData(filteredData, groupDuplicates);
}

function displayPaginatedData(data, groupDuplicates = false) {
    if (itemsPerPage === -1) {
        displayData(data, groupDuplicates);
        document.getElementById('pagination').style.display = 'none';
        return;
    }
    
    const startIndex = (currentPage - 1) * itemsPerPage;
    const endIndex = startIndex + itemsPerPage;
    const pageData = data.slice(startIndex, endIndex);
    
    displayData(pageData, groupDuplicates);
    
    const totalPages = Math.ceil(data.length / itemsPerPage);
    if (totalPages > 1) {
        document.getElementById('pagination').style.display = 'flex';
        updatePaginationControls();
    } else {
        document.getElementById('pagination').style.display = 'none';
    }
}

function displayData(data, groupDuplicates = false) {
    const tableBody = document.getElementById('tableBody');
    const table = document.getElementById('dataTable');
    
    tableBody.innerHTML = '';
    
    if (data.length === 0) {
        tableBody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #7f8c8d; padding: 40px;">No data found</td></tr>';
    } else {
        if (groupDuplicates && dataStats.keyGroups) {
            displayGroupedData(data, tableBody);
        } else {
            displayFlatData(data, tableBody);
        }
    }
    
    table.style.display = 'table';
}

function displayFlatData(data, tableBody) {
    data.forEach(item => {
        const row = tableBody.insertRow();
        const duplicateCount = dataStats.keyGroups[item.key] ? dataStats.keyGroups[item.key].length : 1;
        const duplicateIndicator = duplicateCount > 1 ? `<span class="duplicate-indicator">×${duplicateCount}</span>` : '';
        const binaryIndicator = item.hasBinary ? '<span class="binary-indicator">BINARY</span>' : '';
        const displayValue = item.hasBinary ? 
            `[${item.valueLength} bytes]` : 
            escapeHtml(truncateString(item.value, 100));
        
        if (duplicateCount > 1) {
            row.classList.add('key-group');
        }
        
        row.innerHTML = `
            <td class="key-cell">${formatKeyDisplay(item.key)}${duplicateIndicator}</td>
            <td class="value-cell">${displayValue}${binaryIndicator}</td>
            <td class="length-cell">${item.valueLength}</td>
            <td class="actions-cell">
                <button class="btn-small" onclick="viewValueAdvanced('${escapeJs(item.key)}', '${escapeJs(item.value)}', '${escapeJs(item.hexValue)}', ${item.hasBinary})">View</button>
                <button class="btn-small" onclick="editKey('${escapeJs(item.key)}', '${escapeJs(item.value)}')">Edit</button>
                <button class="btn-small danger" onclick="deleteKey('${escapeJs(item.key)}')">Delete</button>
            </td>
        `;
    });
}

function displayGroupedData(data, tableBody) {
    // Group data by key
    const grouped = {};
    data.forEach(item => {
        if (!grouped[item.key]) {
            grouped[item.key] = [];
        }
        grouped[item.key].push(item);
    });
    
    // Display grouped data
    Object.entries(grouped).forEach(([key, items]) => {
        items.forEach((item, index) => {
            const row = tableBody.insertRow();
            const isFirstInGroup = index === 0;
            const duplicateCount = items.length;
            const duplicateIndicator = duplicateCount > 1 && isFirstInGroup ? 
                `<span class="duplicate-indicator">×${duplicateCount}</span>` : '';
            const binaryIndicator = item.hasBinary ? '<span class="binary-indicator">BINARY</span>' : '';
            const displayValue = item.hasBinary ? 
                `[${item.valueLength} bytes]` : 
                escapeHtml(truncateString(item.value, 100));
            
            if (duplicateCount > 1) {
                row.classList.add('key-group');
            }
            
            row.innerHTML = `
                <td class="key-cell">${isFirstInGroup ? formatKeyDisplay(item.key) + duplicateIndicator : '↳ ' + formatKeyDisplay(item.key)}</td>
                <td class="value-cell">${displayValue}${binaryIndicator}</td>
                <td class="length-cell">${item.valueLength}</td>
                <td class="actions-cell">
                    <button class="btn-small" onclick="viewValueAdvanced('${escapeJs(item.key)}', '${escapeJs(item.value)}', '${escapeJs(item.hexValue)}', ${item.hasBinary})">View</button>
                    <button class="btn-small" onclick="editKey('${escapeJs(item.key)}', '${escapeJs(item.value)}')">Edit</button>
                    <button class="btn-small danger" onclick="deleteKey('${escapeJs(item.key)}')">Delete</button>
                </td>
            `;
        });
    });
}

function formatKeyDisplay(key) {
    if (!key) return '';
    
    let result = '';
    for (let i = 0; i < key.length; i++) {
        const char = key[i];
        const charCode = char.charCodeAt(0);
        
        // Check if character is printable ASCII (32-126) or common extended ASCII
        if (charCode >= 32 && charCode <= 126) {
            // Printable ASCII
            result += escapeHtml(char);
        } else if (charCode === 9) {
            // Tab
            result += '<span class="hex-char">\\t</span>';
        } else if (charCode === 10) {
            // Newline
            result += '<span class="hex-char">\\n</span>';
        } else if (charCode === 13) {
            // Carriage return
            result += '<span class="hex-char">\\r</span>';
        } else {
            // Non-printable characters - show as hex in gray
            result += `<span class="hex-char">\\x${charCode.toString(16).padStart(2, '0').toUpperCase()}</span>`;
        }
    }
    return result;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function escapeJs(text) {
    if (text === null || text === undefined) return '';
    if (typeof text !== 'string') text = String(text);
    return text.replace(/\\/g, '\\\\')
                  .replace(/'/g, "\\'")
                  .replace(/"/g, '\\"')
                  .replace(/\n/g, '\\n')
                  .replace(/\r/g, '\\r')
                  .replace(/\t/g, '\\t');
}

function truncateString(str, length) {
    return str.length > length ? str.substring(0, length) + '...' : str;
}