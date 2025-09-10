// KV Store Viewer - Data Analysis Module

function analyzeData(data) {
    // Group by key to find duplicates
    const keyGroups = {};
    const hexKeyGroups = {};
    let totalSize = 0;
    let binaryCount = 0;
    
    data.forEach(item => {
        if (!keyGroups[item.key]) {
            keyGroups[item.key] = [];
        }
        keyGroups[item.key].push(item);
        
        // Also group by hex representation to detect subtle differences
        const hexKey = stringToHex(item.key);
        if (!hexKeyGroups[hexKey]) {
            hexKeyGroups[hexKey] = [];
        }
        hexKeyGroups[hexKey].push(item);
        
        totalSize += item.valueLength;
        if (item.hasBinary) binaryCount++;
    });
    
    const uniqueKeys = Object.keys(keyGroups).length;
    const duplicateGroups = Object.values(keyGroups).filter(group => group.length > 1).length;
    const suspiciousDuplicates = Object.values(keyGroups).filter(group => group.length > 2);
    
    dataStats = {
        totalKeys: data.length,
        uniqueKeys: uniqueKeys,
        duplicateGroups: duplicateGroups,
        avgValueSize: data.length > 0 ? Math.round(totalSize / data.length) : 0,
        totalDataSize: Math.round(totalSize / 1024),
        binaryKeys: binaryCount,
        keyGroups: keyGroups,
        hexKeyGroups: hexKeyGroups,
        suspiciousDuplicates: suspiciousDuplicates
    };
    
    updateSummary();
    updateDuplicateAnalysis();
    
    // Update analysis badge if tabs are available
    if (typeof updateAnalysisBadge === 'function') {
        updateAnalysisBadge();
    }
}

function stringToHex(str) {
    let result = '';
    for (let i = 0; i < str.length; i++) {
        result += str.charCodeAt(i).toString(16).padStart(2, '0');
    }
    return result;
}

function updateSummary() {
    // Only update elements that exist (since Analysis tab was removed)
    const totalKeysEl = document.getElementById('totalKeys');
    const uniqueKeysEl = document.getElementById('uniqueKeys');
    const duplicateGroupsEl = document.getElementById('duplicateGroups');
    const avgValueSizeEl = document.getElementById('avgValueSize');
    const totalDataSizeEl = document.getElementById('totalDataSize');
    const binaryKeysEl = document.getElementById('binaryKeys');
    
    if (totalKeysEl) totalKeysEl.textContent = dataStats.totalKeys;
    if (uniqueKeysEl) uniqueKeysEl.textContent = dataStats.uniqueKeys;
    if (duplicateGroupsEl) duplicateGroupsEl.textContent = dataStats.duplicateGroups;
    if (avgValueSizeEl) avgValueSizeEl.textContent = dataStats.avgValueSize;
    if (totalDataSizeEl) totalDataSizeEl.textContent = dataStats.totalDataSize;
    if (binaryKeysEl) binaryKeysEl.textContent = dataStats.binaryKeys;
    
    // Always show the summary in the analysis tab context (if it exists)
    const summaryDiv = document.getElementById('dataSummary');
    if (summaryDiv) {
        summaryDiv.style.display = 'block';
    }
}

function updateDuplicateAnalysis() {
    const analysisDiv = document.getElementById('duplicateAnalysis');
    const contentDiv = document.getElementById('duplicateAnalysisContent');
    
    if (dataStats.suspiciousDuplicates.length > 0) {
        if (analysisDiv) analysisDiv.style.display = 'block';
        
        let content = `<strong>⚠️ Found ${dataStats.suspiciousDuplicates.length} keys with 3+ duplicates:</strong><br><br>`;
        
        dataStats.suspiciousDuplicates.forEach(group => {
            const key = group[0].key;
            const count = group.length;
            const hexKey = stringToHex(key);
            const keyLength = key.length;
            
            content += `<div class="duplicate-details">`;
            content += `<strong>Key:</strong> ${formatKeyDisplay(key)} <br>`;
            content += `<strong>Count:</strong> ${count} duplicates<br>`;
            content += `<strong>Length:</strong> ${keyLength} bytes<br>`;
            content += `<strong>Hex:</strong> <span class="key-hex-display">${hexKey}</span>`;
            
            // Check if all values are identical
            const firstValue = group[0].value;
            const allValuesSame = group.every(item => item.value === firstValue);
            const firstValueLength = group[0].valueLength;
            const allLengthsSame = group.every(item => item.valueLength === firstValueLength);
            
            content += `<strong>Values identical:</strong> ${allValuesSame ? '✅ Yes' : '❌ No'}<br>`;
            content += `<strong>Value lengths identical:</strong> ${allLengthsSame ? '✅ Yes' : '❌ No'}<br>`;
            
            if (!allValuesSame || !allLengthsSame) {
                content += `<span style="color: #e74c3c;">⚠️ This indicates potential data corruption!</span>`;
            } else {
                content += `<span style="color: #e67e22;">⚠️ Identical key-value pairs - possible transaction/concurrency issue</span>`;
            }
            
            content += `</div>`;
        });
        
        if (contentDiv) contentDiv.innerHTML = content;
    } else {
        if (analysisDiv) analysisDiv.style.display = 'none';
    }
}

function showDetailedDuplicateAnalysis() {
    let details = '<div style="max-height: 400px; overflow-y: auto;">';
    details += '<h4>Detailed Hex Analysis of All Duplicates:</h4><br>';
    
    Object.entries(dataStats.keyGroups).forEach(([key, group]) => {
        if (group.length > 1) {
            details += `<div class="duplicate-details">`;
            details += `<strong>Key Group:</strong> ${formatKeyDisplay(key)} (${group.length} items)<br>`;
            
            group.forEach((item, index) => {
                const hexKey = stringToHex(item.key);
                details += `<div style="margin-left: 10px; margin: 5px 0;">`;
                details += `<strong>Item ${index + 1}:</strong><br>`;
                details += `Key Hex: <code>${hexKey}</code><br>`;
                details += `Key Length: ${item.key.length} bytes<br>`;
                details += `Value Length: ${item.valueLength} bytes<br>`;
                details += `Value Preview: ${item.hasBinary ? '[BINARY]' : item.value.substring(0, 50)}`;
                if (item.value.length > 50) details += '...';
                details += `</div>`;
            });
            
            details += `</div>`;
        }
    });
    
    details += '</div>';
    
    // Show in a modal-like alert for now (could be enhanced to use a proper modal)
    const newWindow = window.open('', '_blank', 'width=800,height=600,scrollbars=yes');
    newWindow.document.write(`
        <html>
            <head><title>Detailed Duplicate Analysis</title></head>
            <body style="font-family: monospace; padding: 20px;">
                ${details}
            </body>
        </html>
    `);
}