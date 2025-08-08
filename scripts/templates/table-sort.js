// Table sorting functionality
document.addEventListener('DOMContentLoaded', function() {
    const table = document.getElementById('performance-summary-table');
    if (!table) return;  // Skip if table doesn't exist
    
    const headers = table.querySelectorAll('th.sortable');
    
    // Helper functions
    function parseNumber(str) {
        if (str === 'N/A' || str === '') return -1;
        return parseFloat(str.replace(/[^0-9.-]/g, ''));
    }
    
    function parseDuration(str) {
        if (str === 'N/A' || str === '') return -1;
        const match = str.match(/([0-9.]+)([a-zA-Z]+)/);
        if (!match) return -1;
        
        const value = parseFloat(match[1]);
        const unit = match[2];
        
        // Convert to nanoseconds for consistent comparison
        switch(unit) {
            case 'ns': return value;
            case 'μs': return value * 1000;
            case 'ms': return value * 1000000;
            case 's': return value * 1000000000;
            default: return value;
        }
    }
    
    function sortTable(columnIndex, sortType, ascending = true) {
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        
        rows.sort((a, b) => {
            const aText = a.cells[columnIndex].textContent.trim();
            const bText = b.cells[columnIndex].textContent.trim();
            
            let aValue, bValue;
            
            switch(sortType) {
                case 'number':
                    aValue = parseNumber(aText);
                    bValue = parseNumber(bText);
                    break;
                case 'duration':
                    aValue = parseDuration(aText);
                    bValue = parseDuration(bText);
                    break;
                case 'string':
                default:
                    aValue = aText.toLowerCase();
                    bValue = bText.toLowerCase();
                    break;
            }
            
            if (aValue < bValue) return ascending ? -1 : 1;
            if (aValue > bValue) return ascending ? 1 : -1;
            return 0;
        });
        
        // Re-append rows in sorted order
        rows.forEach(row => tbody.appendChild(row));
    }
    
    // Add click listeners to sortable headers
    headers.forEach((header, index) => {
        header.addEventListener('click', function() {
            const sortType = this.getAttribute('data-sort');
            const isCurrentlyAsc = this.classList.contains('sort-asc');
            const isCurrentlyDesc = this.classList.contains('sort-desc');
            
            // Clear all sort indicators
            headers.forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
            
            // Determine sort direction
            let ascending = true;
            if (isCurrentlyAsc) {
                ascending = false;
                this.classList.add('sort-desc');
            } else {
                this.classList.add('sort-asc');
            }
            
            // Sort the table
            sortTable(index, sortType, ascending);
        });
    });
});