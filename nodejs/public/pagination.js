// KV Store Viewer - Pagination Module

function changePage(page) {
    itemsPerPage = parseInt(document.getElementById('itemsPerPage').value);
    if (itemsPerPage === -1) {
        // Show all items
        displayCurrentData();
        document.getElementById('pagination').style.display = 'none';
        return;
    }
    
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    currentPage = Math.max(1, Math.min(page, totalPages));
    displayCurrentData();
    updatePaginationControls();
}

function updatePaginationControls() {
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    const startItem = (currentPage - 1) * itemsPerPage + 1;
    const endItem = Math.min(currentPage * itemsPerPage, filteredData.length);
    
    document.getElementById('paginationInfo').textContent = 
        `Showing ${startItem}-${endItem} of ${filteredData.length} items`;
    
    document.getElementById('prevBtn').disabled = currentPage === 1;
    document.getElementById('nextBtn').disabled = currentPage === totalPages;
    
    // Generate page number buttons
    const pageNumbers = document.getElementById('pageNumbers');
    pageNumbers.innerHTML = '';
    
    const maxVisiblePages = 5;
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage < maxVisiblePages - 1) {
        startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }
    
    for (let i = startPage; i <= endPage; i++) {
        const pageBtn = document.createElement('button');
        pageBtn.className = 'pagination-btn';
        pageBtn.textContent = i;
        pageBtn.onclick = () => changePage(i);
        
        if (i === currentPage) {
            pageBtn.style.backgroundColor = '#2c3e50';
        }
        
        pageNumbers.appendChild(pageBtn);
    }
}