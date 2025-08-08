// Wait for Chart.js to load and DOM to be ready
function initCharts() {
    if (typeof Chart === 'undefined') {
        console.log('Chart.js not loaded yet, retrying...');
        setTimeout(initCharts, 100);
        return;
    }
    
    console.log('Chart.js loaded, initializing charts...');
    // Chart generation code will be inserted here dynamically
}

// Initialize charts when page loads
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCharts);
} else {
    initCharts();
}