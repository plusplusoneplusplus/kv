// Component Loader Utility
class ComponentLoader {
    static async loadComponent(componentPath, targetElementId) {
        try {
            const response = await fetch(componentPath);
            if (!response.ok) {
                throw new Error(`Failed to load component: ${componentPath}`);
            }
            const html = await response.text();
            const targetElement = document.getElementById(targetElementId);
            if (targetElement) {
                targetElement.innerHTML = html;
            } else {
                console.error(`Target element with ID '${targetElementId}' not found`);
            }
        } catch (error) {
            console.error('Error loading component:', error);
        }
    }

    static async loadComponents(components) {
        const promises = components.map(({ path, targetId }) => 
            this.loadComponent(path, targetId)
        );
        await Promise.all(promises);
    }

    static async initializeApp() {
        // Load all components
        const components = [
            { path: 'components/browse-data.html', targetId: 'browse-tab-container' },
            { path: 'components/clear-data.html', targetId: 'clear-tab-container' },
            { path: 'components/settings.html', targetId: 'settings-tab-container' },
            { path: 'components/cluster-health.html', targetId: 'cluster-tab-container' },
            { path: 'components/node-details.html', targetId: 'node-details-tab-container' },
            { path: 'components/replication-monitor.html', targetId: 'replication-tab-container' },
            { path: 'components/modals.html', targetId: 'modals-container' }
        ];

        try {
            await this.loadComponents(components);
            console.log('All components loaded successfully');
            
            // Initialize any post-load functionality
            this.initializePostLoad();
        } catch (error) {
            console.error('Error initializing app:', error);
        }
    }

    static initializePostLoad() {
        // Any initialization that needs to happen after components are loaded
        // For example, setting up event listeners, initializing charts, etc.
        
        // Re-establish tab functionality since we've loaded new HTML
        this.setupTabEventListeners();
    }

    static setupTabEventListeners() {
        // This ensures tab switching works after components are loaded
        const tabButtons = document.querySelectorAll('.tab-button');
        tabButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                const tabName = button.getAttribute('data-tab');
                if (tabName && typeof showTab === 'function') {
                    showTab(tabName);
                }
            });
        });
    }
}

// Auto-initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    ComponentLoader.initializeApp();
});
