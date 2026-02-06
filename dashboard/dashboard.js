// Space Debris Dashboard - Main JavaScript

const API_BASE_URL = 'http://localhost:5001/api';
let updateInterval = 30000; // 30 seconds default
let timelineChart = null;
let riskChart = null;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', async () => {
    console.log('Dashboard initializing...');
    
    // Initial data load
    await updateDashboard();
    
    // Set up auto-refresh
    setInterval(updateDashboard, updateInterval);
    
    console.log('Dashboard ready');
});

// Main update function
async function updateDashboard() {
    try {
        document.getElementById('status-indicator').className = 'status-indicator status-ok';
        document.getElementById('status-indicator').textContent = '● Connected';
        
        // Fetch dashboard stats from PostgreSQL
        const stats = await fetchDashboardStats();
        const highRisk = await fetchHighRiskCollisions();
        const satellites = await fetchSatellites();
        
        // Update UI components
        updateSummaryCards(stats);
        updateAlertsTable(highRisk);
        updateSatellitesTable(satellites);
        
        // Update timestamp
        const now = new Date();
        document.getElementById('last-update').textContent = 
            `Last Updated: ${now.toLocaleTimeString()}`;
        
    } catch (error) {
        console.error('Error updating dashboard:', error);
        document.getElementById('status-indicator').className = 'status-indicator status-error';
        document.getElementById('status-indicator').textContent = '● Error';
    }
}

// API Fetch Functions
async function fetchDashboardStats() {
    const response = await fetch(`${API_BASE_URL}/dashboard/stats`);
    return await response.json();
}

async function fetchHighRiskCollisions() {
    const response = await fetch(`${API_BASE_URL}/collisions/high-risk`);
    const data = await response.json();
    return data.high_risk_collisions || [];
}

async function fetchSatellites() {
    const response = await fetch(`${API_BASE_URL}/satellites?status=ACTIVE&limit=20`);
    const data = await response.json();
    return data.satellites || [];
}

// Update Summary Cards
function updateSummaryCards(stats) {
    document.getElementById('high-risk-count').textContent = stats.high_risk_collisions || 0;
    document.getElementById('medium-risk-count').textContent = stats.total_active_collisions || 0;
    document.getElementById('low-risk-count').textContent = stats.active_satellites || 0;
    document.getElementById('total-count').textContent = stats.stopped_satellites || 0;
}

// Update Satellites Table
function updateSatellitesTable(satellites) {
    const tbody = document.getElementById('satellites-body');
    
    if (!tbody) {
        return; // Table doesn't exist in current HTML
    }
    
    if (satellites.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading">No satellites found</td></tr>';
        return;
    }
    
    tbody.innerHTML = satellites.map(sat => {
        const lastEpoch = sat.last_tle_epoch ? new Date(sat.last_tle_epoch).toLocaleString() : 'Unknown';
        const altitude = Number.isFinite(sat.last_altitude_km) ? sat.last_altitude_km.toFixed(2) : '--';
        
        return `
            <tr>
                <td>${sat.norad_id}</td>
                <td>${sat.name || 'Unknown'}</td>
                <td>${sat.tracking_status}</td>
                <td>${altitude} km</td>
                <td>${lastEpoch}</td>
            </tr>
        `;
    }).join('');
}

function parseTimestamp(value) {
    if (value === null || value === undefined) {
        return null;
    }

    if (typeof value === 'number') {
        const ms = value < 1e12 ? value * 1000 : value;
        const date = new Date(ms);
        return isNaN(date.getTime()) ? null : date;
    }

    if (typeof value === 'string') {
        const direct = new Date(value);
        if (!isNaN(direct.getTime())) {
            return direct;
        }

        const numeric = Number(value);
        if (!Number.isNaN(numeric)) {
            const ms = numeric < 1e12 ? numeric * 1000 : numeric;
            const date = new Date(ms);
            return isNaN(date.getTime()) ? null : date;
        }
    }

    return null;
}

// Update Alerts Table
function updateAlertsTable(alerts) {
    const tbody = document.getElementById('alerts-body');
    
    if (alerts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading">No high-risk alerts</td></tr>';
        return;
    }
    
    tbody.innerHTML = alerts.slice(0, 20).map(alert => {
        // Support both field naming conventions (PostgreSQL uses predicted_time, miss_distance_km)
        const collisionDate = parseTimestamp(alert.predicted_time || alert.predicted_collision_time);
        const collisionTimeText = collisionDate ? collisionDate.toLocaleString() : 'Unknown';
        const riskClass = (alert.risk_level || 'LOW').toLowerCase();
        const distance = alert.miss_distance_km ?? alert.min_distance_km;
        const distanceText = Number.isFinite(distance) ? distance.toFixed(3) : '--';
        
        return `
            <tr>
                <td>${alert.satellite_1_id}</td>
                <td>${alert.satellite_2_id}</td>
                <td>${collisionTimeText}</td>
                <td>${distanceText}</td>
                <td><span class="risk-badge risk-${riskClass}">${alert.risk_level}</span></td>
            </tr>
        `;
    }).join('');
}

// Error handling for fetch
async function safeFetch(url) {
    try {
        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (error) {
        console.error(`Error fetching ${url}:`, error);
        return null;
    }
}
