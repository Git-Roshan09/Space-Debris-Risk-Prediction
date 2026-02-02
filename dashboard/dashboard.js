// Space Debris Dashboard - Main JavaScript

const API_BASE_URL = 'http://localhost:5001/api';
let updateInterval = 30000; // 30 seconds default
let timelineChart = null;
let riskChart = null;

// Initialize dashboard
document.addEventListener('DOMContentLoaded', async () => {
    console.log('Dashboard initializing...');
    
    // Load configuration
    await loadConfig();
    
    // Initial data load
    await updateDashboard();
    
    // Set up auto-refresh
    setInterval(updateDashboard, updateInterval);
    
    console.log('Dashboard ready');
});

// Load configuration from API
async function loadConfig() {
    try {
        const response = await fetch(`${API_BASE_URL}/config`);
        const config = await response.json();
        
        updateInterval = config.update_interval_seconds * 1000;
        document.getElementById('update-interval').textContent = config.update_interval_seconds;
        
        console.log('Configuration loaded:', config);
    } catch (error) {
        console.error('Error loading configuration:', error);
    }
}

// Main update function
async function updateDashboard() {
    try {
        document.getElementById('status-indicator').className = 'status-indicator status-ok';
        document.getElementById('status-indicator').textContent = '● Connected';
        
        // Fetch all data in parallel
        const [stats, timeline, highRisk, pairs] = await Promise.all([
            fetchCollisionStats(),
            fetchCollisionTimeline(),
            fetchHighRiskCollisions(),
            fetchSatellitePairs()
        ]);
        
        // Update UI components
        updateSummaryCards(stats);
        updateDistanceStats(stats);
        updateTimelineChart(timeline);
        updateRiskChart(stats);
        updateAlertsTable(highRisk);
        updatePairsTable(pairs);
        
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
async function fetchCollisionStats() {
    const response = await fetch(`${API_BASE_URL}/collisions/stats`);
    return await response.json();
}

async function fetchCollisionTimeline() {
    const response = await fetch(`${API_BASE_URL}/collisions/timeline?days=7`);
    const data = await response.json();
    return data.data || [];
}

async function fetchHighRiskCollisions() {
    const response = await fetch(`${API_BASE_URL}/collisions/high-risk`);
    const data = await response.json();
    return data.data || [];
}

async function fetchSatellitePairs() {
    const response = await fetch(`${API_BASE_URL}/satellites/pairs`);
    const data = await response.json();
    return data.data || [];
}

// Update Summary Cards
function updateSummaryCards(stats) {
    const riskDist = stats.risk_distribution || {};
    
    document.getElementById('high-risk-count').textContent = riskDist.HIGH || 0;
    document.getElementById('medium-risk-count').textContent = riskDist.MEDIUM || 0;
    document.getElementById('low-risk-count').textContent = riskDist.LOW || 0;
    document.getElementById('total-count').textContent = stats.total_collisions || 0;
}

// Update Distance Statistics
function updateDistanceStats(stats) {
    const distStats = stats.distance_stats || {};
    
    document.getElementById('min-distance').textContent = 
        distStats.min_km ? distStats.min_km.toFixed(2) + ' km' : '-- km';
    document.getElementById('avg-distance').textContent = 
        distStats.avg_km ? distStats.avg_km.toFixed(2) + ' km' : '-- km';
    document.getElementById('max-distance').textContent = 
        distStats.max_km ? distStats.max_km.toFixed(2) + ' km' : '-- km';
}

// Update Timeline Chart
function updateTimelineChart(timeline) {
    const ctx = document.getElementById('timelineChart').getContext('2d');
    
    // Group data by time and risk level
    const timeMap = {};
    timeline.forEach(item => {
        const time = new Date(item.collision_time).toISOString().slice(0, 13) + ':00:00';
        if (!timeMap[time]) {
            timeMap[time] = { HIGH: 0, MEDIUM: 0, LOW: 0 };
        }
        timeMap[time][item.risk_level] = (timeMap[time][item.risk_level] || 0) + item.collision_count;
    });
    
    const times = Object.keys(timeMap).sort();
    const highData = times.map(t => timeMap[t].HIGH || 0);
    const mediumData = times.map(t => timeMap[t].MEDIUM || 0);
    const lowData = times.map(t => timeMap[t].LOW || 0);
    
    if (timelineChart) {
        timelineChart.destroy();
    }
    
    timelineChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: times,
            datasets: [
                {
                    label: 'High Risk',
                    data: highData,
                    borderColor: '#ff4757',
                    backgroundColor: 'rgba(255, 71, 87, 0.2)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Medium Risk',
                    data: mediumData,
                    borderColor: '#ffa502',
                    backgroundColor: 'rgba(255, 165, 2, 0.2)',
                    tension: 0.4,
                    fill: true
                },
                {
                    label: 'Low Risk',
                    data: lowData,
                    borderColor: '#2ed573',
                    backgroundColor: 'rgba(46, 213, 115, 0.2)',
                    tension: 0.4,
                    fill: true
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    labels: { color: '#e8e9ed' }
                }
            },
            scales: {
                x: {
                    type: 'time',
                    time: {
                        unit: 'hour',
                        displayFormats: {
                            hour: 'MMM dd HH:mm'
                        }
                    },
                    ticks: { color: '#a0a4b8' },
                    grid: { color: '#2d3561' }
                },
                y: {
                    beginAtZero: true,
                    ticks: { color: '#a0a4b8' },
                    grid: { color: '#2d3561' }
                }
            }
        }
    });
}

// Update Risk Distribution Chart
function updateRiskChart(stats) {
    const ctx = document.getElementById('riskChart').getContext('2d');
    const riskDist = stats.risk_distribution || {};
    
    if (riskChart) {
        riskChart.destroy();
    }
    
    riskChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['High Risk', 'Medium Risk', 'Low Risk'],
            datasets: [{
                data: [
                    riskDist.HIGH || 0,
                    riskDist.MEDIUM || 0,
                    riskDist.LOW || 0
                ],
                backgroundColor: [
                    'rgba(255, 71, 87, 0.8)',
                    'rgba(255, 165, 2, 0.8)',
                    'rgba(46, 213, 115, 0.8)'
                ],
                borderWidth: 2,
                borderColor: '#1a1f3a'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: { color: '#e8e9ed' }
                }
            }
        }
    });
}

// Update Alerts Table
function updateAlertsTable(alerts) {
    const tbody = document.getElementById('alerts-body');
    
    if (alerts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading">No high-risk alerts</td></tr>';
        return;
    }
    
    tbody.innerHTML = alerts.slice(0, 20).map(alert => {
        const collisionTime = new Date(alert.collision_time);
        const riskClass = alert.risk_level.toLowerCase();
        
        return `
            <tr>
                <td>${alert.satellite_1}</td>
                <td>${alert.satellite_2}</td>
                <td>${collisionTime.toLocaleString()}</td>
                <td>${alert.distance_km.toFixed(3)}</td>
                <td><span class="risk-badge risk-${riskClass}">${alert.risk_level}</span></td>
            </tr>
        `;
    }).join('');
}

// Update Pairs Table
function updatePairsTable(pairs) {
    const tbody = document.getElementById('pairs-body');
    
    if (pairs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="loading">No collision pairs found</td></tr>';
        return;
    }
    
    tbody.innerHTML = pairs.slice(0, 15).map(pair => `
        <tr>
            <td>${pair.satellite_1}</td>
            <td>${pair.satellite_2}</td>
            <td>${pair.collision_count}</td>
            <td>${pair.min_distance.toFixed(3)}</td>
            <td>${pair.avg_distance.toFixed(3)}</td>
        </tr>
    `).join('');
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
