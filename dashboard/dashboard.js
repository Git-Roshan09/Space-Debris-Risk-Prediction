// Space Debris Dashboard — Professional Monitoring JavaScript

const API_BASE_URL = 'http://localhost:5001/api';
let updateInterval = 30000;
let autoRefreshTimer = null;
let autoRefreshEnabled = true;
let riskChart = null;

// Pagination state
let currentPage = 1;
let totalPages = 1;
let currentRiskFilter = '';

// ═══════════════════════════════════════════
// INITIALIZATION
// ═══════════════════════════════════════════
document.addEventListener('DOMContentLoaded', async () => {
    initFilterBar();
    initPagination();
    initAutoRefreshToggle();
    await updateDashboard();
    startAutoRefresh();
});

function startAutoRefresh() {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    autoRefreshTimer = setInterval(() => {
        if (autoRefreshEnabled) updateDashboard();
    }, updateInterval);
}

function initAutoRefreshToggle() {
    const btn = document.getElementById('btn-auto-refresh');
    btn.addEventListener('click', () => {
        autoRefreshEnabled = !autoRefreshEnabled;
        btn.classList.toggle('active', autoRefreshEnabled);
        btn.textContent = autoRefreshEnabled ? 'AUTO ⟳' : 'PAUSED';
    });
}

// ═══════════════════════════════════════════
// MAIN UPDATE
// ═══════════════════════════════════════════
async function updateDashboard() {
    try {
        setStatus('ok', 'Connected');

        const [stats, allCollisions, pairs, simulationTime] = await Promise.all([
            safeFetch(`${API_BASE_URL}/dashboard/stats`),
            safeFetch(`${API_BASE_URL}/collisions/all?page=${currentPage}&per_page=50${currentRiskFilter ? '&risk_level=' + currentRiskFilter : ''}`),
            safeFetch(`${API_BASE_URL}/collisions/frequency?limit=20`),
            safeFetch(`${API_BASE_URL}/simulation/time`)
        ]);

        if (stats) {
            updateSummaryCards(stats);
            updateDistanceStats(stats);
            updateClosestApproach(stats);
            updateRiskChart(stats);
        }
        if (allCollisions) {
            updateCollisionsTable(allCollisions);
            updatePaginationUI(allCollisions);
        }
        if (pairs) {
            updatePairsTable(pairs);
        }
        if (simulationTime) {
            updateSimulationTime(simulationTime);
        }

        document.getElementById('last-update').textContent =
            `Last Updated: ${new Date().toLocaleTimeString()}`;

    } catch (error) {
        console.error('Dashboard update error:', error);
        setStatus('error', 'Error');
    }
}

function setStatus(type, text) {
    const el = document.getElementById('status-indicator');
    el.className = `status-indicator status-${type}`;
    el.textContent = text;
}

// ═══════════════════════════════════════════
// SIMULATION TIME FORMATTING
// ═══════════════════════════════════════════
function formatSimulationDateTime(dateValue) {
    if (!dateValue) return '<span class="sim-timestamp">Unknown</span>';
    
    const date = dateValue instanceof Date ? dateValue : new Date(dateValue);
    if (isNaN(date.getTime())) return '<span class="sim-timestamp">Invalid Date</span>';
    
    // Format as simulation time with clear indication it's not real-time
    const options = {
        year: 'numeric',
        month: '2-digit', 
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZone: 'UTC'  // Use UTC to avoid confusion with local time
    };
    
    const formattedTime = date.toLocaleString('en-GB', options);  // DD/MM/YYYY format
    return `<span class="sim-timestamp">Sim: ${formattedTime}</span>`;
}

// ═══════════════════════════════════════════
// SIMULATION TIME UPDATE
// ═══════════════════════════════════════════
function updateSimulationTime(simulationData) {
    const simTime = new Date(simulationData.current_simulated_time);
    const elapsedDays = Math.floor(simulationData.elapsed_simulated_days);
    
    // Format the simulation time nicely
    const formattedTime = simTime.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
    
    document.getElementById('simulation-time').innerHTML = 
        `🕐 Simulation Time: <strong>${formattedTime}</strong> (Day ${elapsedDays + 1})`;
}

// ═══════════════════════════════════════════
// SUMMARY CARDS
// ═══════════════════════════════════════════
function updateSummaryCards(stats) {
    animateValue('critical-risk-count', stats.critical_risk_collisions || 0);
    animateValue('high-risk-count', stats.high_risk_collisions || 0);
    animateValue('medium-risk-count', stats.medium_risk_collisions || 0);
    animateValue('low-risk-count', stats.low_risk_collisions || 0);
    animateValue('total-count', stats.total_active_collisions || 0);
}

function animateValue(elementId, endValue) {
    const el = document.getElementById(elementId);
    const current = parseInt(el.textContent) || 0;
    if (current === endValue) { el.textContent = endValue; return; }

    const duration = 600;
    const startTime = performance.now();

    function step(timestamp) {
        const progress = Math.min((timestamp - startTime) / duration, 1);
        const eased = 1 - Math.pow(1 - progress, 3); // ease-out cubic
        el.textContent = Math.round(current + (endValue - current) * eased);
        if (progress < 1) requestAnimationFrame(step);
    }
    requestAnimationFrame(step);
}

// ═══════════════════════════════════════════
// DISTANCE STATISTICS
// ═══════════════════════════════════════════
function updateDistanceStats(stats) {
    document.getElementById('min-distance').textContent =
        stats.min_distance_km != null ? stats.min_distance_km.toFixed(3) : '--';
    document.getElementById('avg-distance').textContent =
        stats.avg_distance_km != null ? stats.avg_distance_km.toFixed(3) : '--';
    document.getElementById('max-distance').textContent =
        stats.max_distance_km != null ? stats.max_distance_km.toFixed(3) : '--';
}

function updateClosestApproach(stats) {
    const container = document.getElementById('closest-approach');
    const detail = document.getElementById('closest-detail');

    if (stats.closest_approach) {
        const ca = stats.closest_approach;
        // Show "Name [ID]" format to distinguish objects with same names
        const name1 = ca.satellite_1_name 
            ? `${ca.satellite_1_name} <span class="norad-id">[${ca.satellite_1_id}]</span>`
            : ca.satellite_1_id;
        const name2 = ca.satellite_2_name 
            ? `${ca.satellite_2_name} <span class="norad-id">[${ca.satellite_2_id}]</span>`
            : ca.satellite_2_id;
        const dist = ca.miss_distance_km != null ? ca.miss_distance_km.toFixed(3) : '--';
        const time = formatSimulationDateTime(ca.predicted_time);

        detail.innerHTML = `<strong>${name1}</strong> ↔ <strong>${name2}</strong> — ` +
            `<strong>${dist} km</strong> | ${time} | ` +
            `<span class="risk-badge risk-${(ca.risk_level || 'low').toLowerCase()}">${ca.risk_level}</span>`;
        container.style.display = 'block';
    } else {
        container.style.display = 'none';
    }
}

// ═══════════════════════════════════════════
// RISK DISTRIBUTION CHART
// ═══════════════════════════════════════════
function updateRiskChart(stats) {
    const ctx = document.getElementById('riskChart');
    const data = [
        stats.critical_risk_collisions || 0,
        stats.high_risk_collisions || 0,
        stats.medium_risk_collisions || 0,
        stats.low_risk_collisions || 0
    ];

    if (riskChart) {
        riskChart.data.datasets[0].data = data;
        riskChart.update('none');
        return;
    }

    riskChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Critical', 'High', 'Medium', 'Low'],
            datasets: [{
                data: data,
                backgroundColor: [
                    'rgba(224, 64, 251, 0.8)',
                    'rgba(255, 82, 82, 0.8)',
                    'rgba(255, 171, 64, 0.8)',
                    'rgba(105, 240, 174, 0.8)'
                ],
                borderColor: [
                    'rgba(224, 64, 251, 1)',
                    'rgba(255, 82, 82, 1)',
                    'rgba(255, 171, 64, 1)',
                    'rgba(105, 240, 174, 1)'
                ],
                borderWidth: 2,
                hoverBorderWidth: 3,
                spacing: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            cutout: '65%',
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#8892b0',
                        font: { family: 'Inter', size: 12, weight: 600 },
                        padding: 16,
                        usePointStyle: true,
                        pointStyleWidth: 10
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(16, 22, 48, 0.95)',
                    titleFont: { family: 'Inter', size: 13, weight: 700 },
                    bodyFont: { family: 'Inter', size: 12 },
                    borderColor: 'rgba(68, 138, 255, 0.3)',
                    borderWidth: 1,
                    cornerRadius: 8,
                    padding: 12
                }
            }
        }
    });
}

// ═══════════════════════════════════════════
// ALL COLLISIONS TABLE
// ═══════════════════════════════════════════
function updateCollisionsTable(data) {
    const tbody = document.getElementById('collisions-body');
    const badge = document.getElementById('collision-total-badge');
    const collisions = data.collisions || [];

    badge.textContent = data.total_count || collisions.length;

    if (collisions.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="loading">No predicted close approaches found</td></tr>';
        return;
    }

    tbody.innerHTML = collisions.map(c => {
        const time = parseTimestamp(c.predicted_time);
        const timeText = formatSimulationDateTime(time);
        const riskClass = (c.risk_level || 'low').toLowerCase();
        const distance = Number.isFinite(c.miss_distance_km) ? c.miss_distance_km.toFixed(3) : '--';
        const velocity = Number.isFinite(c.relative_velocity_kms) ? c.relative_velocity_kms.toFixed(3) : '--';
        const prob = Number.isFinite(c.collision_probability) ? (c.collision_probability * 100).toFixed(4) + '%' : '--';
        
        // Show "Name [ID]" format to distinguish objects with same names
        const name1 = c.satellite_1_name 
            ? `${c.satellite_1_name} <span class="norad-id">[${c.satellite_1_id}]</span>`
            : c.satellite_1_id;
        const name2 = c.satellite_2_name 
            ? `${c.satellite_2_name} <span class="norad-id">[${c.satellite_2_id}]</span>`
            : c.satellite_2_id;

        return `<tr>
            <td>${name1}</td>
            <td>${name2}</td>
            <td>${timeText}</td>
            <td>${distance}</td>
            <td>${velocity}</td>
            <td>${prob}</td>
            <td><span class="risk-badge risk-${riskClass}">${c.risk_level}</span></td>
        </tr>`;
    }).join('');
}

// ═══════════════════════════════════════════
// SATELLITE PAIRS TABLE
// ═══════════════════════════════════════════
function updatePairsTable(data) {
    const tbody = document.getElementById('pairs-body');
    const pairs = data.pairs || [];

    if (pairs.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">No recurring pairs found</td></tr>';
        return;
    }

    tbody.innerHTML = pairs.map(p => {
        // Show "Name [ID]" format to distinguish objects with same names
        const name1 = p.satellite_1_name 
            ? `${p.satellite_1_name} <span class="norad-id">[${p.satellite_1_id}]</span>`
            : p.satellite_1_id;
        const name2 = p.satellite_2_name 
            ? `${p.satellite_2_name} <span class="norad-id">[${p.satellite_2_id}]</span>`
            : p.satellite_2_id;
        const minD = Number.isFinite(p.min_distance_km) ? p.min_distance_km.toFixed(3) : '--';
        const avgD = Number.isFinite(p.avg_distance_km) ? p.avg_distance_km.toFixed(3) : '--';
        const maxD = Number.isFinite(p.max_distance_km) ? p.max_distance_km.toFixed(3) : '--';

        return `<tr>
            <td>${name1}</td>
            <td>${name2}</td>
            <td>${p.collision_count || p.approach_events || 0}</td>
            <td>${minD}</td>
            <td>${avgD}</td>
            <td>${maxD}</td>
        </tr>`;
    }).join('');
}

// ═══════════════════════════════════════════
// FILTER BAR
// ═══════════════════════════════════════════
function initFilterBar() {
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            currentRiskFilter = btn.dataset.risk;
            currentPage = 1;
            updateDashboard();
        });
    });
}

// ═══════════════════════════════════════════
// PAGINATION
// ═══════════════════════════════════════════
function initPagination() {
    document.getElementById('btn-prev').addEventListener('click', () => {
        if (currentPage > 1) { currentPage--; updateDashboard(); }
    });
    document.getElementById('btn-next').addEventListener('click', () => {
        if (currentPage < totalPages) { currentPage++; updateDashboard(); }
    });
}

function updatePaginationUI(data) {
    totalPages = data.total_pages || 1;
    currentPage = data.page || 1;
    document.getElementById('page-info').textContent = `Page ${currentPage} of ${totalPages}`;
    document.getElementById('btn-prev').disabled = currentPage <= 1;
    document.getElementById('btn-next').disabled = currentPage >= totalPages;
}

// ═══════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════
function parseTimestamp(value) {
    if (value === null || value === undefined) return null;

    if (typeof value === 'number') {
        const ms = value < 1e12 ? value * 1000 : value;
        const date = new Date(ms);
        return isNaN(date.getTime()) ? null : date;
    }

    if (typeof value === 'string') {
        const direct = new Date(value);
        if (!isNaN(direct.getTime())) return direct;

        const numeric = Number(value);
        if (!Number.isNaN(numeric)) {
            const ms = numeric < 1e12 ? numeric * 1000 : numeric;
            const date = new Date(ms);
            return isNaN(date.getTime()) ? null : date;
        }
    }

    return null;
}

async function safeFetch(url) {
    try {
        const response = await fetch(url);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return await response.json();
    } catch (error) {
        console.error(`Fetch error [${url}]:`, error);
        return null;
    }
}
