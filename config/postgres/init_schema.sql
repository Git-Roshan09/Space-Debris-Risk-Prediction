-- Space Debris Risk Prediction - PostgreSQL Schema
-- Database: space_debris
-- Purpose: Store operational metadata and current state for fast dashboard queries

-- ==========================================
-- 1. SATELLITE CATALOG & TRACKING STATUS
-- ==========================================
CREATE TABLE IF NOT EXISTS satellites (
    norad_id INTEGER PRIMARY KEY,
    name VARCHAR(255),
    object_type VARCHAR(50),
    country VARCHAR(100),
    
    -- Current Tracking Status
    tracking_status VARCHAR(50) NOT NULL DEFAULT 'ACTIVE',
    
    -- Latest Observations
    last_tle_epoch TIMESTAMP,
    tle_age_days INTEGER,
    last_altitude_km DOUBLE PRECISION,
    last_velocity_kms DOUBLE PRECISION,
    last_position_x DOUBLE PRECISION,
    last_position_y DOUBLE PRECISION,
    last_position_z DOUBLE PRECISION,
    last_sgp4_error_code INTEGER DEFAULT 0,
    
    -- Orbital Elements (from latest TLE)
    inclination DOUBLE PRECISION,
    eccentricity VARCHAR(20),
    mean_motion DOUBLE PRECISION,
    
    -- Statistics
    total_observations INTEGER DEFAULT 0,
    first_observed_at TIMESTAMP,
    status_updated_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_sat_tracking_status ON satellites(tracking_status);
CREATE INDEX IF NOT EXISTS idx_sat_last_tle_epoch ON satellites(last_tle_epoch DESC);
CREATE INDEX IF NOT EXISTS idx_sat_altitude ON satellites(last_altitude_km);
CREATE INDEX IF NOT EXISTS idx_sat_status_updated ON satellites(status_updated_at DESC);

COMMENT ON TABLE satellites IS 'Current state and metadata for all tracked satellites';
COMMENT ON COLUMN satellites.tracking_status IS 'ACTIVE, STOPPED_SGP4_ERROR, STOPPED_LOW_ALTITUDE, STOPPED_STALE_TLE';


-- ==========================================
-- 2. COLLISION ALERTS (Active/Recent Only)
-- ==========================================
CREATE TABLE IF NOT EXISTS collision_alerts (
    id SERIAL PRIMARY KEY,
    satellite_1_id INTEGER NOT NULL,
    satellite_2_id INTEGER NOT NULL,
    satellite_1_name VARCHAR(255),
    satellite_2_name VARCHAR(255),
    
    -- Prediction Details
    predicted_time TIMESTAMP NOT NULL,
    miss_distance_km DOUBLE PRECISION NOT NULL,
    relative_velocity_kms DOUBLE PRECISION,
    
    -- Position at closest approach
    approach_position_x DOUBLE PRECISION,
    approach_position_y DOUBLE PRECISION,
    approach_position_z DOUBLE PRECISION,
    
    -- Risk Assessment
    risk_level VARCHAR(20) NOT NULL,
    collision_probability DOUBLE PRECISION,
    
    -- Metadata
    detected_at TIMESTAMP DEFAULT NOW(),
    batch_id VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    archived_at TIMESTAMP,
    
    FOREIGN KEY (satellite_1_id) REFERENCES satellites(norad_id) ON DELETE CASCADE,
    FOREIGN KEY (satellite_2_id) REFERENCES satellites(norad_id) ON DELETE CASCADE,
    
    CONSTRAINT unique_collision UNIQUE (satellite_1_id, satellite_2_id, predicted_time)
);

-- Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_col_risk_level ON collision_alerts(risk_level);
CREATE INDEX IF NOT EXISTS idx_col_predicted_time ON collision_alerts(predicted_time);
CREATE INDEX IF NOT EXISTS idx_col_active_high_risk ON collision_alerts(is_active, risk_level, predicted_time) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_col_satellites ON collision_alerts(satellite_1_id, satellite_2_id);
CREATE INDEX IF NOT EXISTS idx_col_miss_distance ON collision_alerts(miss_distance_km);

COMMENT ON TABLE collision_alerts IS 'Active and recent collision predictions for dashboard (HDFS has full history)';
COMMENT ON COLUMN collision_alerts.risk_level IS 'HIGH (< 5km), MEDIUM (5-10km), LOW (10-50km)';


-- ==========================================
-- 3. TRACKING STATUS CHANGE HISTORY
-- ==========================================
CREATE TABLE IF NOT EXISTS tracking_status_changes (
    id SERIAL PRIMARY KEY,
    norad_id INTEGER NOT NULL,
    satellite_name VARCHAR(255),
    
    old_status VARCHAR(50),
    new_status VARCHAR(50) NOT NULL,
    
    -- Why status changed
    reason TEXT,
    altitude_km DOUBLE PRECISION,
    tle_age_days INTEGER,
    sgp4_error_code INTEGER,
    
    changed_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (norad_id) REFERENCES satellites(norad_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_status_changes_satellite ON tracking_status_changes(norad_id, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_status_changes_new ON tracking_status_changes(new_status, changed_at DESC);
CREATE INDEX IF NOT EXISTS idx_status_changes_time ON tracking_status_changes(changed_at DESC);

COMMENT ON TABLE tracking_status_changes IS 'Audit log of all satellite tracking status changes';


-- ==========================================
-- 4. SYSTEM METRICS FOR DASHBOARD
-- ==========================================
CREATE TABLE IF NOT EXISTS system_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_unit VARCHAR(50),
    metric_description TEXT,
    recorded_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_name_time ON system_metrics(metric_name, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON system_metrics(recorded_at DESC);

COMMENT ON TABLE system_metrics IS 'System performance and operational metrics';

-- Common metrics to track:
-- - active_satellites_count
-- - stopped_satellites_count
-- - high_risk_collisions_count
-- - processing_time_seconds
-- - kafka_lag_seconds


-- ==========================================
-- 5. PIPELINE STATE MANAGEMENT
-- ==========================================
CREATE TABLE IF NOT EXISTS pipeline_state (
    component_name VARCHAR(50) PRIMARY KEY,
    last_run_start TIMESTAMP,
    last_run_end TIMESTAMP,
    last_run_status VARCHAR(20) NOT NULL DEFAULT 'IDLE',  -- IDLE, RUNNING, SUCCESS, FAILED
    records_processed INTEGER DEFAULT 0,
    data_version BIGINT DEFAULT 0,           -- Kafka offset or batch ID
    next_run_allowed BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_state_status ON pipeline_state(last_run_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_state_updated ON pipeline_state(updated_at DESC);

COMMENT ON TABLE pipeline_state IS 'Tracks execution state and coordination between pipeline components';
COMMENT ON COLUMN pipeline_state.component_name IS 'TLE_INGESTION, SGP4_PROCESSING, COLLISION_PREDICTION';
COMMENT ON COLUMN pipeline_state.data_version IS 'Kafka offset, batch ID, or data timestamp for coordination';
COMMENT ON COLUMN pipeline_state.next_run_allowed IS 'Flag to prevent concurrent runs or coordinate dependent jobs';

-- Initialize pipeline components
INSERT INTO pipeline_state (component_name, last_run_status) VALUES
    ('TLE_INGESTION', 'IDLE'),
    ('SGP4_PROCESSING', 'IDLE'),
    ('COLLISION_PREDICTION', 'IDLE')
ON CONFLICT (component_name) DO NOTHING;


-- ==========================================
-- 6. HELPER VIEWS FOR DASHBOARD
-- ==========================================

-- View: Active satellites summary
CREATE OR REPLACE VIEW active_satellites_summary AS
SELECT 
    tracking_status,
    COUNT(*) as count,
    AVG(last_altitude_km) as avg_altitude_km,
    MIN(last_altitude_km) as min_altitude_km,
    MAX(last_altitude_km) as max_altitude_km,
    AVG(tle_age_days) as avg_tle_age_days
FROM satellites
GROUP BY tracking_status;

COMMENT ON VIEW active_satellites_summary IS 'Summary statistics of satellites by tracking status';


-- View: Recent high-risk collisions
CREATE OR REPLACE VIEW high_risk_collisions_today AS
SELECT 
    ca.*,
    s1.name as sat1_name,
    s1.tracking_status as sat1_status,
    s2.name as sat2_name,
    s2.tracking_status as sat2_status
FROM collision_alerts ca
JOIN satellites s1 ON ca.satellite_1_id = s1.norad_id
JOIN satellites s2 ON ca.satellite_2_id = s2.norad_id
WHERE ca.risk_level = 'HIGH'
  AND ca.is_active = TRUE
  AND ca.predicted_time >= CURRENT_DATE
  AND ca.predicted_time < CURRENT_DATE + INTERVAL '7 days'
ORDER BY ca.miss_distance_km ASC;

COMMENT ON VIEW high_risk_collisions_today IS 'High-risk collisions in next 7 days';


-- View: Recent status changes
CREATE OR REPLACE VIEW recent_status_changes AS
SELECT 
    tsc.*,
    s.name as satellite_name_current,
    s.tracking_status as current_status
FROM tracking_status_changes tsc
JOIN satellites s ON tsc.norad_id = s.norad_id
WHERE tsc.changed_at >= NOW() - INTERVAL '7 days'
ORDER BY tsc.changed_at DESC;

COMMENT ON VIEW recent_status_changes IS 'Satellite status changes in last 7 days';


-- ==========================================
-- 7. INITIAL SEED DATA (Optional)
-- ==========================================

-- Insert some common system metrics for dashboard
INSERT INTO system_metrics (metric_name, metric_value, metric_unit, metric_description)
VALUES 
    ('active_satellites_count', 0, 'count', 'Number of actively tracked satellites'),
    ('high_risk_collisions_count', 0, 'count', 'Number of active high-risk collision alerts'),
    ('processing_time_seconds', 0, 'seconds', 'Last batch processing time')
ON CONFLICT DO NOTHING;


-- ==========================================
-- 8. UTILITY FUNCTIONS
-- ==========================================

-- Function to archive old collision alerts
CREATE OR REPLACE FUNCTION archive_old_collision_alerts()
RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    -- Mark collisions older than 7 days as inactive
    UPDATE collision_alerts
    SET is_active = FALSE,
        archived_at = NOW()
    WHERE is_active = TRUE
      AND predicted_time < NOW() - INTERVAL '7 days';
    
    GET DIAGNOSTICS archived_count = ROW_COUNT;
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION archive_old_collision_alerts() IS 'Archive collision alerts older than 7 days';


-- Function to update system metrics
CREATE OR REPLACE FUNCTION update_system_metrics()
RETURNS VOID AS $$
BEGIN
    -- Update active satellites count
    INSERT INTO system_metrics (metric_name, metric_value, metric_unit)
    SELECT 'active_satellites_count', COUNT(*), 'count'
    FROM satellites WHERE tracking_status = 'ACTIVE';
    
    -- Update high-risk collisions count
    INSERT INTO system_metrics (metric_name, metric_value, metric_unit)
    SELECT 'high_risk_collisions_count', COUNT(*), 'count'
    FROM collision_alerts 
    WHERE risk_level = 'HIGH' AND is_active = TRUE;
    
    -- Update total collisions count
    INSERT INTO system_metrics (metric_name, metric_value, metric_unit)
    SELECT 'total_active_collisions_count', COUNT(*), 'count'
    FROM collision_alerts WHERE is_active = TRUE;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION update_system_metrics() IS 'Update dashboard system metrics';


-- ==========================================
-- GRANT PERMISSIONS
-- ==========================================

-- Grant permissions to application user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO postgres;


-- ==========================================
-- COMPLETION MESSAGE
-- ==========================================

DO $$ 
BEGIN
    RAISE NOTICE '✓ Space Debris PostgreSQL schema initialized successfully';
    RAISE NOTICE '✓ Tables: satellites, collision_alerts, tracking_status_changes, system_metrics, pipeline_state';
    RAISE NOTICE '✓ Views: active_satellites_summary, high_risk_collisions_today, recent_status_changes';
    RAISE NOTICE '✓ Functions: archive_old_collision_alerts(), update_system_metrics()';
END $$;
