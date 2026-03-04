// ── Types matching dashboard_api.py response shapes ──

export type RiskLevel = 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';

export interface HealthResponse {
  status: 'healthy' | 'unhealthy';
  hdfs:   string;
  timestamp: string;
}

export interface ClosestApproach {
  norad_id_1:     string;
  norad_id_2:     string;
  distance_km:    number;
  risk_level:     RiskLevel;
  epoch_1:        string;
  collision_type: string;
}

export interface StatsResponse {
  total_debris_objects:     number;
  critical_risk_collisions: number;
  high_risk_collisions:     number;
  medium_risk_collisions:   number;
  low_risk_collisions:      number;
  total_collision_pairs:    number;
  min_distance_km:          number | null;
  avg_distance_km:          number | null;
  max_distance_km:          number | null;
  closest_approach:         ClosestApproach | null;
  timestamp:                string;
}

export interface Collision {
  norad_id_1:              string;
  norad_id_2:              string;
  type_1:                  string;
  type_2:                  string;
  epoch_1:                 string;
  epoch_2:                 string;
  distance_km:             number | null;
  relative_velocity_kms:   number | null;
  risk_level:              RiskLevel;
  collision_probability:   number | null;
  collision_type:          string;
  altitude_km_1:           number | null;
  altitude_km_2:           number | null;
  detection_timestamp:     string;
  approach_pos_x?:         number | null;
  approach_pos_y?:         number | null;
  approach_pos_z?:         number | null;
}

export interface CollisionListResponse {
  count:       number;
  total_count: number;
  page:        number;
  per_page:    number;
  total_pages: number;
  collisions:  Collision[];
}

export interface HighRiskResponse {
  count:                 number;
  high_risk_collisions:  Collision[];
}

export interface DebrisObject {
  norad_id:    string;
  name:        string;
  country:     string;
  launch:      string;
  period:      number | null;
  inclination: number | null;
  apogee:      number | null;
  perigee:     number | null;
  rcs_size:    string;
}

export interface DebrisListResponse {
  count:  number;
  debris: DebrisObject[];
}

// ── Globe-specific types ──

export interface GlobeCollision {
  norad_id_1:            string;
  norad_id_2:            string;
  type_1:                string;
  type_2:                string;
  distance_km:           number | null;
  risk_level:            RiskLevel;
  collision_probability: number | null;
  collision_type:        string;
  relative_velocity_kms: number | null;
  epoch_1:               string;
  detection_timestamp:   string;
  approach_position_x:   number | null;
  approach_position_y:   number | null;
  approach_position_z:   number | null;
}

export interface GlobeCollisionsResponse {
  count:      number;
  collisions: GlobeCollision[];
}

// ── Reference-compatible types ──

export interface SimulationTimeResponse {
  current_simulated_time: string;
  elapsed_simulated_days: number;
  simulation_mode:        string;
}

/** Reference-style collision record (from /api/collisions/all) */
export interface RefCollision {
  satellite_1_id:        string;
  satellite_2_id:        string;
  satellite_1_name:      string;
  satellite_2_name:      string;
  miss_distance_km:      number | null;
  relative_velocity_kms: number | null;
  risk_level:            RiskLevel;
  collision_probability: number | null;
  predicted_time:        string;
  collision_type:        string;
  is_active:             boolean;
  // original fields also present
  norad_id_1:            string;
  norad_id_2:            string;
  distance_km:           number | null;
  approach_position_x:   number | null;
  approach_position_y:   number | null;
  approach_position_z:   number | null;
}

export interface RefCollisionListResponse {
  count:          number;
  total_count:    number;
  page:           number;
  per_page:       number;
  total_pages:    number;
  collisions:     RefCollision[];
  simulated_time: string;
}

export interface CollisionPair {
  satellite_1_id:   string;
  satellite_2_id:   string;
  satellite_1_name: string;
  satellite_2_name: string;
  approach_events:  number;
  collision_count:  number;
  min_distance_km:  number | null;
  avg_distance_km:  number | null;
  max_distance_km:  number | null;
  risk_level:       RiskLevel;
}

export interface CollisionFrequencyResponse {
  count: number;
  pairs: CollisionPair[];
}
