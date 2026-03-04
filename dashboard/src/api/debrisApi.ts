import apiClient from './client';
import type {
  HealthResponse,
  StatsResponse,
  CollisionListResponse,
  HighRiskResponse,
  DebrisListResponse,
  DebrisObject,
  GlobeCollisionsResponse,
} from './types';

export async function fetchHealth(): Promise<HealthResponse> {
  const { data } = await apiClient.get('/health');
  return data;
}

export async function fetchStats(): Promise<StatsResponse> {
  const { data } = await apiClient.get('/stats');
  return data;
}

export async function fetchCollisions(params: {
  page?: number;
  per_page?: number;
  risk_level?: string;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}): Promise<CollisionListResponse> {
  const { data } = await apiClient.get('/collisions', { params });
  return data;
}

export async function fetchHighRiskCollisions(): Promise<HighRiskResponse> {
  const { data } = await apiClient.get('/collisions/high-risk');
  return data;
}

export async function fetchDebris(limit = 1000): Promise<DebrisListResponse> {
  const { data } = await apiClient.get('/debris', { params: { limit } });
  return data;
}

export async function fetchDebrisById(noradId: string): Promise<DebrisObject> {
  const { data } = await apiClient.get(`/debris/${noradId}`);
  return data;
}

export async function fetchGlobeCollisions(params?: {
  risk_level?: string;
  limit?: number;
}): Promise<GlobeCollisionsResponse> {
  const { data } = await apiClient.get('/collisions/globe', { params });
  return data;
}

export async function fetchDashboardStats(): Promise<StatsResponse> {
  const { data } = await apiClient.get('/dashboard/stats');
  // Map reference field names → local StatsResponse shape
  return {
    total_debris_objects:     data.total_debris_objects     ?? 0,
    critical_risk_collisions: data.critical_risk_collisions ?? 0,
    high_risk_collisions:     data.high_risk_collisions     ?? 0,
    medium_risk_collisions:   data.medium_risk_collisions   ?? 0,
    low_risk_collisions:      data.low_risk_collisions      ?? 0,
    total_collision_pairs:    data.total_collision_pairs    ?? data.total_active_collisions ?? 0,
    min_distance_km:          data.min_distance_km          ?? null,
    avg_distance_km:          data.avg_distance_km          ?? null,
    max_distance_km:          data.max_distance_km          ?? null,
    closest_approach: data.closest_approach ? {
      norad_id_1:     data.closest_approach.satellite_1_id ?? '',
      norad_id_2:     data.closest_approach.satellite_2_id ?? '',
      distance_km:    data.closest_approach.miss_distance_km ?? 0,
      risk_level:     data.closest_approach.risk_level ?? 'LOW',
      epoch_1:        data.closest_approach.predicted_time ?? '',
      collision_type: data.closest_approach.collision_type ?? '',
    } : null,
    timestamp: data.timestamp ?? new Date().toISOString(),
  };
}

export async function fetchSimulationTime(): Promise<{ current_simulated_time: string; elapsed_simulated_days: number }> {
  const { data } = await apiClient.get('/simulation/time');
  return data;
}

export async function fetchCollisionFrequency(limit = 20): Promise<{ count: number; pairs: import('./types').CollisionPair[] }> {
  const { data } = await apiClient.get('/collisions/frequency', { params: { limit } });
  return data;
}

export async function fetchAllCollisions(params: {
  page?: number;
  per_page?: number;
  risk_level?: string;
}): Promise<import('./types').RefCollisionListResponse> {
  const { data } = await apiClient.get('/collisions/all', { params });
  return data;
}
