import { useQuery } from '@tanstack/react-query';
import {
  fetchHealth, fetchStats, fetchCollisions,
  fetchHighRiskCollisions, fetchDebris, fetchDebrisById,
  fetchGlobeCollisions, fetchDashboardStats,
  fetchSimulationTime, fetchCollisionFrequency, fetchAllCollisions,
} from '../api/debrisApi';

const REFETCH = 30_000; // 30 s

export const useHealth = () =>
  useQuery({ queryKey: ['health'], queryFn: fetchHealth, refetchInterval: REFETCH });

export const useStats = () =>
  useQuery({ queryKey: ['stats'], queryFn: fetchStats, refetchInterval: REFETCH });

export const useCollisions = (params: Parameters<typeof fetchCollisions>[0]) =>
  useQuery({
    queryKey: ['collisions', params],
    queryFn:  () => fetchCollisions(params),
    refetchInterval: REFETCH,
  });

export const useHighRiskCollisions = () =>
  useQuery({
    queryKey: ['highRiskCollisions'],
    queryFn:  fetchHighRiskCollisions,
    refetchInterval: REFETCH,
  });

export const useDebris = (limit = 1000) =>
  useQuery({
    queryKey: ['debris', limit],
    queryFn:  () => fetchDebris(limit),
    refetchInterval: 60_000,
  });

export const useDebrisById = (noradId: string | null) =>
  useQuery({
    queryKey: ['debrisDetail', noradId],
    queryFn:  () => fetchDebrisById(noradId!),
    enabled:  !!noradId,
  });

export const useGlobeCollisions = (params?: Parameters<typeof fetchGlobeCollisions>[0]) =>
  useQuery({
    queryKey: ['globeCollisions', params],
    queryFn:  () => fetchGlobeCollisions(params),
    refetchInterval: REFETCH,
  });

export const useDashboardStats = () =>
  useQuery({
    queryKey: ['dashboardStats'],
    queryFn:  fetchDashboardStats,
    refetchInterval: REFETCH,
  });

export const useSimulationTime = () =>
  useQuery({
    queryKey: ['simulationTime'],
    queryFn:  fetchSimulationTime,
    refetchInterval: REFETCH,
  });

export const useCollisionFrequency = (limit = 20) =>
  useQuery({
    queryKey: ['collisionFrequency', limit],
    queryFn:  () => fetchCollisionFrequency(limit),
    refetchInterval: REFETCH,
  });

export const useAllCollisions = (params: Parameters<typeof fetchAllCollisions>[0]) =>
  useQuery({
    queryKey: ['allCollisions', params],
    queryFn:  () => fetchAllCollisions(params),
    refetchInterval: REFETCH,
  });
