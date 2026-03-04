import {
  ShieldAlert, AlertTriangle, Shield, ShieldCheck,
  Satellite, Crosshair, Layers, Clock, RefreshCw,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from 'recharts';
import {
  useStats, useHighRiskCollisions,
  useSimulationTime, useCollisionFrequency,
} from '../hooks/useQueries';
import StatCard from '../components/ui/StatCard';
import RiskBadge from '../components/ui/RiskBadge';
import ChartCard from '../components/ui/ChartCard';
import EmptyState from '../components/ui/EmptyState';
import { StatCardSkeleton, ChartSkeleton } from '../components/ui/Skeleton';
import { formatDistance, formatNumber, formatDateShort } from '../utils/formatters';
import { RISK_COLORS } from '../utils/constants';
import type { RiskLevel } from '../api/types';

export default function CommandCenter() {
  const { data: stats, isLoading: statsLoading } = useStats();
  const { data: highRisk, isLoading: hrLoading }  = useHighRiskCollisions();
  const { data: simTime }                          = useSimulationTime();
  const { data: freqData, isLoading: freqLoading } = useCollisionFrequency(20);

  const riskChartData = stats
    ? [
        { name: 'Critical', value: stats.critical_risk_collisions, color: RISK_COLORS.CRITICAL },
        { name: 'High',     value: stats.high_risk_collisions,     color: RISK_COLORS.HIGH },
        { name: 'Medium',   value: stats.medium_risk_collisions,   color: RISK_COLORS.MEDIUM },
        { name: 'Low',      value: stats.low_risk_collisions,      color: RISK_COLORS.LOW },
      ]
    : [];

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Simulation time bar */}
      {simTime && (
        <div className="flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-blue/10 border border-accent-blue/20 text-xs text-accent-blue">
          <Clock className="w-3.5 h-3.5" />
          <span>
            System Time: <span className="font-mono font-semibold">
              {new Date(simTime.current_simulated_time).toUTCString()}
            </span>
          </span>
          <RefreshCw className="w-3 h-3 ml-auto animate-spin opacity-40" />
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-text-primary">Command Center</h2>
          <p className="text-sm text-text-muted mt-0.5">Real-time space debris collision monitoring</p>
        </div>
        <Link to="/collisions" className="text-xs text-accent-blue hover:underline">
          View all alerts →
        </Link>
      </div>

      {/* KPI Row */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {statsLoading ? (
          Array.from({ length: 6 }).map((_, i) => <StatCardSkeleton key={i} />)
        ) : (
          <>
            <StatCard icon={ShieldAlert}  label="Critical"          value={stats?.critical_risk_collisions ?? 0}
              iconColor="text-risk-critical" iconBg="bg-risk-critical/10"
              glow={stats?.critical_risk_collisions ? 'card-glow-critical' : ''} />
            <StatCard icon={AlertTriangle} label="High Risk"         value={stats?.high_risk_collisions ?? 0}
              iconColor="text-risk-high"     iconBg="bg-risk-high/10"
              glow={stats?.high_risk_collisions ? 'card-glow-high' : ''} />
            <StatCard icon={Shield}        label="Medium Risk"       value={stats?.medium_risk_collisions ?? 0}
              iconColor="text-risk-medium"   iconBg="bg-risk-medium/10" />
            <StatCard icon={ShieldCheck}   label="Low Risk"          value={stats?.low_risk_collisions ?? 0}
              iconColor="text-risk-low"      iconBg="bg-risk-low/10" />
            <StatCard icon={Satellite}     label="Tracked Objects"  value={formatNumber(stats?.total_debris_objects ?? 0)}
              iconColor="text-accent-blue"   iconBg="bg-accent-blue/10" />
            <StatCard icon={Layers}        label="Total Pairs"      value={formatNumber(stats?.total_collision_pairs ?? 0)}
              iconColor="text-accent-cyan"   iconBg="bg-accent-cyan/10" />
          </>
        )}
      </div>

      {/* Closest Approach + Risk Pie */}
      <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
        {/* Closest approach */}
        <div className="lg:col-span-3">
          {statsLoading ? (
            <ChartSkeleton />
          ) : stats?.closest_approach ? (
            <div className={`bg-bg-card border rounded-xl p-6 ${
              stats.closest_approach.risk_level === 'CRITICAL' ? 'border-risk-critical/30 card-glow-critical'
              : stats.closest_approach.risk_level === 'HIGH'   ? 'border-risk-high/30 card-glow-high'
              : 'border-border-primary'}`}>
              <div className="flex items-center gap-2 mb-4">
                <Crosshair className="w-4 h-4 text-risk-critical" />
                <h3 className="text-sm font-semibold text-text-primary uppercase tracking-wider">
                  Closest Approach — Threat Highlight
                </h3>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                <div>
                  <p className="text-xs text-text-muted mb-1">Object Pair (NORAD)</p>
                  <p className="text-sm font-mono text-text-primary">{stats.closest_approach.norad_id_1}</p>
                  <p className="text-xs text-text-muted my-0.5">vs</p>
                  <p className="text-sm font-mono text-text-primary">{stats.closest_approach.norad_id_2}</p>
                </div>
                <div>
                  <p className="text-xs text-text-muted mb-1">Miss Distance</p>
                  <p className="text-2xl font-bold font-mono text-text-primary">
                    {formatDistance(stats.closest_approach.distance_km)}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-text-muted mb-1">Epoch</p>
                  <p className="text-sm font-mono text-text-primary">
                    {formatDateShort(stats.closest_approach.epoch_1)}
                  </p>
                </div>
                <div>
                  <p className="text-xs text-text-muted mb-1">Risk Level</p>
                  <RiskBadge level={stats.closest_approach.risk_level as RiskLevel} />
                  <p className="text-xs text-text-secondary mt-1">{stats.closest_approach.collision_type}</p>
                </div>
              </div>
            </div>
          ) : (
            <ChartCard title="Closest Approach">
              <EmptyState title="No collision data yet" message="Run CollisionDetector.scala first" />
            </ChartCard>
          )}
        </div>

        {/* Risk Pie Chart */}
        <div className="lg:col-span-2">
          <ChartCard title="Risk Distribution" subtitle="All detected collision pairs">
            {statsLoading ? (
              <div className="h-48 animate-pulse bg-border-primary/20 rounded-lg" />
            ) : riskChartData.some((d) => d.value > 0) ? (
              <ResponsiveContainer width="100%" height={200}>
                <PieChart>
                  <Pie data={riskChartData} dataKey="value" cx="50%" cy="50%" outerRadius={75} strokeWidth={0}>
                    {riskChartData.map((entry) => (
                      <Cell key={entry.name} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{ background: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px', color: '#f1f5f9', fontSize: 12 }}
                    formatter={(v: any, name: any) => [v.toLocaleString(), name]}
                  />
                </PieChart>
              </ResponsiveContainer>
            ) : (
              <EmptyState title="No risk data" />
            )}
            {/* Legend */}
            <div className="flex flex-wrap gap-3 mt-2">
              {riskChartData.map((d) => (
                <div key={d.name} className="flex items-center gap-1.5 text-xs text-text-secondary">
                  <span className="w-2 h-2 rounded-full" style={{ background: d.color }} />
                  {d.name}: <span className="font-mono text-text-primary">{d.value.toLocaleString()}</span>
                </div>
              ))}
            </div>
          </ChartCard>
        </div>
      </div>

      {/* High-risk table */}
      <ChartCard title="Active Threats" subtitle="Top high-risk / critical collision pairs">
        {hrLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-10 animate-pulse bg-border-primary/20 rounded" />
            ))}
          </div>
        ) : (highRisk?.high_risk_collisions?.length ?? 0) > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border-primary text-text-muted uppercase tracking-wider">
                  <th className="text-left pb-2 pr-4">NORAD 1</th>
                  <th className="text-left pb-2 pr-4">NORAD 2</th>
                  <th className="text-left pb-2 pr-4">Type</th>
                  <th className="text-left pb-2 pr-4">Distance</th>
                  <th className="text-left pb-2 pr-4">Probability</th>
                  <th className="text-left pb-2">Risk</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-primary/40">
                {highRisk!.high_risk_collisions.slice(0, 10).map((c, i) => (
                  <tr key={i} className="hover:bg-bg-surface-hover/50 transition-colors">
                    <td className="py-2.5 pr-4 font-mono text-accent-cyan">{c.norad_id_1}</td>
                    <td className="py-2.5 pr-4 font-mono text-accent-cyan">{c.norad_id_2}</td>
                    <td className="py-2.5 pr-4 text-text-secondary">{c.collision_type}</td>
                    <td className="py-2.5 pr-4 font-mono text-text-primary">{formatDistance(c.distance_km)}</td>
                    <td className="py-2.5 pr-4 font-mono text-text-secondary">
                      {c.collision_probability != null ? c.collision_probability.toExponential(2) : '—'}
                    </td>
                    <td className="py-2.5"><RiskBadge level={c.risk_level as RiskLevel} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="No high-risk pairs" message="Run CollisionDetector.scala to detect collision candidates" />
        )}
      </ChartCard>

      {/* Recurring pairs table — mirrors reference dashboard */}
      <ChartCard title="Recurring Close-Approach Pairs" subtitle="Same object pairs detected multiple times">
        {freqLoading ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="h-9 animate-pulse bg-border-primary/20 rounded" />
            ))}
          </div>
        ) : (freqData?.pairs?.length ?? 0) > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border-primary text-text-muted uppercase tracking-wider">
                  <th className="text-left pb-2 pr-4">Object 1</th>
                  <th className="text-left pb-2 pr-4">Object 2</th>
                  <th className="text-right pb-2 pr-4">Events</th>
                  <th className="text-right pb-2 pr-4">Min Dist (km)</th>
                  <th className="text-right pb-2 pr-4">Avg Dist (km)</th>
                  <th className="text-left pb-2">Risk</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border-primary/40">
                {freqData!.pairs.map((p, i) => (
                  <tr key={i} className="hover:bg-bg-surface-hover/50 transition-colors">
                    <td className="py-2.5 pr-4 font-mono text-accent-cyan">{p.satellite_1_name}</td>
                    <td className="py-2.5 pr-4 font-mono text-accent-cyan">{p.satellite_2_name}</td>
                    <td className="py-2.5 pr-4 font-mono text-text-primary text-right">{p.collision_count}</td>
                    <td className="py-2.5 pr-4 font-mono text-text-primary text-right">{p.min_distance_km?.toFixed(3) ?? '—'}</td>
                    <td className="py-2.5 pr-4 font-mono text-text-secondary text-right">{p.avg_distance_km?.toFixed(3) ?? '—'}</td>
                    <td className="py-2.5"><RiskBadge level={p.risk_level as RiskLevel} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState title="No recurring pairs" message="Pairs appear here once collision data is loaded" />
        )}
      </ChartCard>
    </div>
  );
}
