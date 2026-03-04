import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ScatterChart, Scatter, Cell,
} from 'recharts';
import { useCollisions, useStats } from '../hooks/useQueries';
import ChartCard from '../components/ui/ChartCard';
import EmptyState from '../components/ui/EmptyState';
import { RISK_COLORS } from '../utils/constants';
import type { RiskLevel } from '../api/types';

export default function Analytics() {
  const { data: collisions } = useCollisions({ page: 1, per_page: 200, sort_by: 'distance_km', sort_order: 'asc' });
  const { data: stats } = useStats();

  // Distance histogram
  const distanceBuckets = (() => {
    if (!collisions?.collisions) return [];
    const buckets = [
      { range: '0-1 km',  min: 0,  max: 1,   count: 0 },
      { range: '1-5 km',  min: 1,  max: 5,   count: 0 },
      { range: '5-20 km', min: 5,  max: 20,  count: 0 },
      { range: '20-50',   min: 20, max: 50,  count: 0 },
      { range: '50-100',  min: 50, max: 100, count: 0 },
    ];
    collisions.collisions.forEach((c) => {
      if (c.distance_km == null) return;
      const b = buckets.find((b) => c.distance_km! >= b.min && c.distance_km! < b.max);
      if (b) b.count++;
    });
    return buckets;
  })();

  // Scatter: distance vs velocity coloured by risk
  const scatter = collisions?.collisions
    ? collisions.collisions.slice(0, 150).map((c) => ({
        distance: c.distance_km,
        velocity: c.relative_velocity_kms,
        risk:     c.risk_level as RiskLevel,
      })).filter((d) => d.distance != null && d.velocity != null)
    : [];

  // Risk bar chart
  const riskBar = stats
    ? [
        { name: 'Critical', count: stats.critical_risk_collisions, fill: RISK_COLORS.CRITICAL },
        { name: 'High',     count: stats.high_risk_collisions,     fill: RISK_COLORS.HIGH },
        { name: 'Medium',   count: stats.medium_risk_collisions,   fill: RISK_COLORS.MEDIUM },
        { name: 'Low',      count: stats.low_risk_collisions,      fill: RISK_COLORS.LOW },
      ]
    : [];

  const tooltipStyle = { background: '#0f172a', border: '1px solid #1e293b', borderRadius: '8px', color: '#f1f5f9', fontSize: 12 };

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-bold text-text-primary">Analytics</h2>
        <p className="text-sm text-text-muted mt-0.5">Statistical analysis of detected collision pairs</p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Risk breakdown bar */}
        <ChartCard title="Collision Count by Risk Level" subtitle="Total pairs per risk category">
          {riskBar.some((r) => r.count > 0) ? (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={riskBar} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="name" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <Tooltip contentStyle={tooltipStyle} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]} barSize={40}>
                  {riskBar.map((r) => <Cell key={r.name} fill={r.fill} />)}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState title="No risk data" />
          )}
        </ChartCard>

        {/* Distance distribution */}
        <ChartCard title="Miss Distance Distribution" subtitle="Histogram of approach distances">
          {distanceBuckets.some((b) => b.count > 0) ? (
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={distanceBuckets} margin={{ top: 5, right: 10, bottom: 5, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="range" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <Tooltip contentStyle={tooltipStyle} />
                <Bar dataKey="count" radius={[4, 4, 0, 0]} barSize={40}>
                  {distanceBuckets.map((_, i) => (
                    <Cell key={i} fill={i === 0 ? RISK_COLORS.CRITICAL : i === 1 ? RISK_COLORS.HIGH : i < 3 ? RISK_COLORS.MEDIUM : RISK_COLORS.LOW} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState title="No distance data" />
          )}
        </ChartCard>

        {/* Distance vs Velocity scatter */}
        <ChartCard title="Distance vs. Relative Velocity" subtitle="Approach parameters, coloured by risk" className="lg:col-span-2">
          {scatter.length > 0 ? (
            <ResponsiveContainer width="100%" height={260}>
              <ScatterChart margin={{ top: 5, right: 20, bottom: 5, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
                <XAxis dataKey="distance" name="Distance" unit=" km" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <YAxis dataKey="velocity" name="Velocity" unit=" km/s" tick={{ fill: '#94a3b8', fontSize: 11 }} axisLine={{ stroke: '#1e293b' }} />
                <Tooltip
                  contentStyle={tooltipStyle}
                  formatter={(v: any, n: any) => [v, n]}
                />
                <Scatter data={scatter} shape="circle">
                  {scatter.map((s, i) => (
                    <Cell key={i} fill={RISK_COLORS[s.risk]} fillOpacity={0.7} />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          ) : (
            <EmptyState title="No scatter data" />
          )}
          {/* Legend */}
          <div className="flex gap-4 mt-3">
            {(['CRITICAL','HIGH','MEDIUM','LOW'] as RiskLevel[]).map((r) => (
              <div key={r} className="flex items-center gap-1.5 text-xs text-text-muted">
                <span className="w-2 h-2 rounded-full" style={{ background: RISK_COLORS[r] }} />
                {r}
              </div>
            ))}
          </div>
        </ChartCard>
      </div>
    </div>
  );
}
