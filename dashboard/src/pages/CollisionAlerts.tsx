import { useState } from 'react';
import { Download, Filter, ChevronLeft, ChevronRight } from 'lucide-react';
import { useCollisions } from '../hooks/useQueries';
import RiskBadge from '../components/ui/RiskBadge';
import EmptyState from '../components/ui/EmptyState';
import { TableSkeleton } from '../components/ui/Skeleton';
import { formatDistance, formatVelocity, formatProbability, formatDateShort } from '../utils/formatters';
import type { RiskLevel } from '../api/types';

const RISK_FILTERS = ['ALL', 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'] as const;
const SORT_OPTIONS = [
  { label: 'Distance ↑',   value: 'distance_km',          order: 'asc'  as const },
  { label: 'Distance ↓',   value: 'distance_km',          order: 'desc' as const },
  { label: 'Probability ↓',value: 'collision_probability', order: 'desc' as const },
  { label: 'Detected ↓',   value: 'detection_timestamp',  order: 'desc' as const },
];
const PAGE_SIZES = [25, 50, 100];

export default function CollisionAlerts() {
  const [page,       setPage]       = useState(1);
  const [perPage,    setPerPage]    = useState(50);
  const [riskFilter, setRiskFilter] = useState<string>('ALL');
  const [sortIdx,    setSortIdx]    = useState(0);

  const sort = SORT_OPTIONS[sortIdx];
  const { data, isLoading } = useCollisions({
    page,
    per_page:   perPage,
    risk_level: riskFilter === 'ALL' ? undefined : riskFilter,
    sort_by:    sort.value,
    sort_order: sort.order,
  });

  const handleExport = () => {
    if (!data?.collisions) return;
    const headers = ['NORAD 1','NORAD 2','Type','Distance (km)','Velocity (km/s)','Probability','Risk','Epoch 1','Detected'];
    const rows = data.collisions.map((c) => [
      c.norad_id_1, c.norad_id_2, c.collision_type,
      c.distance_km, c.relative_velocity_kms, c.collision_probability,
      c.risk_level, c.epoch_1, c.detection_timestamp,
    ]);
    const csv  = [headers, ...rows].map((r) => r.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = `collision_alerts_${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-bold text-text-primary">Collision Alerts</h2>
          <p className="text-sm text-text-muted mt-0.5">
            {data ? `${data.total_count.toLocaleString()} total alerts` : 'Loading…'}
          </p>
        </div>
        <button
          onClick={handleExport}
          className="flex items-center gap-2 px-3 py-2 bg-bg-surface border border-border-primary rounded-lg text-xs text-text-secondary hover:text-text-primary hover:border-border-secondary transition-colors"
        >
          <Download className="w-3.5 h-3.5" /> Export CSV
        </button>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-4 bg-bg-card border border-border-primary rounded-xl px-4 py-3">
        <div className="flex items-center gap-2">
          <Filter className="w-3.5 h-3.5 text-text-muted" />
          <span className="text-xs text-text-muted">Risk:</span>
          <div className="flex gap-1">
            {RISK_FILTERS.map((f) => (
              <button key={f}
                onClick={() => { setRiskFilter(f); setPage(1); }}
                className={`text-xs px-2.5 py-1 rounded-lg transition-colors ${
                  riskFilter === f
                    ? 'bg-accent-blue/15 text-accent-blue border border-accent-blue/30'
                    : 'text-text-muted hover:text-text-secondary hover:bg-bg-surface-hover border border-transparent'
                }`}
              >{f}</button>
            ))}
          </div>
        </div>
        <div className="flex items-center gap-2 ml-auto">
          <span className="text-xs text-text-muted">Sort:</span>
          <select value={sortIdx} onChange={(e) => { setSortIdx(Number(e.target.value)); setPage(1); }}
            className="bg-bg-surface border border-border-primary rounded-lg px-2 py-1 text-xs text-text-primary outline-none">
            {SORT_OPTIONS.map((s, i) => <option key={i} value={i}>{s.label}</option>)}
          </select>
          <span className="text-xs text-text-muted ml-2">Per page:</span>
          <select value={perPage} onChange={(e) => { setPerPage(Number(e.target.value)); setPage(1); }}
            className="bg-bg-surface border border-border-primary rounded-lg px-2 py-1 text-xs text-text-primary outline-none">
            {PAGE_SIZES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </div>
      </div>

      {/* Table */}
      {isLoading ? (
        <TableSkeleton rows={10} />
      ) : (data?.collisions?.length ?? 0) > 0 ? (
        <div className="bg-bg-card border border-border-primary rounded-xl overflow-hidden">
          <div className="overflow-x-auto max-h-[calc(100vh-360px)]">
            <table className="w-full text-xs">
              <thead className="sticky top-0 z-10">
                <tr className="bg-bg-surface border-b border-border-primary">
                  {['NORAD 1','NORAD 2','Type','Distance','Velocity','Probability','Risk','Altitude 1','Epoch','Detected'].map((h) => (
                    <th key={h} className="text-left px-3 py-3 text-text-muted uppercase tracking-wider font-medium">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-border-primary/40">
                {data!.collisions.map((c, i) => (
                  <tr key={i} className="hover:bg-bg-surface-hover/50 transition-colors">
                    <td className="px-3 py-2.5 font-mono text-accent-cyan">{c.norad_id_1}</td>
                    <td className="px-3 py-2.5 font-mono text-accent-cyan">{c.norad_id_2}</td>
                    <td className="px-3 py-2.5 text-text-secondary">{c.collision_type || '—'}</td>
                    <td className="px-3 py-2.5 font-mono text-text-primary">{formatDistance(c.distance_km)}</td>
                    <td className="px-3 py-2.5 font-mono text-text-secondary">{formatVelocity(c.relative_velocity_kms)}</td>
                    <td className="px-3 py-2.5 font-mono text-text-secondary">{formatProbability(c.collision_probability)}</td>
                    <td className="px-3 py-2.5"><RiskBadge level={c.risk_level as RiskLevel} /></td>
                    <td className="px-3 py-2.5 font-mono text-text-muted">
                      {c.altitude_km_1 != null ? `${c.altitude_km_1.toFixed(0)} km` : '—'}
                    </td>
                    <td className="px-3 py-2.5 font-mono text-text-muted">{formatDateShort(c.epoch_1)}</td>
                    <td className="px-3 py-2.5 font-mono text-text-muted">{formatDateShort(c.detection_timestamp)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {/* Pagination */}
          <div className="flex items-center justify-between px-4 py-3 border-t border-border-primary">
            <span className="text-xs text-text-muted">
              Page {data!.page} / {data!.total_pages} — {data!.total_count.toLocaleString()} total
            </span>
            <div className="flex items-center gap-2">
              <button disabled={page === 1} onClick={() => setPage((p) => p - 1)}
                className="p-1 rounded hover:bg-bg-surface-hover disabled:opacity-40 transition-colors">
                <ChevronLeft className="w-4 h-4 text-text-secondary" />
              </button>
              <button disabled={page >= data!.total_pages} onClick={() => setPage((p) => p + 1)}
                className="p-1 rounded hover:bg-bg-surface-hover disabled:opacity-40 transition-colors">
                <ChevronRight className="w-4 h-4 text-text-secondary" />
              </button>
            </div>
          </div>
        </div>
      ) : (
        <EmptyState title="No collision alerts" message="Run CollisionDetector.scala to generate alerts" />
      )}
    </div>
  );
}
