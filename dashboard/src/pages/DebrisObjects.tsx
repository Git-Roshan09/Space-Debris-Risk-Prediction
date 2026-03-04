import { useState } from 'react';
import { useDebris, useDebrisById } from '../hooks/useQueries';
import StatCard from '../components/ui/StatCard';
import EmptyState from '../components/ui/EmptyState';
import { TableSkeleton, StatCardSkeleton } from '../components/ui/Skeleton';
import { formatNumber, formatAltitude } from '../utils/formatters';
import { Globe2, Orbit, X } from 'lucide-react';

export default function DebrisObjects() {
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const { data, isLoading } = useDebris(2000);
  const { data: detail } = useDebrisById(selectedId);

  const byCountry = data?.debris
    ? Object.entries(
        data.debris.reduce<Record<string, number>>((acc, d) => {
          acc[d.country || 'UNK'] = (acc[d.country || 'UNK'] || 0) + 1;
          return acc;
        }, {}),
      ).sort((a, b) => b[1] - a[1]).slice(0, 5)
    : [];

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-bold text-text-primary">Debris Objects</h2>
        <p className="text-sm text-text-muted mt-0.5">Catalog of tracked space debris from Space-Track</p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {isLoading ? (
          Array.from({ length: 4 }).map((_, i) => <StatCardSkeleton key={i} />)
        ) : (
          <>
            <StatCard icon={Globe2}  label="Total Objects"   value={formatNumber(data?.count ?? 0)} iconColor="text-accent-blue"  iconBg="bg-accent-blue/10" />
            <StatCard icon={Orbit}   label="Countries"        value={formatNumber(new Set(data?.debris.map((d) => d.country)).size)} iconColor="text-accent-cyan" iconBg="bg-accent-cyan/10" />
            <StatCard icon={Globe2}  label="Top Origin"      value={byCountry[0]?.[0] ?? '—'}       iconColor="text-risk-medium" iconBg="bg-risk-medium/10" />
            <StatCard icon={Orbit}   label="Top Origin Count" value={byCountry[0]?.[1] ?? 0}        iconColor="text-risk-high"   iconBg="bg-risk-high/10" />
          </>
        )}
      </div>

      {/* Table + detail panel */}
      <div className="flex gap-6">
        <div className="flex-1 min-w-0">
          {isLoading ? (
            <TableSkeleton rows={12} />
          ) : (data?.debris?.length ?? 0) > 0 ? (
            <div className="bg-bg-card border border-border-primary rounded-xl overflow-hidden">
              <div className="overflow-x-auto max-h-[calc(100vh-360px)]">
                <table className="w-full text-xs">
                  <thead className="sticky top-0 z-10">
                    <tr className="bg-bg-surface border-b border-border-primary">
                      {['NORAD ID','Name','Country','Launch','Inclination (°)','Apogee (km)','Perigee (km)','Size'].map((h) => (
                        <th key={h} className="text-left px-3 py-3 text-text-muted uppercase tracking-wider font-medium">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border-primary/40">
                    {data!.debris.map((d) => (
                      <tr key={d.norad_id}
                        onClick={() => setSelectedId(d.norad_id)}
                        className={`cursor-pointer transition-colors ${selectedId === d.norad_id ? 'bg-accent-blue/5' : 'hover:bg-bg-surface-hover/50'}`}>
                        <td className="px-3 py-2.5 font-mono text-accent-cyan">{d.norad_id}</td>
                        <td className="px-3 py-2.5 font-medium text-text-primary max-w-[180px] truncate">{d.name || '—'}</td>
                        <td className="px-3 py-2.5 text-text-secondary">{d.country || '—'}</td>
                        <td className="px-3 py-2.5 text-text-muted font-mono">{d.launch?.slice(0, 10) || '—'}</td>
                        <td className="px-3 py-2.5 font-mono text-text-secondary">{d.inclination?.toFixed(2) ?? '—'}</td>
                        <td className="px-3 py-2.5 font-mono text-text-secondary">{formatAltitude(d.apogee)}</td>
                        <td className="px-3 py-2.5 font-mono text-text-secondary">{formatAltitude(d.perigee)}</td>
                        <td className="px-3 py-2.5 text-text-muted">{d.rcs_size || '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <EmptyState title="No debris catalog" message="Run python debris.py to download the catalog" />
          )}
        </div>

        {/* Detail side panel */}
        {selectedId && detail && (
          <div className="w-[280px] flex-shrink-0 bg-bg-card border border-border-primary rounded-xl p-5">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-sm font-semibold text-text-primary">Debris Detail</h3>
              <button onClick={() => setSelectedId(null)}
                className="p-1 rounded hover:bg-bg-surface-hover text-text-muted hover:text-text-primary transition-colors">
                <X className="w-4 h-4" />
              </button>
            </div>
            <div className="space-y-3 text-xs">
              {Object.entries({
                'NORAD ID':    (detail as any).NORAD_CAT_ID,
                'Name':        (detail as any).OBJECT_NAME,
                'Country':     (detail as any).COUNTRY,
                'Launch':      (detail as any).LAUNCH,
                'Period (min)':(detail as any).PERIOD,
                'Inclination': (detail as any).INCLINATION,
                'Apogee (km)': (detail as any).APOGEE,
                'Perigee (km)':(detail as any).PERIGEE,
                'RCS Size':    (detail as any).RCS_SIZE,
                'Decay':       (detail as any).DECAY,
              }).map(([k, v]) => (
                <div key={k} className="flex justify-between gap-2">
                  <span className="text-text-muted">{k}</span>
                  <span className="text-text-primary font-mono text-right">{v != null ? String(v) : '—'}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
