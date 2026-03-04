import {
  Server, Database, CheckCircle2, XCircle, Satellite, Cpu,
} from 'lucide-react';
import { useHealth, useStats } from '../hooks/useQueries';
import StatCard from '../components/ui/StatCard';
import StatusDot from '../components/ui/StatusDot';
import ChartCard from '../components/ui/ChartCard';
import { StatCardSkeleton } from '../components/ui/Skeleton';
import { formatNumber, formatDistance } from '../utils/formatters';

export default function SystemStatus() {
  const { data: health, isLoading: healthLoading, isError } = useHealth();
  const { data: stats } = useStats();

  const apiOk  = health?.status === 'healthy' && !isError;
  const hdfsOk = health?.hdfs === 'connected';

  return (
    <div className="space-y-6 animate-fade-in">
      <div>
        <h2 className="text-xl font-bold text-text-primary">System Status</h2>
        <p className="text-sm text-text-muted mt-0.5">Infrastructure health & pipeline metrics</p>
      </div>

      {/* Health cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {healthLoading ? (
          Array.from({ length: 4 }).map((_, i) => <StatCardSkeleton key={i} />)
        ) : (
          <>
            {/* Dashboard API */}
            <div className="bg-bg-card border border-border-primary rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Server className="w-4 h-4 text-accent-blue" />
                  <span className="text-sm font-semibold text-text-primary">Dashboard API</span>
                </div>
                <StatusDot status={apiOk ? 'online' : 'offline'} />
              </div>
              <div className="space-y-1.5 text-xs">
                <div className="flex items-center gap-2">
                  {apiOk ? <CheckCircle2 className="w-3.5 h-3.5 text-risk-low" /> : <XCircle className="w-3.5 h-3.5 text-risk-critical" />}
                  <span className="text-text-secondary">Flask API: {health?.status ?? 'unknown'}</span>
                </div>
                <div className="flex items-center gap-2 text-text-muted">
                  <span className="font-mono">{health?.timestamp?.slice(0,19).replace('T',' ') ?? '—'}</span>
                </div>
              </div>
            </div>

            {/* HDFS */}
            <div className="bg-bg-card border border-border-primary rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Database className="w-4 h-4 text-accent-cyan" />
                  <span className="text-sm font-semibold text-text-primary">HDFS NameNode</span>
                </div>
                <StatusDot status={hdfsOk ? 'online' : 'offline'} />
              </div>
              <div className="space-y-1.5 text-xs">
                <div className="flex items-center gap-2">
                  {hdfsOk ? <CheckCircle2 className="w-3.5 h-3.5 text-risk-low" /> : <XCircle className="w-3.5 h-3.5 text-risk-critical" />}
                  <span className="text-text-secondary">WebHDFS: {health?.hdfs ?? 'unknown'}</span>
                </div>
                <p className="text-text-muted">Port 9870 — /space-debris-webhdfs</p>
              </div>
            </div>

            {/* Spark / Kafka */}
            <div className="bg-bg-card border border-border-primary rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Cpu className="w-4 h-4 text-risk-medium" />
                  <span className="text-sm font-semibold text-text-primary">Spark + Kafka</span>
                </div>
                <StatusDot status="warning" pulse={false} />
              </div>
              <div className="space-y-1.5 text-xs text-text-muted">
                <p>Kafka broker — localhost:19092</p>
                <p>Spark 3.5.0 — local[*]</p>
                <p>Topic: tle-raw</p>
              </div>
            </div>

            {/* Debris catalog */}
            <div className="bg-bg-card border border-border-primary rounded-xl p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-2">
                  <Satellite className="w-4 h-4 text-risk-low" />
                  <span className="text-sm font-semibold text-text-primary">Debris Catalog</span>
                </div>
                <StatusDot status={stats?.total_debris_objects ? 'online' : 'offline'} />
              </div>
              <div className="space-y-1.5 text-xs">
                <p className="text-text-secondary">{formatNumber(stats?.total_debris_objects ?? 0)} objects tracked</p>
                <p className="text-text-muted">Source: Space-Track.org</p>
              </div>
            </div>
          </>
        )}
      </div>

      {/* Pipeline summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatCard icon={Satellite} label="Total Collision Pairs" value={formatNumber(stats?.total_collision_pairs ?? 0)} iconColor="text-accent-blue" iconBg="bg-accent-blue/10" />
        <StatCard icon={Server}    label="Min Distance"          value={formatDistance(stats?.min_distance_km ?? null)} iconColor="text-risk-critical" iconBg="bg-risk-critical/10" />
        <StatCard icon={Database}  label="Avg Distance"          value={formatDistance(stats?.avg_distance_km ?? null)} iconColor="text-risk-medium"   iconBg="bg-risk-medium/10" />
      </div>

      {/* Pipeline description */}
      <ChartCard title="Pipeline Architecture" subtitle="End-to-end data flow">
        <div className="font-mono text-xs text-text-secondary space-y-2 p-2">
          {[
            { step: '1. debris.py',          desc: 'Download debris catalog from Space-Track → Output/space_debris_catalog.csv' },
            { step: '2. tle_stream_producer.py', desc: 'Fetch TLE history → publish JSON to Kafka topic: tle-raw' },
            { step: '3. TLEStreamProcessor',  desc: 'Spark Structured Streaming: Kafka → Orekit SGP4 → HDFS state-vectors' },
            { step: '4. TLEProcessor',        desc: 'Batch Spark: local TLE CSVs → Orekit SGP4 → HDFS state-vectors' },
            { step: '5. CollisionDetector',   desc: 'Spark cross-join: state-vectors → pairwise distances → HDFS collision-alerts' },
            { step: '6. dashboard_api.py',    desc: 'Flask API: reads HDFS CSVs → serves REST endpoints → this dashboard' },
          ].map(({ step, desc }) => (
            <div key={step} className="flex gap-4">
              <span className="text-accent-cyan w-52 flex-shrink-0">{step}</span>
              <span>{desc}</span>
            </div>
          ))}
        </div>
      </ChartCard>

      {/* Quick commands */}
      <ChartCard title="Quick Commands" subtitle="Run these in your terminal to operate the pipeline">
        <div className="space-y-2 font-mono text-xs">
          {[
            { label: 'Start Infrastructure',    cmd: 'docker-compose up -d broker namenode' },
            { label: 'Download catalog',         cmd: 'python debris.py' },
            { label: 'Stream TLEs to Kafka',    cmd: 'N_DEBRIS=100 python tle_stream_producer.py' },
            { label: 'Run Spark stream processor', cmd: 'sbt "runMain TLEStreamProcessor"' },
            { label: 'Run batch TLE processor',  cmd: 'N_DEBRIS=200 sbt "runMain TLEProcessor"' },
            { label: 'Run collision detector',   cmd: 'sbt "runMain CollisionDetector"' },
            { label: 'Start this API',           cmd: 'python dashboard_api.py' },
          ].map(({ label, cmd }) => (
            <div key={label} className="flex items-start gap-4 bg-bg-surface rounded-lg px-3 py-2">
              <span className="text-text-muted w-44 flex-shrink-0"># {label}</span>
              <span className="text-accent-cyan">{cmd}</span>
            </div>
          ))}
        </div>
      </ChartCard>
    </div>
  );
}
