import { useState, useEffect } from 'react';
import { Wifi, WifiOff, RefreshCw, Satellite } from 'lucide-react';
import { useHealth, useStats } from '../../hooks/useQueries';

export default function TopBar() {
  const { data: health, isError } = useHealth();
  const { data: stats } = useStats();
  const [now, setNow] = useState(new Date());

  useEffect(() => {
    const t = setInterval(() => setNow(new Date()), 1000);
    return () => clearInterval(t);
  }, []);

  const isConnected = health?.status === 'healthy' && !isError;

  return (
    <header className="h-[56px] border-b border-border-primary bg-bg-secondary/80 backdrop-blur-sm flex items-center justify-between px-6 flex-shrink-0">
      {/* Left: live clock + total debris */}
      <div className="flex items-center gap-6">
        <div className="flex items-center gap-2">
          <Satellite className="w-4 h-4 text-accent-cyan" />
          <span className="text-sm font-mono text-text-primary font-medium">
            {now.toUTCString().slice(0, 25)} UTC
          </span>
        </div>
        {stats && (
          <span className="hidden md:flex items-center gap-1.5 px-2 py-1 bg-bg-surface rounded text-xs text-text-muted">
            <span className="w-1.5 h-1.5 rounded-full bg-accent-indigo animate-pulse-glow" />
            {stats.total_debris_objects.toLocaleString()} tracked objects
          </span>
        )}
      </div>

      {/* Right: connection + refresh */}
      <div className="flex items-center gap-4">
        <div className="flex items-center gap-1.5 text-xs">
          {isConnected ? (
            <>
              <Wifi className="w-3.5 h-3.5 text-risk-low" />
              <span className="text-risk-low">API Online</span>
            </>
          ) : (
            <>
              <WifiOff className="w-3.5 h-3.5 text-risk-critical" />
              <span className="text-risk-critical">API Offline</span>
            </>
          )}
        </div>
        <div className="flex items-center gap-1 text-xs text-text-muted">
          <RefreshCw className="w-3.5 h-3.5" />
          <span>30 s</span>
        </div>
        <div className={`text-xs px-2 py-1 rounded font-mono ${isConnected ? 'bg-risk-low/10 text-risk-low' : 'bg-risk-critical/10 text-risk-critical'}`}>
          HDFS: {health?.hdfs ?? '—'}
        </div>
      </div>
    </header>
  );
}
