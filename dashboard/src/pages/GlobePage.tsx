import { useEffect, useRef, useState, useCallback } from 'react';
import { Globe2, Maximize2, Minimize2, RefreshCw, RotateCcw, AlertTriangle } from 'lucide-react';
import { useGlobeCollisions } from '../hooks/useQueries';
import type { GlobeCollision } from '../api/types';

// ── Constants ────────────────────────────────────────────────────────────────
const EARTH_RADIUS_KM = 6371;
const GM_EARTH = 398600.4418;           // km³/s²
const EARTH_ROTATION_DEG_S = 360 / 86164.1; // sidereal rotation rate deg/s
const ORBIT_SPEED_MULTIPLIER = 1;       // 1× = real orbital speed (no visual flyby)
const GLOBE_RENDER_INTERVAL = 80;       // ms between globe.gl re-renders (~12 fps, smooth)

const RISK_COLORS: Record<string, string> = {
  CRITICAL: '#e040fb',   // purple/magenta  — matches reference
  HIGH:     '#ff5252',   // red
  MEDIUM:   '#ffab40',   // orange
  LOW:      '#69f0ae',   // green
};

const RISK_RING_COLORS: Record<string, [string, string]> = {
  CRITICAL: ['rgba(224,64,251,0.8)',  'rgba(224,64,251,0)'],
  HIGH:     ['rgba(255,82,82,0.6)',   'rgba(255,82,82,0)'],
};

const RISK_RADIUS: Record<string, number> = {
  CRITICAL: 0.5,
  HIGH:     0.4,
  MEDIUM:   0.3,
  LOW:      0.25,
};

type RiskFilter = 'all' | 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW';
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type GlobeInstance = any;

// ── ECI → geodetic (same as reference eciToGeodetic) ─────────────────────────
function eciToGeodetic(x: number, y: number, z: number) {
  const r   = Math.sqrt(x * x + y * y + z * z);
  const lat = Math.atan2(z, Math.sqrt(x * x + y * y)) * (180 / Math.PI);
  const lng = Math.atan2(y, x) * (180 / Math.PI);
  const alt = r - EARTH_RADIUS_KM;
  return { lat, lng, alt };
}

// ── Kepler's 3rd law: orbital angular rate (deg/s) ───────────────────────────
function computeOrbitalRate(altKm: number): number {
  const r = EARTH_RADIUS_KM + Math.max(altKm, 160);
  const period = 2 * Math.PI * Math.sqrt((r * r * r) / GM_EARTH); // seconds
  return 360 / period;
}

// ── Build a globe point from a collision record ──────────────────────────────
function buildGlobePoint(c: GlobeCollision) {
  const pos  = eciToGeodetic(c.approach_position_x!, c.approach_position_y!, c.approach_position_z!);
  const risk = (c.risk_level || 'LOW').toUpperCase();
  const alt  = Math.max(pos.alt, 100);

  // Deterministic pseudo-random from NORAD IDs (stable across refreshes)
  const s1   = parseInt(c.norad_id_1 || '0') || 0;
  const s2   = parseInt(c.norad_id_2 || '0') || 0;
  const seed = s1 * 7 + s2 * 13;
  const rand = ((seed * 9301 + 49297) % 233280) / 233280;

  const inclination = Math.max(Math.abs(pos.lat), 15) + rand * 15;
  const latRatio    = Math.max(-1, Math.min(1, pos.lat / inclination));
  const orbitPhase  = rand > 0.5
    ? Math.asin(latRatio)
    : Math.PI - Math.asin(latRatio);

  return {
    lat:           pos.lat,
    lng:           pos.lng,
    alt:           alt,
    color:         RISK_COLORS[risk] ?? RISK_COLORS.LOW,
    radius:        RISK_RADIUS[risk]  ?? 0.25,
    risk_level:    risk,
    sat1_name:     c.norad_id_1 || '?',
    sat2_name:     c.norad_id_2 || '?',
    miss_distance: c.distance_km  != null ? c.distance_km.toFixed(3)  : '--',
    velocity:      c.relative_velocity_kms != null ? c.relative_velocity_kms.toFixed(3) : '--',
    probability:   c.collision_probability != null
      ? (c.collision_probability * 100).toFixed(4) + '%'
      : '--',
    ringColor:  RISK_RING_COLORS[risk] ?? null,
    maxRadius:  risk === 'CRITICAL' ? 3 : 2,
    // orbital mechanics
    orbitalRate: computeOrbitalRate(alt),
    inclination,
    orbitPhase,
  };
}

// === Main Component ===
export default function GlobePage() {
  const containerRef       = useRef<HTMLDivElement>(null);
  const globeRef           = useRef<GlobeInstance>(null);
  const globeDataRef       = useRef<ReturnType<typeof buildGlobePoint>[]>([]);
  const animFrameRef       = useRef<number>(0);
  const lastAnimTimeRef    = useRef<number | null>(null);
  const lastRenderTimeRef  = useRef<number>(0);

  const [riskFilter, setRiskFilter]   = useState<RiskFilter>('all');
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [globeReady, setGlobeReady]   = useState(false);
  const riskFilterRef = useRef<RiskFilter>('all'); // ref for animation loop

  const { data, isLoading, refetch, dataUpdatedAt } = useGlobeCollisions({ limit: 500 });

  // ── Derived counts ──────────────────────────────────────────────────────────
  const allCollisions = data?.collisions ?? [];
  const counts = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0 };
  allCollisions.forEach((c) => {
    if (c.risk_level in counts) counts[c.risk_level as keyof typeof counts]++;
  });
  const validCollisions = allCollisions.filter(
    (c) => c.approach_position_x != null && c.approach_position_y != null && c.approach_position_z != null,
  );

  // ── Apply risk filter to globeDataRef ─────────────────────────────────────
  const applyFilter = useCallback((filter: RiskFilter) => {
    const g = globeRef.current;
    if (!g) return;
    const filtered = filter === 'all'
      ? globeDataRef.current
      : globeDataRef.current.filter((d) => d.risk_level === filter);

    g.htmlElementsData(filtered);
    const rings = filtered.filter((d) => d.ringColor != null);
    g.ringsData(rings);
  }, []);

  // ── Initialise globe.gl — mirrors reference initGlobe() ──────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    let cancelled = false;

    (async () => {
      const mod = await import('globe.gl');
      if (cancelled || !containerRef.current) return;

      const Globe = (mod as any).default ?? mod;
      const { width, height } = containerRef.current.getBoundingClientRect();

      // Exact same init as reference: new Globe(container, { animateIn: true })
      const g = new Globe(containerRef.current, { animateIn: true })
        .globeImageUrl('/textures/earth-blue-marble.jpg')
        .bumpImageUrl('/textures/earth-topology.png')
        .backgroundImageUrl('/textures/night-sky.png')
        .backgroundColor('#040714')
        .showAtmosphere(true)
        .atmosphereColor('#448aff')
        .atmosphereAltitude(0.18)
        .width(width  || 800)
        .height(height || 600)
        // ── HTML marker layer — renders flat circular dots, no altitude bars ──
        .htmlElementsData([])
        .htmlLat('lat')
        .htmlLng('lng')
        .htmlAltitude(0)            // fixed at surface level, no extrusion
        .htmlElement((d: any) => {
          const el = document.createElement('div');
          const size = d.risk_level === 'CRITICAL' ? 14
                     : d.risk_level === 'HIGH'     ? 12
                     : d.risk_level === 'MEDIUM'   ? 10 : 8;
          el.style.cssText = `
            width: ${size}px;
            height: ${size}px;
            border-radius: 50%;
            background: ${d.color};
            border: 2px solid rgba(255,255,255,0.5);
            box-shadow: 0 0 ${size}px ${d.color}, 0 0 ${size * 2}px ${d.color}40;
            cursor: pointer;
            pointer-events: all;
            transition: transform 0.15s;
          `;
          el.title = `${d.risk_level} | ${d.sat1_name} ↔ ${d.sat2_name} | ${d.miss_distance} km`;
          el.addEventListener('mouseenter', () => { el.style.transform = 'scale(1.8)'; });
          el.addEventListener('mouseleave', () => { el.style.transform = 'scale(1)'; });
          el.addEventListener('click', () => {
            g.pointOfView({ lat: d.lat, lng: d.lng, altitude: 1.5 }, 1000);
          });
          return el;
        })
        // ── Rings layer — CRITICAL/HIGH pulsing rings ─────────────────────────
        .ringsData([])
        .ringLat('lat')
        .ringLng('lng')
        .ringAltitude(0)
        .ringColor('ringColor')
        .ringMaxRadius('maxRadius')
        .ringPropagationSpeed(1.5)
        .ringRepeatPeriod((d: any) => (d.risk_level === 'CRITICAL' ? 700 : 1200));

      // Auto-rotate — same as reference
      const ctrl = g.controls();
      if (ctrl) {
        ctrl.autoRotate      = true;
        ctrl.autoRotateSpeed = 0.4;
        ctrl.enableDamping   = true;
      }

      // Stop auto-rotate on user interaction
      containerRef.current.addEventListener('mousedown',  () => { if (g.controls()) g.controls().autoRotate = false; });
      containerRef.current.addEventListener('touchstart', () => { if (g.controls()) g.controls().autoRotate = false; }, { passive: true });

      // High-DPI rendering
      try {
        g.renderer().setPixelRatio(Math.min(window.devicePixelRatio, 2.5));
      } catch (_) { /* ignore */ }

      globeRef.current = g;
      setGlobeReady(true);
    })();

    return () => { cancelled = true; };
  }, []);

  // ── ResizeObserver ──────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    const ro = new ResizeObserver(() => {
      if (globeRef.current && containerRef.current) {
        const { width, height } = containerRef.current.getBoundingClientRect();
        globeRef.current.width(width).height(height);
      }
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, [globeReady]);

  // ── Push new collision data into globeDataRef ──────────────────────────────
  useEffect(() => {
    if (!globeReady) return;
    globeDataRef.current = validCollisions.map(buildGlobePoint);
    applyFilter(riskFilterRef.current);
  }, [globeReady, data, applyFilter]); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Real-time orbital animation loop — mirrors reference animateDebris() ──
  useEffect(() => {
    if (!globeReady) return;

    const animate = (ts: number) => {
      animFrameRef.current = requestAnimationFrame(animate);
      const g = globeRef.current;
      if (!g || globeDataRef.current.length === 0) return;
      if (!lastAnimTimeRef.current) { lastAnimTimeRef.current = ts; return; }

      const dt = (ts - lastAnimTimeRef.current) / 1000; // seconds
      lastAnimTimeRef.current = ts;
      if (dt > 1) return; // skip if tab was in background

      // Advance each point along its orbit
      for (const d of globeDataRef.current) {
        const groundTrackRate = d.orbitalRate - EARTH_ROTATION_DEG_S;
        d.lng = ((d.lng + groundTrackRate * dt * ORBIT_SPEED_MULTIPLIER + 540) % 360) - 180;
        d.orbitPhase += d.orbitalRate * dt * ORBIT_SPEED_MULTIPLIER * (Math.PI / 180);
        d.lat = d.inclination * Math.sin(d.orbitPhase);
      }

      // Throttle globe.gl re-render to ~20 fps
      if (ts - lastRenderTimeRef.current < GLOBE_RENDER_INTERVAL) return;
      lastRenderTimeRef.current = ts;

      const filter   = riskFilterRef.current;
      const filtered = filter === 'all'
        ? globeDataRef.current
        : globeDataRef.current.filter((d) => d.risk_level === filter);

      g.htmlElementsData([...filtered]);   // spread to trigger re-render
      const rings = filtered.filter((d) => d.ringColor != null);
      g.ringsData([...rings]);
    };

    animFrameRef.current = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(animFrameRef.current);
  }, [globeReady]);

  // ── Filter change ───────────────────────────────────────────────────────────
  const handleFilterChange = (f: RiskFilter) => {
    setRiskFilter(f);
    riskFilterRef.current = f;
    applyFilter(f);
  };

  // ── Controls ────────────────────────────────────────────────────────────────
  const resetView = () =>
    globeRef.current?.pointOfView({ lat: 20, lng: 0, altitude: 2.5 }, 1000);

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      containerRef.current?.parentElement?.requestFullscreen?.();
      setIsFullscreen(true);
    } else {
      document.exitFullscreen?.();
      setIsFullscreen(false);
    }
  };

  const lastUpdated = dataUpdatedAt ? new Date(dataUpdatedAt).toLocaleTimeString() : '—';
  const visibleCount = riskFilter === 'all'
    ? validCollisions.length
    : validCollisions.filter((c) => c.risk_level === riskFilter).length;

  // ────────────────────────────────────────────────────────────────────────────
  return (
    <div className="h-full flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Globe2 className="w-7 h-7 text-blue-400" />
          <div>
            <h1 className="text-2xl font-bold text-white">3D Orbital Globe</h1>
            <p className="text-sm text-slate-400">
              {allCollisions.length} collision events — real-time orbital animation
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs text-slate-500">Updated {lastUpdated}</span>
          <button onClick={() => refetch()}
            className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition"
            title="Refresh data">
            <RefreshCw className="w-4 h-4" />
          </button>
          <button onClick={resetView}
            className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition"
            title="Reset view">
            <RotateCcw className="w-4 h-4" />
          </button>
          <button onClick={toggleFullscreen}
            className="p-2 rounded-lg bg-slate-800 hover:bg-slate-700 text-slate-300 hover:text-white transition"
            title={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}>
            {isFullscreen ? <Minimize2 className="w-4 h-4" /> : <Maximize2 className="w-4 h-4" />}
          </button>
        </div>
      </div>

      {/* Risk filter chips — same options as reference */}
      <div className="flex flex-wrap items-center gap-2">
        {(['all', 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'] as const).map((level) => (
          <button
            key={level}
            onClick={() => handleFilterChange(level)}
            className={`px-3 py-1.5 rounded-full text-xs font-semibold border transition ${
              riskFilter === level
                ? 'border-transparent text-white shadow-lg'
                : 'border-slate-600 text-slate-400 hover:border-slate-400 hover:text-slate-200'
            }`}
            style={riskFilter === level
              ? { backgroundColor: level === 'all' ? '#3b82f6' : RISK_COLORS[level] }
              : {}}
          >
            {level === 'all' ? 'ALL' : level}
            {level !== 'all' && <span className="ml-1 opacity-70">({counts[level]})</span>}
          </button>
        ))}
        <span className="ml-auto text-xs text-slate-500">
          {visibleCount} of {validCollisions.length} events plotted
        </span>
      </div>

      {/* Globe */}
      <div className="relative flex-1 rounded-xl overflow-hidden bg-[#040714] border border-slate-800 min-h-[500px]">
        <div ref={containerRef} className="w-full h-full" />

        {/* Init / loading overlay */}
        {(isLoading || !globeReady) && (
          <div className="absolute inset-0 flex flex-col items-center justify-center bg-[#040714]/90 z-10">
            <Globe2 className="w-12 h-12 text-blue-400 animate-pulse mb-3" />
            <p className="text-slate-300 text-sm">
              {!globeReady ? 'Initialising 3D globe…' : 'Loading collision data…'}
            </p>
          </div>
        )}

        {/* No ECI position data */}
        {globeReady && !isLoading && validCollisions.length === 0 && allCollisions.length > 0 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none z-10">
            <AlertTriangle className="w-10 h-10 text-yellow-400 mb-2" />
            <p className="text-yellow-300 text-sm text-center px-8">
              Collision data found but no ECI position coordinates present.<br />
              Run <code className="bg-slate-700 px-1 rounded">CollisionDetector</code> to generate approach positions.
            </p>
          </div>
        )}

        {/* No data at all */}
        {globeReady && !isLoading && allCollisions.length === 0 && (
          <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none z-10">
            <Globe2 className="w-10 h-10 text-slate-600 mb-2" />
            <p className="text-slate-500 text-sm text-center px-8">
              No collision data yet.<br />
              Run the pipeline: <code className="bg-slate-700 px-1 rounded">TLEProcessor</code> → <code className="bg-slate-700 px-1 rounded">CollisionDetector</code>
            </p>
          </div>
        )}

        {/* Legend — bottom left */}
        <div className="absolute bottom-4 left-4 bg-[#0d1b2a]/90 backdrop-blur-sm border border-slate-700 rounded-lg px-3 py-2.5 z-20 text-xs space-y-1.5">
          <p className="text-slate-400 font-semibold mb-1 uppercase tracking-wider">Risk Level</p>
          {(['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'] as const).map((lvl) => (
            <div key={lvl} className="flex items-center gap-2">
              <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ backgroundColor: RISK_COLORS[lvl] }} />
              <span className="text-slate-300">{lvl}</span>
              <span className="text-slate-500 ml-auto pl-4">({counts[lvl]})</span>
            </div>
          ))}
          <div className="pt-1 border-t border-slate-700 text-slate-500">
            Rings = CRITICAL / HIGH<br />
            Real-time orbital motion
          </div>
        </div>

        {/* Stats — top right */}
        <div className="absolute top-4 right-4 bg-[#0d1b2a]/90 backdrop-blur-sm border border-slate-700 rounded-lg px-3 py-2.5 z-20 text-xs text-right">
          <p className="text-slate-400">Total events</p>
          <p className="text-white text-2xl font-bold font-mono">{allCollisions.length}</p>
          <p className="text-slate-500">plotted: {visibleCount}</p>
        </div>
      </div>

      {/* Info bar */}
      <p className="text-xs text-slate-500 text-center pb-1">
        Positions converted from ECI (Earth-Centred Inertial) coords · Pulsing rings = CRITICAL / HIGH
        · Drag to rotate · scroll to zoom · click dot to focus
      </p>
    </div>
  );
}
