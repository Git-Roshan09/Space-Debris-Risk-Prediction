export function formatDistance(km: number | null | undefined): string {
  if (km == null) return '—';
  if (km < 1) return `${(km * 1000).toFixed(0)} m`;
  return `${km.toFixed(2)} km`;
}

export function formatVelocity(kms: number | null | undefined): string {
  if (kms == null) return '—';
  return `${kms.toFixed(3)} km/s`;
}

export function formatProbability(p: number | null | undefined): string {
  if (p == null) return '—';
  if (p < 0.0001) return p.toExponential(2);
  return `${(p * 100).toFixed(4)}%`;
}

export function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString('en-GB', {
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
    });
  } catch { return iso; }
}

export function formatDateShort(iso: string | null | undefined): string {
  if (!iso) return '—';
  try { return iso.slice(0, 19).replace('T', ' '); } catch { return iso; }
}

export function formatNumber(n: number | null | undefined): string {
  if (n == null) return '—';
  return n.toLocaleString();
}

export function formatAltitude(km: number | null | undefined): string {
  if (km == null) return '—';
  return `${km.toFixed(0)} km`;
}
