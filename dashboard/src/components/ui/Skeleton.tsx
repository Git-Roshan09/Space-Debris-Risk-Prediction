export function Skeleton({ className = '' }: { className?: string }) {
  return <div className={`animate-pulse bg-border-primary/50 rounded ${className}`} />;
}

export function StatCardSkeleton() {
  return (
    <div className="bg-bg-card border border-border-primary rounded-xl p-4">
      <Skeleton className="w-9 h-9 rounded-lg mb-3" />
      <Skeleton className="h-8 w-20 rounded mb-2" />
      <Skeleton className="h-3 w-24 rounded" />
    </div>
  );
}

export function TableSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2">
      <Skeleton className="h-10 w-full rounded-lg" />
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className="h-12 w-full rounded-lg" />
      ))}
    </div>
  );
}

export function ChartSkeleton() {
  return (
    <div className="bg-bg-card border border-border-primary rounded-xl p-6">
      <Skeleton className="h-4 w-32 rounded mb-4" />
      <Skeleton className="h-48 w-full rounded-lg" />
    </div>
  );
}
