import { Inbox } from 'lucide-react';

export default function EmptyState({
  title   = 'No data available',
  message = 'Run the Spark pipeline first to populate data.',
}: { title?: string; message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-12 text-text-muted">
      <Inbox className="w-12 h-12 mb-3 opacity-40" />
      <p className="text-sm font-medium">{title}</p>
      <p className="text-xs mt-1 opacity-70">{message}</p>
    </div>
  );
}
