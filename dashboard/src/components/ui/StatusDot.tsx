type Status = 'online' | 'offline' | 'warning';

const colors: Record<Status, string> = {
  online:  'bg-risk-low',
  offline: 'bg-risk-critical',
  warning: 'bg-risk-medium',
};

export default function StatusDot({ status, pulse = true }: { status: Status; pulse?: boolean }) {
  return (
    <span className="relative inline-flex h-2 w-2">
      {pulse && status === 'online' && (
        <span className={`animate-ping absolute inline-flex h-full w-full rounded-full ${colors[status]} opacity-75`} />
      )}
      <span className={`relative inline-flex rounded-full h-2 w-2 ${colors[status]}`} />
    </span>
  );
}
