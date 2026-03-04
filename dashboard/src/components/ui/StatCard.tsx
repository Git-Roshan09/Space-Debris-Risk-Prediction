import type { ReactNode } from 'react';
import type { LucideIcon } from 'lucide-react';

interface StatCardProps {
  icon:       LucideIcon;
  label:      string;
  value:      string | number;
  subtitle?:  string;
  iconColor?: string;
  iconBg?:    string;
  glow?:      string;
  trend?:     ReactNode;
}

export default function StatCard({
  icon: Icon, label, value, subtitle,
  iconColor = 'text-accent-blue',
  iconBg    = 'bg-accent-blue/10',
  glow      = '',
  trend,
}: StatCardProps) {
  return (
    <div className={`bg-bg-card border border-border-primary rounded-xl p-4 hover:border-border-secondary transition-all duration-200 animate-fade-in ${glow}`}>
      <div className="flex items-start justify-between mb-3">
        <div className={`w-9 h-9 rounded-lg ${iconBg} flex items-center justify-center`}>
          <Icon className={`w-[18px] h-[18px] ${iconColor}`} />
        </div>
        {trend && <div>{trend}</div>}
      </div>
      <p className="text-2xl font-bold font-mono text-text-primary font-tabular">{value}</p>
      <p className="text-xs text-text-muted mt-1 uppercase tracking-wider">{label}</p>
      {subtitle && <p className="text-xs text-text-secondary mt-0.5">{subtitle}</p>}
    </div>
  );
}
