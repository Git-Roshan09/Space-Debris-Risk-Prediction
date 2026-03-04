import type { ReactNode } from 'react';

interface ChartCardProps {
  title:     string;
  subtitle?: string;
  children:  ReactNode;
  actions?:  ReactNode;
  className?: string;
}

export default function ChartCard({ title, subtitle, children, actions, className = '' }: ChartCardProps) {
  return (
    <div className={`bg-bg-card border border-border-primary rounded-xl overflow-hidden animate-fade-in ${className}`}>
      <div className="flex items-center justify-between px-5 py-4 border-b border-border-primary">
        <div>
          <h3 className="text-sm font-semibold text-text-primary">{title}</h3>
          {subtitle && <p className="text-xs text-text-muted mt-0.5">{subtitle}</p>}
        </div>
        {actions && <div className="flex items-center gap-2">{actions}</div>}
      </div>
      <div className="p-5">{children}</div>
    </div>
  );
}
