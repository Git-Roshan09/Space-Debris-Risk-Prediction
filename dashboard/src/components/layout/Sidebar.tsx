import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import {
  LayoutDashboard, AlertTriangle, Satellite, BarChart3,
  Activity, ChevronLeft, ChevronRight, Globe2,
} from 'lucide-react';

const navItems = [
  { to: '/',           icon: LayoutDashboard, label: 'Command Center' },
  { to: '/collisions', icon: AlertTriangle,    label: 'Collision Alerts' },
  { to: '/globe',      icon: Globe2,           label: '3D Globe' },
  { to: '/debris',     icon: Satellite,        label: 'Debris Objects' },
  { to: '/analytics',  icon: BarChart3,        label: 'Analytics' },
  { to: '/system',     icon: Activity,         label: 'System Status' },
];

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside className={`flex flex-col h-full border-r border-border-primary bg-bg-secondary transition-all duration-300 ${collapsed ? 'w-[68px]' : 'w-[240px]'}`}>
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 h-[56px] border-b border-border-primary">
        <div className="w-8 h-8 rounded-lg bg-accent-blue/20 flex items-center justify-center flex-shrink-0">
          <Satellite className="w-4 h-4 text-accent-blue" />
        </div>
        {!collapsed && (
          <div className="overflow-hidden">
            <h1 className="text-sm font-bold text-text-primary tracking-wider">SDRPS</h1>
            <p className="text-[10px] text-text-muted leading-none">Debris Risk Prediction</p>
          </div>
        )}
      </div>

      {/* Nav */}
      <nav className="flex-1 py-3 px-2 space-y-1 overflow-y-auto">
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === '/'}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-200 relative ${
                isActive
                  ? 'bg-accent-blue/10 text-accent-blue'
                  : 'text-text-secondary hover:text-text-primary hover:bg-bg-surface-hover'
              }`
            }
          >
            {({ isActive }) => (
              <>
                {isActive && (
                  <div className="absolute left-0 top-1/2 -translate-y-1/2 w-[3px] h-5 bg-accent-blue rounded-r-full" />
                )}
                <Icon className={`w-[18px] h-[18px] flex-shrink-0 ${isActive ? 'text-accent-blue' : ''}`} />
                {!collapsed && <span>{label}</span>}
              </>
            )}
          </NavLink>
        ))}
      </nav>

      {/* Collapse toggle */}
      <div className="border-t border-border-primary p-2">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="flex items-center justify-center w-full py-2 rounded-lg text-text-muted hover:text-text-primary hover:bg-bg-surface-hover transition-colors"
        >
          {collapsed ? <ChevronRight className="w-4 h-4" /> : <ChevronLeft className="w-4 h-4" />}
        </button>
      </div>
    </aside>
  );
}
