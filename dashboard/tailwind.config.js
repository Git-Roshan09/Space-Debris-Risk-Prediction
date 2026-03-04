/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        'bg-primary':        '#060a13',
        'bg-secondary':      '#0a0e17',
        'bg-surface':        '#111827',
        'bg-surface-hover':  '#1a2235',
        'bg-card':           '#0f172a',
        'bg-card-hover':     '#162039',
        'border-primary':    '#1e293b',
        'border-secondary':  '#334155',
        'text-primary':      '#f1f5f9',
        'text-secondary':    '#94a3b8',
        'text-muted':        '#64748b',
        'accent-blue':       '#3b82f6',
        'accent-cyan':       '#06b6d4',
        'accent-indigo':     '#6366f1',
        'risk-critical':     '#ef4444',
        'risk-high':         '#f97316',
        'risk-medium':       '#eab308',
        'risk-low':          '#22c55e',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', '-apple-system', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'monospace'],
      },
      animation: {
        'pulse-glow': 'pulse-glow 2s ease-in-out infinite',
        'fade-in':    'fade-in 0.3s ease-out',
      },
      keyframes: {
        'pulse-glow': {
          '0%, 100%': { opacity: '1' },
          '50%':      { opacity: '0.6' },
        },
        'fade-in': {
          from: { opacity: '0' },
          to:   { opacity: '1' },
        },
      },
    },
  },
  plugins: [],
};
