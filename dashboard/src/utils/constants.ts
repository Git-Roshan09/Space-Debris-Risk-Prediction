import type { RiskLevel } from '../api/types';

export const RISK_COLORS: Record<RiskLevel, string> = {
  CRITICAL: '#ef4444',
  HIGH:     '#f97316',
  MEDIUM:   '#eab308',
  LOW:      '#22c55e',
};

export const RISK_BG: Record<RiskLevel, string> = {
  CRITICAL: 'bg-risk-critical/10 border-risk-critical/30',
  HIGH:     'bg-risk-high/10 border-risk-high/30',
  MEDIUM:   'bg-risk-medium/10 border-risk-medium/30',
  LOW:      'bg-risk-low/10 border-risk-low/30',
};

export const RISK_TEXT: Record<RiskLevel, string> = {
  CRITICAL: 'text-risk-critical',
  HIGH:     'text-risk-high',
  MEDIUM:   'text-risk-medium',
  LOW:      'text-risk-low',
};
