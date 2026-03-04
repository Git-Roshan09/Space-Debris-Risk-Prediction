import type { RiskLevel } from '../../api/types';
import { RISK_BG, RISK_TEXT } from '../../utils/constants';

export default function RiskBadge({ level }: { level: RiskLevel }) {
  return (
    <span className={`inline-flex items-center text-[10px] px-2 py-0.5 font-bold uppercase tracking-wider rounded-full border ${RISK_BG[level]} ${RISK_TEXT[level]}`}>
      {level === 'CRITICAL' && (
        <span className="w-1.5 h-1.5 rounded-full bg-risk-critical mr-1 animate-pulse-glow" />
      )}
      {level}
    </span>
  );
}
