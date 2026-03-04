#!/usr/bin/env bash
# =============================================================================
#  Space Debris Pipeline Scheduler — Background Runner
#
#  Usage:
#    ./run_pipeline.sh start          # start background scheduler (every 5 min)
#    ./run_pipeline.sh start 10       # start with 10-min interval
#    ./run_pipeline.sh stop           # stop the background scheduler
#    ./run_pipeline.sh restart        # stop then start again
#    ./run_pipeline.sh status         # check if running + last log lines
#    ./run_pipeline.sh once           # run the pipeline exactly once (foreground)
#    ./run_pipeline.sh logs           # tail live log output
# =============================================================================

BASE_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PYTHON="$BASE_DIR/venv/bin/python3"
SCHEDULER="$BASE_DIR/pipeline_scheduler.py"
PID_FILE="$BASE_DIR/scheduler.pid"
LOG_FILE="$BASE_DIR/scheduler.log"

# ── Default pipeline settings (edit here to change) ──────────────────────────
INTERVAL="${2:-5}"      # minutes between pipeline runs  (default: 5)
N_SAT=5000              # max SATELLITE objects per ingest
N_DEB=10000             # max DEBRIS objects per ingest
SAMPLE_FILE=2           # HDFS archive files to sample (0 = all 710, slower)
EXTRA_FLAGS="--no-kafka"  # remove this line to enable Kafka streaming

# ── Colors ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ── Helpers ───────────────────────────────────────────────────────────────────
is_running() {
    [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

# ── Commands ──────────────────────────────────────────────────────────────────
case "${1:-help}" in

  # ── START ──────────────────────────────────────────────────────────────────
  start)
    if is_running; then
        echo -e "${YELLOW}⚠️  Scheduler already running (PID $(cat "$PID_FILE"))${NC}"
        echo "   Use './run_pipeline.sh stop' first, or './run_pipeline.sh restart'"
        exit 1
    fi

    echo -e "${BOLD}${CYAN}"
    echo "  ╔══════════════════════════════════════════════╗"
    echo "  ║   SPACE DEBRIS PIPELINE SCHEDULER            ║"
    echo "  ╚══════════════════════════════════════════════╝${NC}"
    echo -e "  Interval  : ${GREEN}every ${INTERVAL} min${NC}"
    echo -e "  SAT cap   : ${GREEN}${N_SAT}${NC}   DEB cap: ${GREEN}${N_DEB}${NC}"
    echo -e "  Log file  : ${CYAN}${LOG_FILE}${NC}"
    echo ""

    nohup "$VENV_PYTHON" "$SCHEDULER" \
        --interval    "$INTERVAL"    \
        --n-sat       "$N_SAT"       \
        --n-deb       "$N_DEB"       \
        --sample-file "$SAMPLE_FILE" \
        $EXTRA_FLAGS                 \
        >> "$LOG_FILE" 2>&1 &

    echo $! > "$PID_FILE"
    sleep 1

    if is_running; then
        echo -e "  ${GREEN}✅  Scheduler started — PID $(cat "$PID_FILE")${NC}"
        echo ""
        echo "  Quick commands:"
        echo -e "    ${CYAN}./run_pipeline.sh logs${NC}    → watch live output"
        echo -e "    ${CYAN}./run_pipeline.sh status${NC}  → health check"
        echo -e "    ${CYAN}./run_pipeline.sh stop${NC}    → stop scheduler"
    else
        echo -e "  ${RED}❌  Failed to start — check ${LOG_FILE}${NC}"
        exit 1
    fi
    ;;

  # ── STOP ───────────────────────────────────────────────────────────────────
  stop)
    if is_running; then
        PID=$(cat "$PID_FILE")
        kill "$PID" 2>/dev/null
        rm -f "$PID_FILE"
        echo -e "${GREEN}✅  Scheduler stopped (PID $PID)${NC}"
    else
        echo -e "${YELLOW}⚠️  Scheduler is not running${NC}"
        rm -f "$PID_FILE" 2>/dev/null
    fi
    ;;

  # ── RESTART ────────────────────────────────────────────────────────────────
  restart)
    "$0" stop
    sleep 1
    "$0" start "$INTERVAL"
    ;;

  # ── STATUS ─────────────────────────────────────────────────────────────────
  status)
    if is_running; then
        echo -e "${GREEN}✅  Scheduler is RUNNING  (PID $(cat "$PID_FILE"))${NC}"
    else
        echo -e "${RED}⛔  Scheduler is NOT running${NC}"
    fi
    echo ""
    echo -e "${CYAN}Last 8 log lines:${NC}"
    tail -8 "$LOG_FILE" 2>/dev/null | sed 's/^/  /' || echo "  (no log yet)"
    ;;

  # ── ONCE ───────────────────────────────────────────────────────────────────
  once)
    echo -e "${GREEN}▶  Running pipeline once (foreground)...${NC}"
    "$VENV_PYTHON" "$SCHEDULER"  \
        --run-once               \
        --n-sat    "$N_SAT"      \
        --n-deb    "$N_DEB"      \
        --sample-file "$SAMPLE_FILE" \
        $EXTRA_FLAGS
    ;;

  # ── LOGS ───────────────────────────────────────────────────────────────────
  logs)
    echo -e "${CYAN}📋 Tailing ${LOG_FILE}  (Ctrl+C to stop)${NC}"
    tail -f "$LOG_FILE"
    ;;

  # ── HELP ───────────────────────────────────────────────────────────────────
  *)
    echo ""
    echo -e "${BOLD}Usage:${NC}  ./run_pipeline.sh <command> [interval_min]"
    echo ""
    echo "  Commands:"
    echo -e "    ${GREEN}start${NC}  [min]   Start background scheduler (default: 5 min)"
    echo -e "    ${RED}stop${NC}           Stop background scheduler"
    echo -e "    ${YELLOW}restart${NC} [min]  Restart with optional new interval"
    echo -e "    ${CYAN}status${NC}         Show running status + recent log"
    echo -e "    ${CYAN}once${NC}           Run pipeline exactly once (foreground)"
    echo -e "    ${CYAN}logs${NC}           Tail live log output"
    echo ""
    echo "  Examples:"
    echo "    ./run_pipeline.sh start         # every 5 min (default)"
    echo "    ./run_pipeline.sh start 10      # every 10 min"
    echo "    ./run_pipeline.sh once          # single run"
    echo "    ./run_pipeline.sh logs          # watch output"
    echo "    ./run_pipeline.sh stop          # stop it"
    echo ""
    ;;

esac
