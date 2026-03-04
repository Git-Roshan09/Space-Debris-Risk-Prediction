#!/usr/bin/env python3
"""
Pipeline Scheduler — Space Debris Risk Prediction
==================================================
Replaces Airflow with a pure-Python scheduler.

Reference architecture (what Airflow does in reference project):
  Every 5 min:
    Task 1 ─ check_api_health     → verify services are up
    Task 2 ─ ingest_tle_to_hdfs   → live_ingest.py  (sgp4 → HDFS)
    Task 3 ─ run_collision_detect → sbt runMain CollisionPrediction

This script does the same three tasks on a configurable interval.

Usage:
  python3 pipeline_scheduler.py                    # default: every 5 min
  python3 pipeline_scheduler.py --interval 10      # every 10 min
  python3 pipeline_scheduler.py --run-once         # single run then exit
  python3 pipeline_scheduler.py --skip-ingest      # collision only (HDFS data exists)
  python3 pipeline_scheduler.py --interval 5 --n-sat 5000 --n-deb 10000
"""

import argparse
import logging
import os
import subprocess
import sys
import time
import requests
from datetime import datetime, timezone

# ─── Config ──────────────────────────────────────────────────────────────────
BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
VENV_PYTHON     = os.path.join(BASE_DIR, "venv", "bin", "python3")
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE",  "localhost")
WEBHDFS_PORT    = os.getenv("WEBHDFS_PORT",   "9870")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "scheduler.log"), encoding="utf-8"),
    ],
)
log = logging.getLogger("scheduler")


# ─── CLI ─────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Space Debris Pipeline Scheduler (Airflow replacement)")
parser.add_argument("--interval",    type=int,   default=5,     help="Minutes between pipeline runs (default: 5)")
parser.add_argument("--run-once",    action="store_true",       help="Run once then exit")
parser.add_argument("--skip-ingest", action="store_true",       help="Skip live_ingest.py (use existing HDFS data)")
parser.add_argument("--no-kafka",    action="store_true",       help="Pass --no-kafka to live_ingest.py")
parser.add_argument("--n-sat",       type=int,   default=5000,  help="SATELLITE cap for ingest (default: 5000)")
parser.add_argument("--n-deb",       type=int,   default=10000, help="DEBRIS cap for ingest (default: 10000)")
parser.add_argument("--sample-file", type=int,   default=0,     help="Archive files to sample, 0=all (default: 0)")
args = parser.parse_args()


# ─── Task helpers ─────────────────────────────────────────────────────────────

def _header(run_id: int, task: str):
    log.info("─" * 60)
    log.info(f"  Run #{run_id} │ {task}")
    log.info("─" * 60)


def _run(cmd: list[str], task_name: str) -> bool:
    """Run a subprocess; return True on success."""
    log.info(f"  $ {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=BASE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=600,   # 10 min max per step
        )
        for line in result.stdout.splitlines():
            log.info(f"    {line}")
        if result.returncode == 0:
            log.info(f"  ✅ {task_name} — success")
            return True
        else:
            log.error(f"  ❌ {task_name} — exit code {result.returncode}")
            return False
    except subprocess.TimeoutExpired:
        log.error(f"  ❌ {task_name} — TIMEOUT (>600s)")
        return False
    except Exception as e:
        log.error(f"  ❌ {task_name} — error: {e}")
        return False


# ─── Task 1: Health Check ─────────────────────────────────────────────────────

def task_check_health() -> bool:
    """
    Mirrors reference DAG's check_api_health task.
    Verifies HDFS WebUI and Kafka broker are reachable.
    """
    log.info("  [Task 1/3] Health checks…")
    ok = True

    # HDFS
    try:
        r = requests.get(
            f"http://{HDFS_NAMENODE}:{WEBHDFS_PORT}/webhdfs/v1/?op=LISTSTATUS&user.name=root",
            timeout=8
        )
        if r.ok:
            log.info("    ✅ HDFS WebHDFS — reachable")
        else:
            log.warning(f"    ⚠️  HDFS responded {r.status_code}")
    except Exception as e:
        log.error(f"    ❌ HDFS unreachable: {e}")
        ok = False

    # Kafka (TCP connect)
    host, port = KAFKA_BOOTSTRAP.split(":")
    try:
        import socket
        s = socket.create_connection((host, int(port)), timeout=5)
        s.close()
        log.info(f"    ✅ Kafka broker {KAFKA_BOOTSTRAP} — reachable")
    except Exception as e:
        log.warning(f"    ⚠️  Kafka unreachable ({e}) — ingest will use --no-kafka if needed")

    return ok


# ─── Task 2: Ingest TLE → sgp4 → HDFS ────────────────────────────────────────

def task_ingest() -> bool:
    """
    Mirrors reference DAG's ingest_api_to_kafka task.
    Reads TLE lines from HDFS archive, propagates via sgp4, writes live_sv_*.parquet.
    """
    log.info("  [Task 2/3] TLE ingestion (sgp4 → HDFS)…")

    cmd = [
        VENV_PYTHON, os.path.join(BASE_DIR, "live_ingest.py"),
        "--n-sat",  str(args.n_sat),
        "--n-deb",  str(args.n_deb),
    ]
    if args.no_kafka:
        cmd.append("--no-kafka")
    if args.sample_file:
        cmd += ["--sample-file", str(args.sample_file)]

    return _run(cmd, "live_ingest.py")


# ─── Task 3: Collision Detection (sbt) ───────────────────────────────────────

def task_collision_predict() -> bool:
    """
    Mirrors reference DAG's detect_collisions task.
    Runs CollisionPrediction.scala via sbt.
    """
    log.info("  [Task 3/3] Collision prediction (Spark / Scala)…")
    return _run(["sbt", "runMain CollisionPrediction"], "CollisionPrediction.scala")


# ─── Pipeline run ─────────────────────────────────────────────────────────────

def run_pipeline(run_id: int) -> dict:
    """Run all three tasks in sequence; return result summary."""
    start = time.time()
    results = {}

    _header(run_id, f"Pipeline started — {datetime.now(timezone.utc).isoformat()}")

    # Task 1 — health check (non-fatal: continue even if HDFS/Kafka warns)
    results["health"]    = task_check_health()

    # Task 2 — ingest
    if not args.skip_ingest:
        results["ingest"] = task_ingest()
        if not results["ingest"]:
            log.error("  ⛔ Ingest failed — skipping collision step for this run")
            results["collision"] = False
            results["elapsed_s"] = round(time.time() - start, 1)
            return results
    else:
        log.info("  [Task 2/3] Skipped (--skip-ingest flag set)")
        results["ingest"] = None

    # Task 3 — collision prediction
    results["collision"] = task_collision_predict()

    elapsed = round(time.time() - start, 1)
    results["elapsed_s"] = elapsed

    log.info("─" * 60)
    log.info(f"  Run #{run_id} complete in {elapsed}s")
    log.info(f"  health={results['health']}  ingest={results['ingest']}  collision={results['collision']}")
    log.info("─" * 60)

    return results


# ─── Main loop ────────────────────────────────────────────────────────────────

def main():
    interval_sec = args.interval * 60
    run_id = 0

    log.info("=" * 60)
    log.info("  SPACE DEBRIS PIPELINE SCHEDULER")
    log.info(f"  Interval  : every {args.interval} minute(s)")
    log.info(f"  Mode      : {'run-once' if args.run_once else 'continuous'}")
    log.info(f"  Ingest    : {'SKIP' if args.skip_ingest else 'ENABLED'}")
    log.info(f"  Kafka     : {'DISABLED' if args.no_kafka else 'ENABLED'}")
    log.info(f"  SAT cap   : {args.n_sat}   DEB cap: {args.n_deb}")
    log.info("=" * 60)

    while True:
        run_id += 1
        try:
            result = run_pipeline(run_id)
        except KeyboardInterrupt:
            log.info("\n  Stopped by user.")
            break
        except Exception as e:
            log.error(f"  Unexpected error in run #{run_id}: {e}", exc_info=True)

        if args.run_once:
            log.info("  --run-once: exiting.")
            break

        next_run = datetime.now(timezone.utc)
        log.info(f"\n  Sleeping {args.interval}m until next run…  (Ctrl+C to stop)\n")
        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            log.info("\n  Stopped by user.")
            break


if __name__ == "__main__":
    main()
