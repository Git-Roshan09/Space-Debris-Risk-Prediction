"""
TLE Streaming API — serves local TLE data as a live stream.

Mirrors the reference project's optimized_tle_api.py:
  - Reads TLE files from disk
  - Serves them via REST + Server-Sent Events (SSE)
  - Simulates real-time data injection

Endpoints:
  GET /api/health              — health check
  GET /api/objects/stream      — SSE stream of TLE batches
  GET /api/objects/batch       — single batch of TLE records
  GET /api/stats               — data statistics

Usage:
  python tle_streaming_api.py
  # Then point tle_kafka_producer.py at this API, or just use the
  # stream endpoint directly from the Airflow DAG / Kafka producer.
"""

from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import os, glob, json, time, random, logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── Config ───────────────────────────────────────────────────────────────────
TLE_DATA_DIRS = [
    os.path.join(os.path.dirname(__file__), "data"),
    os.path.join(os.path.dirname(__file__), "data1"),
    os.path.join(os.path.dirname(__file__), "data2"),
]

# Cache of parsed TLE records
_tle_cache = []
_cache_loaded = False


def _parse_tle_file(filepath):
    """Parse a TLE file into list of (norad_id, line1, line2) tuples."""
    records = []
    try:
        with open(filepath, "r", errors="ignore") as f:
            lines = [l.strip() for l in f if l.strip()]
    except Exception:
        return records

    i = 0
    while i < len(lines) - 1:
        line1 = lines[i]
        line2 = lines[i + 1]

        # Handle 3-line TLE format (name + line1 + line2)
        if not line1.startswith("1 "):
            if i + 2 < len(lines) and lines[i + 1].startswith("1 "):
                line1 = lines[i + 1]
                line2 = lines[i + 2]
                i += 3
            else:
                i += 1
            continue

        if line1.startswith("1 ") and line2.startswith("2 "):
            try:
                norad_id = int(line1[2:7].strip())
                # Parse orbital elements from line 2
                parts = line2.split()
                inclination = float(parts[2]) if len(parts) > 2 else 0.0
                raan = float(parts[3]) if len(parts) > 3 else 0.0
                ecc = f"0.{parts[4]}" if len(parts) > 4 else "0.0"
                argp = float(parts[5]) if len(parts) > 5 else 0.0
                mean_anomaly = float(parts[6]) if len(parts) > 6 else 0.0
                mean_motion = float(parts[7][:11]) if len(parts) > 7 else 0.0

                records.append({
                    "norad_id": norad_id,
                    "name": f"OBJECT-{norad_id}",
                    "tle_line1": line1,
                    "tle_line2": line2,
                    "classification": "UNKNOWN",
                    "metadata": {"source_file": os.path.basename(filepath)},
                    "inclination": inclination,
                    "raan": raan,
                    "eccentricity": ecc,
                    "argument_of_perigee": argp,
                    "mean_anomaly": mean_anomaly,
                    "mean_motion": mean_motion,
                })
            except (ValueError, IndexError):
                pass
            i += 2
        else:
            i += 1

    return records


def _load_tle_cache():
    """Load all TLE data into cache."""
    global _tle_cache, _cache_loaded
    if _cache_loaded:
        return

    logger.info("Loading TLE data into cache...")
    for d in TLE_DATA_DIRS:
        if not os.path.isdir(d):
            continue
        for fpath in sorted(glob.glob(os.path.join(d, "*.txt"))):
            records = _parse_tle_file(fpath)
            _tle_cache.extend(records)
            logger.info(f"  {os.path.basename(fpath)}: {len(records)} TLEs")
        for fpath in sorted(glob.glob(os.path.join(d, "*.tle"))):
            records = _parse_tle_file(fpath)
            _tle_cache.extend(records)

    _cache_loaded = True
    logger.info(f"Total TLE records cached: {len(_tle_cache):,}")


# ── Routes ───────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    _load_tle_cache()
    return jsonify({
        "status": "healthy",
        "total_tle_records": len(_tle_cache),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/stats", methods=["GET"])
def stats():
    _load_tle_cache()
    norad_ids = set(r["norad_id"] for r in _tle_cache)
    return jsonify({
        "total_records": len(_tle_cache),
        "unique_objects": len(norad_ids),
        "data_dirs": [d for d in TLE_DATA_DIRS if os.path.isdir(d)],
    })


@app.route("/api/objects/batch", methods=["GET"])
def objects_batch():
    """Return a single batch of TLE records."""
    _load_tle_cache()

    batch_size = min(200, int(request.args.get("batch_size", 50)))
    offset = int(request.args.get("offset", 0))
    obj_type = request.args.get("type", "all")

    end = min(offset + batch_size, len(_tle_cache))
    batch = _tle_cache[offset:end]

    return jsonify({
        "batch_number": offset // batch_size + 1,
        "batch_size": len(batch),
        "total_available": len(_tle_cache),
        "objects": batch,
    })


@app.route("/api/objects/stream", methods=["GET"])
def objects_stream():
    """
    Server-Sent Events stream of TLE batches.
    Mimics the reference project's streaming endpoint.

    Query params:
      batch_size  — objects per batch (default: 50)
      delay_ms    — milliseconds between batches (default: 1000)
      type        — 'all' (default)
      max_batches — stop after N batches (default: 0 = unlimited)
    """
    _load_tle_cache()

    batch_size = min(200, int(request.args.get("batch_size", 50)))
    delay_ms = int(request.args.get("delay_ms", 1000))
    max_batches = int(request.args.get("max_batches", 0))

    def generate():
        idx = 0
        batch_num = 0

        while idx < len(_tle_cache):
            batch = _tle_cache[idx:idx + batch_size]
            idx += batch_size
            batch_num += 1

            payload = {
                "batch_number": batch_num,
                "batch_size": len(batch),
                "total_sent": idx,
                "total_available": len(_tle_cache),
                "timestamp": datetime.utcnow().isoformat(),
                "batch": batch,
            }

            yield f"data: {json.dumps(payload)}\n\n"

            if max_batches > 0 and batch_num >= max_batches:
                break

            time.sleep(delay_ms / 1000.0)

        # End of stream
        yield f"data: {json.dumps({'status': 'complete', 'total_sent': idx})}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


if __name__ == "__main__":
    port = int(os.getenv("TLE_API_PORT", "5055"))
    logger.info(f"🛰️  TLE Streaming API on http://0.0.0.0:{port}")
    logger.info(f"   Data dirs: {TLE_DATA_DIRS}")
    logger.info(f"   Stream:    http://localhost:{port}/api/objects/stream")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
