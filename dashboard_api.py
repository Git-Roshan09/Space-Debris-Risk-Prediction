"""
Space Debris Dashboard API — reads from HDFS Parquet (Scala pipeline output)
Falls back to local CSV if HDFS is unavailable.

Data sources:
  HDFS /space-debris/collision-predictions/batch_*   ← CollisionPrediction.scala
  HDFS /space-debris/collision-predictions/pipeline_metrics ← run stats
  HDFS /space-debris/ml-results/metrics              ← MLlibTraining.scala
  HDFS /space-debris/ml-results/clustered-debris     ← orbit clusters
  HDFS /space-debris/catalog                         ← debris catalog
  Local Output/                                      ← legacy CSV fallback

Endpoints:
  GET /api/health
  GET /api/stats
  GET /api/dashboard/stats
  GET /api/collisions          ?risk_level=&page=&per_page=&sort_by=&sort_order=
  GET /api/collisions/all      (reference-compatible)
  GET /api/collisions/high-risk
  GET /api/collisions/globe
  GET /api/collisions/frequency
  GET /api/debris              ?limit=
  GET /api/debris/<norad_id>
  GET /api/ml/metrics
  GET /api/pipeline/status
  GET /api/simulation/time

Run:
  pip install flask flask-cors pandas pyarrow requests
  python dashboard_api.py
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import os, glob, json, math, io, logging, tempfile
from datetime import datetime
from pathlib import Path
import requests as req_lib

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("⚠️  pandas not installed — pip install pandas pyarrow")

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ─── Config ──────────────────────────────────────────────────────────────────
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE",  "localhost")
WEBHDFS_PORT    = os.getenv("WEBHDFS_PORT",   "9870")
HDFS_USER       = os.getenv("HDFS_USER",      "root")
HDFS_BASE       = "/space-debris"

LOCAL_OUTPUT_DIR         = os.path.join(os.path.dirname(__file__), "Output")
LOCAL_COLLISION_GLOB     = os.path.join(LOCAL_OUTPUT_DIR, "collision_alerts_*/part-*.csv")
LOCAL_COLLISION_GLOB_ALT = os.path.join(LOCAL_OUTPUT_DIR, "collision_alerts_*.csv")
LOCAL_COLLISION_GLOB_STREAM = os.path.join(LOCAL_OUTPUT_DIR, "stream_collision_alerts_*/part-*.csv")
LOCAL_CATALOG_PATH       = os.path.join(LOCAL_OUTPUT_DIR, "space_debris_catalog.csv")


# ─── WebHDFS helpers ─────────────────────────────────────────────────────────

def _webhdfs_url(path: str, op: str, **params) -> str:
    base = f"http://{HDFS_NAMENODE}:{WEBHDFS_PORT}/webhdfs/v1{path}?op={op}&user.name={HDFS_USER}"
    for k, v in params.items():
        base += f"&{k}={v}"
    return base


def hdfs_list_dir(hdfs_path: str) -> list:
    try:
        r = req_lib.get(_webhdfs_url(hdfs_path, "LISTSTATUS"), timeout=5)
        if r.status_code == 200:
            return r.json().get("FileStatuses", {}).get("FileStatus", [])
    except Exception as e:
        logger.debug(f"HDFS list failed for {hdfs_path}: {e}")
    return []


def hdfs_download_file(hdfs_path: str) -> bytes | None:
    """Download a file from HDFS via WebHDFS (handles 307 redirect)."""
    try:
        r = req_lib.get(_webhdfs_url(hdfs_path, "OPEN"),
                        allow_redirects=False, timeout=5)
        if r.status_code == 307:
            redirect = r.headers["Location"]
            # Fix docker hostname → localhost
            redirect = redirect.replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
            data_r = req_lib.get(redirect, timeout=60)
            if data_r.status_code == 200:
                return data_r.content
    except Exception as e:
        logger.debug(f"HDFS download failed for {hdfs_path}: {e}")
    return None


def hdfs_read_parquet_dir(hdfs_dir_path: str) -> "pd.DataFrame":
    """Read all Parquet part files from an HDFS directory via WebHDFS."""
    if not HAS_PANDAS:
        return pd.DataFrame()

    files = hdfs_list_dir(hdfs_dir_path)
    dfs = []
    for f in files:
        name = f.get("pathSuffix", "")
        if name.endswith(".parquet") or name.startswith("part-"):
            raw = hdfs_download_file(f"{hdfs_dir_path}/{name}")
            if raw:
                try:
                    df = pd.read_parquet(io.BytesIO(raw))
                    dfs.append(df)
                except Exception as e:
                    logger.debug(f"Could not parse {name}: {e}")

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def hdfs_read_json_dir(hdfs_dir_path: str) -> "pd.DataFrame":
    """Read JSON lines files from an HDFS directory."""
    if not HAS_PANDAS:
        return pd.DataFrame()

    files = hdfs_list_dir(hdfs_dir_path)
    dfs = []
    for f in files:
        name = f.get("pathSuffix", "")
        if name.startswith("part-") or name.endswith(".json"):
            raw = hdfs_download_file(f"{hdfs_dir_path}/{name}")
            if raw:
                try:
                    df = pd.read_json(io.BytesIO(raw), lines=True)
                    dfs.append(df)
                except Exception:
                    pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ─── Column normalisation ────────────────────────────────────────────────────

def _normalise_collisions(df: "pd.DataFrame") -> "pd.DataFrame":
    """Map Scala output column names to the API's standard field names."""
    col_map = {
        # Scala CollisionPrediction.scala output → API names
        "norad_1":              "norad_id_1",
        "norad_2":              "norad_id_2",
        "name_1":               "object_name_1",
        "name_2":               "object_name_2",
        "class_1":              "type_1",
        "class_2":              "type_2",
        "x1":                   "pos_x_1",
        "y1":                   "pos_y_1",
        "z1":                   "pos_z_1",
        "x2":                   "pos_x_2",
        "y2":                   "pos_y_2",
        "z2":                   "pos_z_2",
        "alt_1":                "altitude_km_1",
        "alt_2":                "altitude_km_2",
        "vel_1":                "speed_kms_1",
        "vel_2":                "speed_kms_2",
        "detection_timestamp":  "detection_timestamp",
        # Old CSV column names (backward compat)
        "NORAD_ID_1":           "norad_id_1",
        "NORAD_ID_2":           "norad_id_2",
        "DISTANCE_KM":          "distance_km",
        "RISK_LEVEL":           "risk_level",
        "COLLISION_TYPE":       "collision_type",
        "COLLISION_PROBABILITY":"collision_probability",
        "RELATIVE_VELOCITY_KMS":"relative_velocity_kms",
        "DETECTION_TIMESTAMP":  "detection_timestamp",
        "TYPE_1":               "type_1",
        "TYPE_2":               "type_2",
        "ALTITUDE_KM_1":       "altitude_km_1",
        "ALTITUDE_KM_2":       "altitude_km_2",
        "SPEED_KMS_1":         "speed_kms_1",
        "SPEED_KMS_2":         "speed_kms_2",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


# ─── Data loaders ────────────────────────────────────────────────────────────

def _load_collision_data() -> "pd.DataFrame":
    """Load collision data: HDFS Parquet (primary) → local CSV (fallback)."""
    if not HAS_PANDAS:
        return pd.DataFrame()

    # ── Try HDFS Parquet first ───────────────────────────────────────────────
    hdfs_dir = f"{HDFS_BASE}/collision-predictions"
    batches = hdfs_list_dir(hdfs_dir)

    # Find latest batch_* directory
    batch_dirs = sorted(
        [b for b in batches if b.get("pathSuffix", "").startswith("batch_")],
        key=lambda x: x["pathSuffix"],
        reverse=True
    )

    if batch_dirs:
        latest = batch_dirs[0]["pathSuffix"]
        logger.info(f"Reading collision data from HDFS: {latest}")
        df = hdfs_read_parquet_dir(f"{hdfs_dir}/{latest}")
        if not df.empty:
            logger.info(f"  Loaded {len(df)} collision records from HDFS")
            return _normalise_collisions(df)

    # Also try high_risk batch
    high_risk_dirs = sorted(
        [b for b in batches if b.get("pathSuffix", "").startswith("high_risk_batch_")],
        key=lambda x: x["pathSuffix"],
        reverse=True
    )

    # ── Local CSV fallback ───────────────────────────────────────────────────
    files = sorted(
        glob.glob(LOCAL_COLLISION_GLOB) +
        glob.glob(LOCAL_COLLISION_GLOB_ALT) +
        glob.glob(LOCAL_COLLISION_GLOB_STREAM),
        reverse=True
    )
    if files:
        dfs = []
        latest_dir = str(Path(files[0]).parent)
        for f in files:
            if str(Path(f).parent) == latest_dir:
                try:
                    dfs.append(pd.read_csv(f))
                except Exception:
                    pass
        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Loaded {len(df)} collision records from local CSV")
            return _normalise_collisions(df)

    logger.warning("No collision data found")
    return pd.DataFrame()


def _load_debris_catalog() -> "pd.DataFrame":
    """Load debris catalog: HDFS Parquet → local CSV fallback."""
    if not HAS_PANDAS:
        return pd.DataFrame()

    # Try HDFS first
    df = hdfs_read_parquet_dir(f"{HDFS_BASE}/catalog")
    if not df.empty:
        return df

    # Local fallback
    if os.path.exists(LOCAL_CATALOG_PATH):
        try:
            return pd.read_csv(LOCAL_CATALOG_PATH)
        except Exception:
            pass
    return pd.DataFrame()


def _load_ml_metrics() -> dict:
    """Load ML training metrics from HDFS."""
    df = hdfs_read_json_dir(f"{HDFS_BASE}/ml-results/metrics")
    if not df.empty:
        return df.iloc[-1].to_dict()  # Latest metrics
    return {}


def _load_pipeline_metrics() -> "pd.DataFrame":
    """Load collision pipeline run history."""
    return hdfs_read_parquet_dir(f"{HDFS_BASE}/collision-predictions/pipeline_metrics")


# ─── Helper ──────────────────────────────────────────────────────────────────

def _safe_float(v) -> "float | None":
    try:
        f = float(v)
        return None if math.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


# ─── Routes ──────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    hdfs_ok = False
    try:
        r = req_lib.get(_webhdfs_url("/", "GETFILESTATUS"), timeout=3)
        hdfs_ok = r.status_code in (200, 404)
    except Exception:
        pass

    return jsonify({
        "status":    "healthy",
        "hdfs":      "connected" if hdfs_ok else "unavailable",
        "timestamp": datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/stats", methods=["GET"])
@app.route("/api/dashboard/stats", methods=["GET"])
def stats():
    df = _load_collision_data()
    catalog = _load_debris_catalog()

    if df.empty:
        return jsonify({
            "total_debris_objects":     len(catalog) if not catalog.empty else 0,
            "active_satellites":        0,
            "critical_risk_collisions": 0,
            "high_risk_collisions":     0,
            "medium_risk_collisions":   0,
            "low_risk_collisions":      0,
            "total_active_collisions":  0,
            "total_collision_pairs":    0,
            "min_distance_km":          None,
            "avg_distance_km":          None,
            "max_distance_km":          None,
            "closest_approach":         None,
            "simulated_time":           datetime.utcnow().isoformat() + "Z",
            "timestamp":                datetime.utcnow().isoformat() + "Z",
        })

    risk_counts = df["risk_level"].value_counts().to_dict() if "risk_level" in df.columns else {}
    dist = df["distance_km"] if "distance_km" in df.columns else None

    closest = None
    if dist is not None and not df.empty:
        idx = df["distance_km"].idxmin()
        row = df.loc[idx]
        n1 = str(row.get("norad_id_1", "?"))
        n2 = str(row.get("norad_id_2", "?"))
        closest = {
            "satellite_1_id":   n1,
            "satellite_2_id":   n2,
            "satellite_1_name": str(row.get("object_name_1", n1)),
            "satellite_2_name": str(row.get("object_name_2", n2)),
            "miss_distance_km": _safe_float(row["distance_km"]),
            "risk_level":       str(row.get("risk_level", "?")),
            "predicted_time":   str(row.get("detection_timestamp", "")),
            "collision_type":   str(row.get("collision_type", "")),
            # backward compatible
            "norad_id_1":       n1,
            "norad_id_2":       n2,
            "distance_km":      _safe_float(row["distance_km"]),
            "epoch_1":          str(row.get("detection_timestamp", "")),
        }

    total = len(df)
    return jsonify({
        "total_debris_objects":     len(catalog) if not catalog.empty else 0,
        "active_satellites":        total,
        "critical_risk_collisions": int(risk_counts.get("CRITICAL", 0)),
        "high_risk_collisions":     int(risk_counts.get("HIGH",     0)),
        "medium_risk_collisions":   int(risk_counts.get("MEDIUM",   0)),
        "low_risk_collisions":      int(risk_counts.get("LOW",      0)),
        "total_active_collisions":  total,
        "total_collision_pairs":    total,
        "min_distance_km":          _safe_float(dist.min())  if dist is not None else None,
        "avg_distance_km":          _safe_float(dist.mean()) if dist is not None else None,
        "max_distance_km":          _safe_float(dist.max())  if dist is not None else None,
        "closest_approach":         closest,
        "simulated_time":           datetime.utcnow().isoformat() + "Z",
        "timestamp":                datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/collisions", methods=["GET"])
@app.route("/api/collisions/all", methods=["GET"])
def collisions():
    df = _load_collision_data()
    if df.empty:
        return jsonify({"count": 0, "total_count": 0, "page": 1,
                        "per_page": 50, "total_pages": 0, "collisions": []})

    # Filter
    risk_level = request.args.get("risk_level")
    if risk_level and risk_level.upper() != "ALL" and "risk_level" in df.columns:
        df = df[df["risk_level"] == risk_level.upper()]

    # Sort
    sort_by = request.args.get("sort_by", "distance_km")
    sort_order = request.args.get("sort_order", "asc")
    if sort_by in df.columns:
        df = df.sort_values(sort_by, ascending=(sort_order == "asc"))

    # Paginate
    total = len(df)
    page = max(1, int(request.args.get("page", 1)))
    per_pg = min(200, max(1, int(request.args.get("per_page", 50))))
    start = (page - 1) * per_pg
    page_df = df.iloc[start:start + per_pg]

    records = []
    for _, row in page_df.iterrows():
        n1 = str(row.get("norad_id_1", ""))
        n2 = str(row.get("norad_id_2", ""))
        records.append({
            # Reference-compatible
            "satellite_1_id":        n1,
            "satellite_2_id":        n2,
            "satellite_1_name":      str(row.get("object_name_1", n1)),
            "satellite_2_name":      str(row.get("object_name_2", n2)),
            "miss_distance_km":      _safe_float(row.get("distance_km")),
            "relative_velocity_kms": _safe_float(row.get("relative_velocity_kms")),
            "risk_level":            str(row.get("risk_level", "LOW")),
            "collision_probability": _safe_float(row.get("collision_probability")),
            "predicted_time":        str(row.get("detection_timestamp", "")),
            "collision_type":        str(row.get("collision_type", "")),
            "is_active":             True,
            # Original field names
            "norad_id_1":            n1,
            "norad_id_2":            n2,
            "distance_km":           _safe_float(row.get("distance_km")),
            "type_1":                str(row.get("type_1", "UNKNOWN")),
            "type_2":                str(row.get("type_2", "UNKNOWN")),
            "altitude_km_1":         _safe_float(row.get("altitude_km_1")),
            "altitude_km_2":         _safe_float(row.get("altitude_km_2")),
            "detection_timestamp":   str(row.get("detection_timestamp", "")),
            "approach_position_x":   _safe_float(row.get("pos_x_1")),
            "approach_position_y":   _safe_float(row.get("pos_y_1")),
            "approach_position_z":   _safe_float(row.get("pos_z_1")),
        })

    return jsonify({
        "count":          len(records),
        "total_count":    total,
        "page":           page,
        "per_page":       per_pg,
        "total_pages":    math.ceil(total / per_pg) if per_pg else 1,
        "collisions":     records,
        "simulated_time": datetime.utcnow().isoformat() + "Z",
    })


@app.route("/api/collisions/globe", methods=["GET"])
def collisions_globe():
    df = _load_collision_data()
    if df.empty:
        return jsonify({"count": 0, "collisions": []})

    risk_level = request.args.get("risk_level")
    if risk_level and risk_level.upper() != "ALL" and "risk_level" in df.columns:
        df = df[df["risk_level"] == risk_level.upper()]

    if "distance_km" in df.columns:
        df = df.sort_values("distance_km", ascending=True)

    max_records = min(500, int(request.args.get("limit", 500)))
    df = df.head(max_records)

    records = []
    for _, row in df.iterrows():
        # Compute approach position as midpoint of both objects
        x1 = _safe_float(row.get("pos_x_1"))
        x2 = _safe_float(row.get("pos_x_2"))
        y1 = _safe_float(row.get("pos_y_1"))
        y2 = _safe_float(row.get("pos_y_2"))
        z1 = _safe_float(row.get("pos_z_1"))
        z2 = _safe_float(row.get("pos_z_2"))
        ap_x = ((x1 + x2) / 2) if x1 is not None and x2 is not None else None
        ap_y = ((y1 + y2) / 2) if y1 is not None and y2 is not None else None
        ap_z = ((z1 + z2) / 2) if z1 is not None and z2 is not None else None

        records.append({
            "norad_id_1":            str(row.get("norad_id_1", "")),
            "norad_id_2":            str(row.get("norad_id_2", "")),
            "type_1":                str(row.get("type_1", "UNKNOWN")),
            "type_2":                str(row.get("type_2", "UNKNOWN")),
            "distance_km":           _safe_float(row.get("distance_km")),
            "risk_level":            str(row.get("risk_level", "LOW")),
            "collision_probability": _safe_float(row.get("collision_probability")),
            "collision_type":        str(row.get("collision_type", "")),
            "relative_velocity_kms": _safe_float(row.get("relative_velocity_kms")),
            "detection_timestamp":   str(row.get("detection_timestamp", "")),
            "approach_position_x":   ap_x,
            "approach_position_y":   ap_y,
            "approach_position_z":   ap_z,
        })

    return jsonify({"count": len(records), "collisions": records})


@app.route("/api/collisions/high-risk", methods=["GET"])
def high_risk_collisions():
    df = _load_collision_data()
    if df.empty:
        return jsonify({"count": 0, "high_risk_collisions": []})

    if "risk_level" in df.columns:
        df = df[df["risk_level"].isin(["CRITICAL", "HIGH", "MEDIUM"])]

    if "distance_km" in df.columns:
        df = df.sort_values("distance_km", ascending=True).head(50)

    records = []
    for _, row in df.iterrows():
        n1 = str(row.get("norad_id_1", ""))
        n2 = str(row.get("norad_id_2", ""))
        records.append({
            "satellite_1_id":        n1,
            "satellite_2_id":        n2,
            "satellite_1_name":      str(row.get("object_name_1", n1)),
            "satellite_2_name":      str(row.get("object_name_2", n2)),
            "norad_id_1":            n1,
            "norad_id_2":            n2,
            "type_1":                str(row.get("type_1", "UNKNOWN")),
            "type_2":                str(row.get("type_2", "UNKNOWN")),
            "distance_km":           _safe_float(row.get("distance_km")),
            "miss_distance_km":      _safe_float(row.get("distance_km")),
            "risk_level":            str(row.get("risk_level", "LOW")),
            "collision_type":        str(row.get("collision_type", "")),
            "collision_probability": _safe_float(row.get("collision_probability")),
        })

    return jsonify({"count": len(records), "high_risk_collisions": records})


@app.route("/api/collisions/frequency", methods=["GET"])
def collisions_frequency():
    df = _load_collision_data()
    if df.empty:
        return jsonify({"count": 0, "pairs": []})

    limit = min(100, int(request.args.get("limit", 20)))

    if "norad_id_1" not in df.columns or "norad_id_2" not in df.columns:
        return jsonify({"count": 0, "pairs": []})

    grp = (
        df.groupby(["norad_id_1", "norad_id_2"])
        .agg(
            collision_count=("distance_km", "count"),
            min_distance_km=("distance_km", "min"),
            avg_distance_km=("distance_km", "mean"),
            max_distance_km=("distance_km", "max"),
        )
        .reset_index()
        .sort_values(["collision_count", "min_distance_km"], ascending=[False, True])
        .head(limit)
    )

    pairs = []
    for _, row in grp.iterrows():
        n1 = str(row["norad_id_1"])
        n2 = str(row["norad_id_2"])
        pairs.append({
            "satellite_1_id":   n1,
            "satellite_2_id":   n2,
            "satellite_1_name": n1,
            "satellite_2_name": n2,
            "approach_events":  int(row["collision_count"]),
            "collision_count":  int(row["collision_count"]),
            "min_distance_km":  _safe_float(row["min_distance_km"]),
            "avg_distance_km":  _safe_float(row["avg_distance_km"]),
            "max_distance_km":  _safe_float(row["max_distance_km"]),
        })

    return jsonify({"count": len(pairs), "pairs": pairs})


@app.route("/api/debris", methods=["GET"])
def get_debris():
    catalog = _load_debris_catalog()
    if catalog.empty:
        return jsonify({"count": 0, "debris": []})

    limit = min(5000, int(request.args.get("limit", 1000)))
    catalog = catalog.head(limit)

    records = []
    for _, row in catalog.iterrows():
        records.append({
            "norad_id":    str(row.get("NORAD_CAT_ID", "")),
            "name":        str(row.get("OBJECT_NAME", "")),
            "country":     str(row.get("COUNTRY", "")),
            "launch":      str(row.get("LAUNCH", "")),
            "period":      _safe_float(row.get("PERIOD")),
            "inclination": _safe_float(row.get("INCLINATION")),
            "apogee":      _safe_float(row.get("APOGEE")),
            "perigee":     _safe_float(row.get("PERIGEE")),
            "rcs_size":    str(row.get("RCS_SIZE", "")),
            "object_type": str(row.get("OBJECT_TYPE", "")),
        })

    return jsonify({"count": len(records), "debris": records})


@app.route("/api/debris/<norad_id>", methods=["GET"])
def get_debris_by_id(norad_id: str):
    catalog = _load_debris_catalog()
    if catalog.empty:
        return jsonify({"error": "Catalog not loaded"}), 404

    row = catalog[catalog["NORAD_CAT_ID"].astype(str) == norad_id]
    if row.empty:
        return jsonify({"error": f"NORAD ID {norad_id} not found"}), 404

    r = row.iloc[0].to_dict()
    return jsonify({k: (None if (isinstance(v, float) and math.isnan(v)) else v)
                    for k, v in r.items()})


# ─── ML + Pipeline endpoints ────────────────────────────────────────────────

@app.route("/api/ml/metrics", methods=["GET"])
def ml_metrics():
    """Return ML training metrics from HDFS."""
    metrics = _load_ml_metrics()
    if not metrics:
        return jsonify({"status": "no_data", "metrics": {}})
    return jsonify({"status": "ok", "metrics": metrics})


@app.route("/api/pipeline/status", methods=["GET"])
def pipeline_status():
    """Return pipeline run history."""
    df = _load_pipeline_metrics()
    if df.empty:
        return jsonify({"status": "no_runs", "runs": []})

    runs = []
    for _, row in df.iterrows():
        runs.append({k: (None if isinstance(v, float) and math.isnan(v) else v)
                     for k, v in row.to_dict().items()})

    return jsonify({
        "status":     "ok",
        "total_runs": len(runs),
        "runs":       runs[-10:],  # Last 10 runs
    })


@app.route("/api/simulation/time", methods=["GET"])
def simulation_time():
    now = datetime.utcnow()
    return jsonify({
        "current_simulated_time": now.isoformat() + "Z",
        "elapsed_simulated_days": 0,
        "simulation_mode":        "real-time",
    })


if __name__ == "__main__":
    port = int(os.getenv("DASHBOARD_API_PORT", 5050))
    logger.info(f"🚀 Space Debris Dashboard API on http://0.0.0.0:{port}")
    logger.info(f"   HDFS: {HDFS_NAMENODE}:{WEBHDFS_PORT}{HDFS_BASE}")
    logger.info(f"   Local fallback: {LOCAL_OUTPUT_DIR}")
    app.run(host="0.0.0.0", port=port, debug=False)
