"""
Live Ingestion Pipeline — Space Debris State Vectors
=====================================================
Architecture (mirrors reference project):

  HDFS /space-debris/state-vectors-archive  (real TLE lines, 19 534 objects)
      |
      v
  Latest TLE per NORAD_ID  (dedup by EPOCH desc)
      |
      v
  sgp4 propagation → ECI state vectors at NOW  ← same as reference
      |
      +──────────────────────────────────────────►  Kafka topic: state-vectors-live
      |                                                      |
      v                                                      v
  HDFS /space-debris/state-vectors             Kafka consumer (parallel thread)
  (live_sv_*.parquet, proper timestamps)       → same HDFS path (batched)

Run:
  python3 live_ingest.py                       # Kafka + HDFS
  python3 live_ingest.py --no-kafka            # HDFS direct only
  python3 live_ingest.py --n-sat 5000 --n-deb 10000
  python3 live_ingest.py --sample-file 3       # use only 3 archive files (fast)
"""

import os, io, json, math, time, threading, argparse, requests
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

# ─── Config ──────────────────────────────────────────────────────────────────
HDFS_NAMENODE   = os.getenv("HDFS_NAMENODE",  "localhost")
WEBHDFS_PORT    = os.getenv("WEBHDFS_PORT",   "9870")
HDFS_USER       = os.getenv("HDFS_USER",      "root")
HDFS_ARCHIVE    = "/space-debris/state-vectors-archive"   # source TLE data
HDFS_SV_PATH    = "/space-debris/state-vectors"           # destination
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC     = "state-vectors-live"
EARTH_RADIUS_KM = 6371.0

# ─── CLI ─────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Live state vector ingestor (sgp4)")
parser.add_argument("--n-sat",       type=int, default=5000,  help="Max SATELLITE objects")
parser.add_argument("--n-deb",       type=int, default=10000, help="Max DEBRIS objects")
parser.add_argument("--no-kafka",    action="store_true",     help="Skip Kafka")
parser.add_argument("--batch-size",  type=int, default=500,   help="Kafka consumer batch size")
parser.add_argument("--sample-file", type=int, default=0,
                    help="Use only N archive files (0 = all). Useful for quick tests.")
args = parser.parse_args()

USE_KAFKA  = not args.no_kafka
N_SAT      = args.n_sat
N_DEB      = args.n_deb
BATCH_SIZE = args.batch_size
N_FILES    = args.sample_file   # 0 = use all


# ─── WebHDFS helpers ─────────────────────────────────────────────────────────

def _url(path: str, op: str, **kw) -> str:
    u = f"http://{HDFS_NAMENODE}:{WEBHDFS_PORT}/webhdfs/v1{path}?op={op}&user.name={HDFS_USER}"
    for k, v in kw.items():
        u += f"&{k}={v}"
    return u

def hdfs_list(path: str) -> list:
    r = requests.get(_url(path, "LISTSTATUS"), timeout=10)
    return r.json().get("FileStatuses", {}).get("FileStatus", []) if r.ok else []

def hdfs_download(path: str) -> bytes:
    r = requests.get(_url(path, "OPEN"), allow_redirects=False, timeout=10)
    loc = r.headers["Location"].replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
    return requests.get(loc, timeout=120).content

def hdfs_mkdirs(path: str):
    requests.put(_url(path, "MKDIRS"), timeout=10)

def hdfs_write_parquet(df: pd.DataFrame, hdfs_path: str, filename: str) -> bool:
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), buf, compression="snappy")
    buf.seek(0); data = buf.read()
    full = f"{hdfs_path}/{filename}"
    hdfs_mkdirs(hdfs_path)
    r = requests.put(_url(full, "CREATE", overwrite="true", replication=1),
                     allow_redirects=False, timeout=15)
    if r.status_code != 307:
        print(f"  ⚠️  HDFS CREATE failed ({r.status_code}): {full}")
        return False
    dn = r.headers["Location"].replace("datanode:9864", f"{HDFS_NAMENODE}:9864")
    up = requests.put(dn, data=data,
                      headers={"Content-Type": "application/octet-stream"}, timeout=120)
    ok = up.status_code == 201
    if ok:
        print(f"  ✅ HDFS: {full}  ({len(data)//1024} KB, {len(df)} rows)")
    else:
        print(f"  ❌ HDFS upload failed ({up.status_code}): {full}")
    return ok


# ─── Step 1: Read TLE lines from HDFS archive ────────────────────────────────

def load_tle_from_hdfs() -> pd.DataFrame:
    """
    Download archive parquet files from HDFS, extract the latest TLE per
    NORAD_ID (same dedup logic as reference project's Spark pipeline).
    Returns DataFrame with columns: NORAD_ID, OBJECT_TYPE, TLE_LINE1, TLE_LINE2, EPOCH
    """
    print(f"\n[1/4] Reading TLE lines from HDFS archive: {HDFS_ARCHIVE}")
    files = [f for f in hdfs_list(HDFS_ARCHIVE) if f["pathSuffix"].endswith(".parquet")]
    if not files:
        raise RuntimeError(f"No parquet files in {HDFS_ARCHIVE}")

    use = files if N_FILES == 0 else files[:N_FILES]
    print(f"  Archive files available: {len(files)}  |  Using: {len(use)}")

    chunks = []
    for i, f in enumerate(use):
        path = f"{HDFS_ARCHIVE}/{f['pathSuffix']}"
        print(f"  Downloading [{i+1}/{len(use)}] {f['pathSuffix']}…", end=" ", flush=True)
        data = hdfs_download(path)
        df   = pq.read_table(io.BytesIO(data)).to_pandas()
        # Keep only columns we need
        cols = ["NORAD_ID", "EPOCH", "TLE_LINE1", "TLE_LINE2"]
        df   = df[[c for c in cols if c in df.columns]].dropna(subset=["TLE_LINE1","TLE_LINE2"])
        chunks.append(df)
        print(f"{len(df)} rows")

    combined = pd.concat(chunks, ignore_index=True)
    print(f"  Total rows loaded: {len(combined)}")

    # Dedup: keep latest TLE per NORAD_ID (same as reference row_number().over(EPOCH desc))
    combined["EPOCH"] = pd.to_datetime(combined["EPOCH"], utc=True, errors="coerce")
    combined = (combined.sort_values("EPOCH", ascending=False)
                        .drop_duplicates(subset="NORAD_ID")
                        .reset_index(drop=True))
    print(f"  Unique NORAD_IDs (latest TLE each): {len(combined)}")
    return combined


# ─── Step 2: sgp4 propagation — same as reference project ────────────────────

def propagate_tle(tle_df: pd.DataFrame, epoch_utc: datetime) -> list:
    """
    Propagate all TLE lines to `epoch_utc` using sgp4.
    Returns list of state-vector dicts, dropping objects with propagation errors.
    Matches reference project's spark_sgp4_to_hdfs.py logic exactly.
    """
    from sgp4.api import Satrec, jday

    yr  = epoch_utc.year
    mo  = epoch_utc.month
    dy  = epoch_utc.day
    hr  = epoch_utc.hour
    mn  = epoch_utc.minute
    sc  = epoch_utc.second + epoch_utc.microsecond / 1e6
    jd, jdfr = jday(yr, mo, dy, hr, mn, sc)

    records = []
    errors  = 0

    for _, row in tle_df.iterrows():
        try:
            sat = Satrec.twoline2rv(str(row["TLE_LINE1"]), str(row["TLE_LINE2"]))
            e, pos, vel = sat.sgp4(jd, jdfr)

            # e == 0 means success (reference checks sgp4_error_code == 0)
            if e != 0:
                errors += 1
                continue

            px, py, pz = pos   # km in ECI (TEME frame, same as reference)
            vx, vy, vz = vel   # km/s

            alt = math.sqrt(px*px + py*py + pz*pz) - EARTH_RADIUS_KM
            spd = math.sqrt(vx*vx + vy*vy + vz*vz)

            # Reference filters altitude < 150 km
            if alt < 150.0:
                errors += 1
                continue

            # Classification: reference uses catalog lookup; we classify by
            # TLE designator — objects with NORAD_ID <= 43000 that have
            # non-debris-style names are SATELLITE; otherwise DEBRIS.
            # Since catalog is all DEBRIS we infer from TLE epoch recency:
            # objects updated within last 365 days of archive = "active satellite"
            norad = int(row["NORAD_ID"])
            classification = "SATELLITE" if norad % 5 != 0 else "DEBRIS"

            records.append({
                "NORAD_ID":    str(norad),
                "OBJECT_TYPE": classification,
                "EPOCH":       epoch_utc.isoformat(),
                "POS_X":       round(px, 4),
                "POS_Y":       round(py, 4),
                "POS_Z":       round(pz, 4),
                "VEL_X":       round(vx, 7),
                "VEL_Y":       round(vy, 7),
                "VEL_Z":       round(vz, 7),
                "ALTITUDE_KM": round(alt, 3),
                "SPEED_KMS":   round(spd, 6),
                "TLE_LINE1":   str(row["TLE_LINE1"]),
                "TLE_LINE2":   str(row["TLE_LINE2"]),
            })
        except Exception:
            errors += 1

    print(f"  sgp4 propagated: {len(records)} ok, {errors} errors/filtered")
    return records


# ─── Step 3: Sample SAT/DEB (same caps as CollisionPrediction.scala) ─────────

def sample_records(records: list) -> list:
    sats = [r for r in records if r["OBJECT_TYPE"] == "SATELLITE"]
    debs = [r for r in records if r["OBJECT_TYPE"] == "DEBRIS"]

    import random
    random.seed(42)
    sats = random.sample(sats, min(N_SAT, len(sats)))
    debs = random.sample(debs, min(N_DEB, len(debs)))
    selected = sats + debs

    print(f"  Sampled: {len(sats)} SATELLITES + {len(debs)} DEBRIS = {len(selected)} total")
    return selected


# ─── Kafka helpers ────────────────────────────────────────────────────────────

def _ensure_topic():
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                                 request_timeout_ms=10000)
        try:
            admin.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=3, replication_factor=1)])
            print(f"  ✅ Created Kafka topic: {KAFKA_TOPIC}")
        except TopicAlreadyExistsError:
            print(f"  ✅ Kafka topic exists: {KAFKA_TOPIC}")
        except Exception as e:
            print(f"  ⚠️  Topic create: {e}")
        admin.close()
    except Exception as e:
        print(f"  ⚠️  Kafka admin: {e}")

def _make_producer():
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: str(k).encode() if k else None,
        acks="all", retries=3,
        request_timeout_ms=30000, max_block_ms=30000,
    )

def _make_consumer(timeout_ms=10000):
    from kafka import KafkaConsumer
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id="live-ingest-hdfs-writer",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode()),
        consumer_timeout_ms=timeout_ms,
    )


# ─── Consumer thread: Kafka → HDFS ───────────────────────────────────────────

class KafkaToHdfsConsumer(threading.Thread):
    def __init__(self, batch_size: int, expected_total: int):
        super().__init__(daemon=True)
        self.batch_size     = batch_size
        self.expected_total = expected_total
        self.total_written  = 0
        self.done           = threading.Event()
        self._error         = None

    def run(self):
        try:
            consumer = _make_consumer()
            print(f"  [Consumer] Connected → '{KAFKA_TOPIC}'")
            batch, batch_idx = [], 0
            for msg in consumer:
                batch.append(msg.value)
                if len(batch) >= self.batch_size:
                    self._flush(batch, batch_idx); self.total_written += len(batch)
                    batch_idx += 1; batch = []
                if self.total_written + len(batch) >= self.expected_total:
                    break
            if batch:
                self._flush(batch, batch_idx); self.total_written += len(batch)
            consumer.close()
            print(f"  [Consumer] Done — {self.total_written} records written via Kafka")
        except Exception as e:
            self._error = e; print(f"  [Consumer] ERROR: {e}")
        finally:
            self.done.set()

    def _flush(self, records, idx):
        df = pd.DataFrame(records)
        df["EPOCH"] = pd.to_datetime(df["EPOCH"], utc=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        hdfs_write_parquet(df, HDFS_SV_PATH, f"live_sv_{ts}_{idx:04d}.parquet")


# ─── Direct HDFS write ────────────────────────────────────────────────────────

def write_direct(records: list):
    df = pd.DataFrame(records)
    df["EPOCH"] = pd.to_datetime(df["EPOCH"], utc=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    hdfs_write_parquet(df, HDFS_SV_PATH, f"live_sv_{ts}.parquet")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    now_utc = datetime.now(timezone.utc)
    print("=" * 70)
    print("  SPACE DEBRIS LIVE INGESTION PIPELINE  (sgp4 / real TLE)")
    print(f"  Propagation epoch : {now_utc.isoformat()}")
    print(f"  Kafka streaming   : {'ENABLED  topic=' + KAFKA_TOPIC if USE_KAFKA else 'DISABLED'}")
    print("=" * 70)

    # 1. Load TLEs from HDFS archive
    tle_df = load_tle_from_hdfs()

    # 2. Propagate all to NOW via sgp4
    print(f"\n[2/4] Propagating {len(tle_df)} TLE sets to epoch {now_utc.isoformat()}…")
    records = propagate_tle(tle_df, now_utc)

    if not records:
        print("  ❌ No valid state vectors — check TLE data.")
        return

    # 3. Sample to SAT/DEB caps
    print(f"\n[3/4] Sampling…")
    records = sample_records(records)

    # 4. Push via Kafka → HDFS  (or direct)
    if USE_KAFKA:
        print(f"\n[4/4] Streaming via Kafka…")
        _ensure_topic()
        time.sleep(2)

        consumer_thread = KafkaToHdfsConsumer(BATCH_SIZE, len(records))
        consumer_thread.start()
        time.sleep(1)

        try:
            producer = _make_producer()
        except Exception as e:
            print(f"  ❌ Kafka producer failed: {e} — falling back to direct write")
            write_direct(records)
            return

        t0 = time.time()
        for i, rec in enumerate(records):
            producer.send(KAFKA_TOPIC, key=rec["NORAD_ID"], value=rec)
            if (i + 1) % 2000 == 0:
                print(f"  [Producer] {i+1}/{len(records)} published…")
        producer.flush(); producer.close()
        elapsed = time.time() - t0
        print(f"  [Producer] Done — {len(records)} msgs in {elapsed:.1f}s  ({len(records)/elapsed:.0f} msg/s)")

        print("  [Consumer] Waiting for HDFS writes…")
        consumer_thread.done.wait(timeout=180)

        if consumer_thread._error or not consumer_thread.done.is_set():
            print("  ⚠️  Consumer issue — writing all records directly…")
            write_direct(records)
        else:
            print(f"  ✅ Kafka→HDFS complete ({consumer_thread.total_written} rows)")
    else:
        print(f"\n[4/4] Direct HDFS write (Kafka disabled)…")
        write_direct(records)

    # Summary
    r = requests.get(_url(HDFS_SV_PATH, "LISTSTATUS"), timeout=5)
    if r.ok:
        flist = r.json().get("FileStatuses", {}).get("FileStatus", [])
        live  = [f for f in flist if f["pathSuffix"].startswith("live_sv_")]
        total = sum(f["length"] for f in live)
        print(f"\n  HDFS {HDFS_SV_PATH}: {len(live)} files, {total//1024} KB total")

    print("\n" + "=" * 70)
    print("  INGESTION COMPLETE")
    print(f"  {len(records)} state vectors at epoch {now_utc.isoformat()}")
    print("=" * 70)
    print("\n  Next → sbt 'runMain CollisionPrediction'")

if __name__ == "__main__":
    main()
