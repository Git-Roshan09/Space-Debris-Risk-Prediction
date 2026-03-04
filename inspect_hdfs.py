import requests, io, pyarrow.parquet as pq

NAMENODE = "localhost"
PORT = "9870"
USER = "root"

def download(path):
    r = requests.get(
        f"http://{NAMENODE}:{PORT}/webhdfs/v1{path}?op=OPEN&user.name={USER}",
        allow_redirects=False, timeout=10
    )
    loc = r.headers["Location"].replace("datanode:9864", f"{NAMENODE}:9864")
    return requests.get(loc, timeout=60).content

# --- Archive file (original pipeline output) ---
print("=== ARCHIVED (original pipeline) ===")
data = download("/space-debris/state-vectors-archive/part-00000-1087f85e-a367-494a-965d-04b3457f88a3-c000.snappy.parquet")
tbl = pq.read_table(io.BytesIO(data))
df  = tbl.to_pandas()
print("Schema:", tbl.schema)
print("Rows:", len(df))
print(df.head(3).to_string())
print()

# --- Live file (live_ingest.py output) ---
print("=== LIVE (live_ingest.py output) ===")
r = requests.get(f"http://{NAMENODE}:{PORT}/webhdfs/v1/space-debris/state-vectors?op=LISTSTATUS&user.name={USER}")
files = sorted(
    [f for f in r.json()["FileStatuses"]["FileStatus"] if f["pathSuffix"].startswith("live_sv_")],
    key=lambda x: x["pathSuffix"]
)
data2 = download(f"/space-debris/state-vectors/{files[-1]['pathSuffix']}")
tbl2  = pq.read_table(io.BytesIO(data2))
df2   = tbl2.to_pandas()
print("Schema:", tbl2.schema)
print("Rows:", len(df2))
print(df2.head(3).to_string())
print()
print("OBJECT_TYPE distribution:", df2["OBJECT_TYPE"].value_counts().to_dict() if "OBJECT_TYPE" in df2.columns else "MISSING")
