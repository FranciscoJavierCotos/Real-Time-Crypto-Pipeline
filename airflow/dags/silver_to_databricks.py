"""
DAG: silver_to_databricks
Schedule: every 10 minutes

Tasks:
  1. optimize_silver   — compact small files in the local Silver Delta table (best-effort)
  2. create_table      — CREATE TABLE IF NOT EXISTS in crypto.btc (Unity Catalog)
  3. push_to_dbfs      — read new Silver records for this 10-min window, upload to DBFS
  4. copy_into_table   — COPY INTO the Unity Catalog table from the uploaded parquet file

Task order:
  [optimize_silver, create_table] >> push_to_dbfs >> copy_into_table
  optimize_silver never raises — it is best-effort and must not block the rest.
"""

from __future__ import annotations

import base64
import io
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from airflow.decorators import dag, task
from deltalake import DeltaTable


DELTA_PATH = "/opt/airflow/delta/silver/btc_aggregates"
DBFS_DEST_DIR = "/FileStore/silver/batches"

# Injected from .env via docker-compose env_file
_raw_host = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_HOST = _raw_host if _raw_host.startswith("http") else f"https://{_raw_host}"
DATABRICKS_TOKEN = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

# Unity Catalog target
UC_CATALOG = "crypto"
UC_SCHEMA  = "btc"
UC_TABLE   = "silver_btc_aggregates"
UC_FULL    = f"{UC_CATALOG}.{UC_SCHEMA}.{UC_TABLE}"


def _run_sql(statement: str) -> None:
    """
    Execute a SQL statement on Databricks via the Statement Execution API.
    Polls until completion (up to ~90 s).
    """
    base_url = DATABRICKS_HOST + "/api/2.0/sql/statements"
    headers  = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    resp = requests.post(
        base_url,
        json={
            "statement":     statement,
            "warehouse_id":  DATABRICKS_WAREHOUSE_ID,
            "catalog":       UC_CATALOG,
            "schema":        UC_SCHEMA,
            "wait_timeout":  "30s",   # inline wait; falls back to polling if still running
        },
        headers=headers,
        timeout=60,
    )
    resp.raise_for_status()
    result = resp.json()

    state = result["status"]["state"]
    if state == "SUCCEEDED":
        return
    if state in ("FAILED", "CANCELED", "CLOSED"):
        raise RuntimeError(f"SQL statement failed: {result['status'].get('error', result)}")

    # Still RUNNING or PENDING — poll
    stmt_id = result["statement_id"]
    for _ in range(30):          # 30 × 3 s = 90 s max
        time.sleep(3)
        poll = requests.get(f"{base_url}/{stmt_id}", headers=headers, timeout=30)
        poll.raise_for_status()
        state = poll.json()["status"]["state"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELED", "CLOSED"):
            raise RuntimeError(f"SQL statement failed: {poll.json()['status'].get('error', poll.json())}")

    raise TimeoutError(f"SQL statement {stmt_id} did not finish within 90 s")


def _upload_bytes_to_dbfs(data: bytes, dbfs_path: str) -> None:
    """
    Streaming upload to DBFS: create → add-block (loop, 900KB chunks) → close.
    Works for any file size, unlike the single-call PUT endpoint (1MB cap).
    overwrite=True makes retries idempotent.
    """
    base_url = DATABRICKS_HOST + "/api/2.0/dbfs"
    headers  = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    resp = requests.post(
        f"{base_url}/create",
        json={"path": dbfs_path, "overwrite": True},
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    handle = resp.json()["handle"]

    block_size = 900_000  # 900 KB per block, safely under the 1 MB DBFS limit
    offset = 0
    while offset < len(data):
        chunk = data[offset: offset + block_size]
        resp = requests.post(
            f"{base_url}/add-block",
            json={"handle": handle, "data": base64.b64encode(chunk).decode()},
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
        offset += block_size

    requests.post(
        f"{base_url}/close",
        json={"handle": handle},
        headers=headers,
        timeout=30,
    ).raise_for_status()


@dag(
    dag_id="silver_to_databricks",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,       # don't backfill historical intervals on first run
    max_active_runs=1,   # prevent overlapping runs from racing on the Delta table
    tags=["crypto", "silver", "databricks"],
)
def silver_to_databricks():

    @task(task_id="optimize_silver")
    def optimize_silver():
        """
        Compact small parquet files in the Silver Delta table using delta-rs.
        The 1-second Spark trigger generates many tiny files; OPTIMIZE merges them.
        Uses optimistic concurrency — safe to run alongside the live Spark stream.

        Best-effort: failures are logged but never block downstream tasks.
        """
        try:
            dt = DeltaTable(DELTA_PATH)
            metrics = dt.optimize.compact()
            print(f"Optimize complete. Metrics: {metrics}")
        except Exception as exc:
            print(f"Optimize failed (non-blocking): {exc}")

    @task(task_id="create_table")
    def create_table():
        """
        Ensure the Unity Catalog Delta table exists.
        Safe to run on every DAG execution — IF NOT EXISTS is idempotent.
        """
        _run_sql(f"""
            CREATE TABLE IF NOT EXISTS {UC_FULL} (
                window_start TIMESTAMP,
                window_end   TIMESTAMP,
                symbol       STRING,
                avg_price    DOUBLE,
                total_volume DOUBLE
            )
            USING DELTA
        """)
        print(f"Table {UC_FULL} is ready.")

    @task(task_id="push_to_dbfs")
    def push_to_dbfs(data_interval_start=None, data_interval_end=None) -> str | None:
        """
        Read Silver records whose window_end falls within this scheduling interval,
        then upload them as a single parquet file to Databricks DBFS.

        Returns the DBFS path of the uploaded file, or None if there was nothing to push.
        The return value is passed automatically to copy_into_table via XCom.
        """
        if data_interval_start is None or data_interval_end is None:
            raise ValueError("data_interval_start and data_interval_end are required")

        interval_start = pd.Timestamp(data_interval_start).tz_convert("UTC")
        interval_end   = pd.Timestamp(data_interval_end).tz_convert("UTC")
        print(f"Reading Silver records with window_end in ({interval_start}, {interval_end}]")

        df = DeltaTable(DELTA_PATH).to_pandas()

        if df.empty:
            print("Silver table is empty — skipping upload.")
            return None

        df["window_end"] = pd.to_datetime(df["window_end"], utc=True)

        batch = df[
            (df["window_end"] > interval_start) & (df["window_end"] <= interval_end)
        ].copy()

        if batch.empty:
            print(f"No new Silver records for {interval_start} → {interval_end}. Skipping.")
            return None

        print(f"Found {len(batch)} record(s) to upload.")

        buf = io.BytesIO()
        batch.to_parquet(buf, index=False, engine="pyarrow")
        parquet_bytes = buf.getvalue()

        ts_str    = interval_end.strftime("%Y%m%dT%H%M%SZ")
        dbfs_path = f"{DBFS_DEST_DIR}/silver_{ts_str}.parquet"

        print(f"Uploading {len(parquet_bytes):,} bytes → dbfs:{dbfs_path}")
        _upload_bytes_to_dbfs(parquet_bytes, dbfs_path)
        print(f"Upload complete: dbfs:{dbfs_path}")

        return dbfs_path

    @task(task_id="copy_into_table")
    def copy_into_table(dbfs_path: str | None):
        """
        Load the uploaded parquet file into the Unity Catalog Delta table.
        Skipped automatically if push_to_dbfs found no new records (dbfs_path is None).

        COPY INTO is idempotent when the same file is retried — Databricks tracks
        which files have already been loaded and skips them.
        """
        if dbfs_path is None:
            print("No file uploaded this interval — skipping COPY INTO.")
            return

        _run_sql(f"""
            COPY INTO {UC_FULL}
            FROM 'dbfs:{dbfs_path}'
            FILEFORMAT = PARQUET
        """)
        print(f"COPY INTO complete: dbfs:{dbfs_path} → {UC_FULL}")

    opt  = optimize_silver()
    tbl  = create_table()
    push = push_to_dbfs()
    copy = copy_into_table(push)

    # optimize and create_table run in parallel first;
    # optimize never fails (errors are swallowed), so all_success on push is safe.
    [opt, tbl] >> push >> copy


silver_to_databricks()
