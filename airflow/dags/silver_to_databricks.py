"""
DAG: silver_to_databricks
Schedule: every 5 minutes

Tasks (run in parallel per table, then converge on dbt):

  push_trades    → copy_trades    → run_dbt_silver
  push_klines    → copy_klines    ↘
  push_ticker    → copy_ticker    → run_dbt_gold
  push_orderbook → copy_orderbook ↗

Each push task reads new Bronze Parquet records since its own cursor (tracked in
STATE_FILE), uploads a batch to a Unity Catalog volume, and advances the cursor.

COPY INTO is idempotent: Databricks tracks loaded files and skips duplicates on retry.
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from airflow.decorators import dag, task
from datetime import timedelta


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BRONZE_TRADES_DIR   = "/opt/airflow/data/bronze/btc_trades"
BRONZE_KLINES_DIR   = "/opt/airflow/data/bronze/btc_klines"
BRONZE_TICKER_DIR   = "/opt/airflow/data/bronze/btc_ticker"
BRONZE_ORDERBOOK_DIR = "/opt/airflow/data/bronze/btc_orderbook"

STATE_FILE = "/opt/airflow/data/.push_state.json"
DBT_DIR    = "/opt/airflow/dbt"

# ---------------------------------------------------------------------------
# Databricks credentials (injected from .env via docker-compose env_file)
# ---------------------------------------------------------------------------
_raw_host               = os.environ["DATABRICKS_HOST"].rstrip("/")
DATABRICKS_HOST         = _raw_host if _raw_host.startswith("http") else f"https://{_raw_host}"
DATABRICKS_TOKEN        = os.environ["DATABRICKS_TOKEN"]
DATABRICKS_WAREHOUSE_ID = os.environ["DATABRICKS_WAREHOUSE_ID"]

# Unity Catalog coordinates
UC_CATALOG   = "crypto"
UC_SCHEMA    = "btc"
UC_VOLUME    = "airflow_stage"

# Staging base inside the UC volume
VOLUME_BASE = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{UC_VOLUME}/bronze"


# ---------------------------------------------------------------------------
# Helpers — Databricks API
# ---------------------------------------------------------------------------

def _run_sql(statement: str) -> None:
    """Execute a SQL statement on Databricks via the Statement Execution API."""
    base_url = DATABRICKS_HOST + "/api/2.0/sql/statements"
    headers  = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

    resp = requests.post(
        base_url,
        json={
            "statement":    statement,
            "warehouse_id": DATABRICKS_WAREHOUSE_ID,
            "catalog":      UC_CATALOG,
            "schema":       UC_SCHEMA,
            "wait_timeout": "30s",
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

    stmt_id = result["statement_id"]
    for _ in range(60):
        time.sleep(5)
        poll = requests.get(f"{base_url}/{stmt_id}", headers=headers, timeout=30)
        poll.raise_for_status()
        state = poll.json()["status"]["state"]
        if state == "SUCCEEDED":
            return
        if state in ("FAILED", "CANCELED", "CLOSED"):
            raise RuntimeError(f"SQL statement failed: {poll.json()['status'].get('error', poll.json())}")

    raise TimeoutError(f"SQL statement {stmt_id} did not finish within 300 s")


def _upload_bytes_to_volume(data: bytes, volume_path: str) -> None:
    """Upload a file to a Unity Catalog volume via the Databricks Files API."""
    api_path = volume_path.lstrip("/")
    url = f"{DATABRICKS_HOST}/api/2.0/fs/files/{api_path}"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream",
    }
    resp = requests.put(url, params={"overwrite": "true"}, headers=headers, data=data, timeout=120)
    resp.raise_for_status()


def _ensure_stage_volume() -> None:
    _run_sql(f"CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{UC_VOLUME}")


# ---------------------------------------------------------------------------
# Helpers — state cursor (per-table, backward-compatible with old format)
# ---------------------------------------------------------------------------

def _read_cursor(table: str) -> pd.Timestamp:
    """Return the last-pushed ingested_at cursor for this table."""
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        # backward-compat: old format had {"last_ingested_at": "..."} at the top level
        if table == "btc_trades" and table not in data and "last_ingested_at" in data:
            return pd.Timestamp(data["last_ingested_at"], tz="UTC")
        return pd.Timestamp(data.get(table, "1970-01-01"), tz="UTC")
    except (FileNotFoundError, KeyError, ValueError):
        return pd.Timestamp("1970-01-01", tz="UTC")


def _write_cursor(table: str, ts: pd.Timestamp) -> None:
    """Advance the cursor for this table."""
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        # migrate legacy format
        if "last_ingested_at" in data and "btc_trades" not in data:
            data["btc_trades"] = data.pop("last_ingested_at")
    except (FileNotFoundError, ValueError):
        data = {}
    data[table] = ts.isoformat()
    with open(STATE_FILE, "w") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# Generic push + copy helpers (called inside task functions)
# ---------------------------------------------------------------------------

def _push_bronze_table(
    bronze_dir: str,
    table_key: str,
    volume_subdir: str,
) -> str | None:
    """
    Read new Bronze records since last push, upload to UC Volume.
    Returns the staged volume path, or None if nothing new.
    """
    import glob as glob_mod

    last_pushed = _read_cursor(table_key)
    print(f"[{table_key}] Last pushed cursor: {last_pushed}")

    parquet_files = glob_mod.glob(f"{bronze_dir}/*.parquet")
    if not parquet_files:
        print(f"[{table_key}] Bronze directory empty — skipping.")
        return None

    df = pd.read_parquet(bronze_dir, engine="pyarrow")
    if df.empty:
        print(f"[{table_key}] No records — skipping.")
        return None

    df["ingested_at"] = pd.to_datetime(df["ingested_at"], utc=True)
    batch = df[df["ingested_at"] > last_pushed].copy()
    if batch.empty:
        print(f"[{table_key}] No new records since {last_pushed}. Skipping.")
        return None

    new_max = batch["ingested_at"].max()
    print(f"[{table_key}] Found {len(batch):,} new record(s) up to {new_max}.")

    buf = io.BytesIO()
    batch.to_parquet(buf, index=False, engine="pyarrow")

    ts_str = new_max.strftime("%Y%m%dT%H%M%SZ")
    staged_path = f"{VOLUME_BASE}/{volume_subdir}/{table_key}_{ts_str}.parquet"

    _ensure_stage_volume()
    print(f"[{table_key}] Uploading {len(buf.getvalue()):,} bytes → {staged_path}")
    _upload_bytes_to_volume(buf.getvalue(), staged_path)

    _write_cursor(table_key, new_max)
    print(f"[{table_key}] Upload done — cursor advanced to {new_max}.")
    return staged_path


def _copy_into_landing(
    staged_path: str | None,
    table_name: str,
    create_ddl: str,
) -> str | None:
    """COPY INTO a landing Delta table from staged Parquet. Idempotent."""
    # Always ensure the table exists so downstream dbt models never hit
    # TABLE_OR_VIEW_NOT_FOUND even on the first run before any data arrives.
    _run_sql(create_ddl)
    if staged_path is None:
        print(f"[{table_name}] No new data — skipping COPY INTO.")
        return None
    full_name = f"{UC_CATALOG}.{UC_SCHEMA}.{table_name}"
    _run_sql(f"COPY INTO {full_name} FROM '{staged_path}' FILEFORMAT = PARQUET")
    print(f"[{table_name}] COPY INTO complete: {staged_path}")
    return staged_path


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="silver_to_databricks",
    schedule="*/2 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["crypto", "bronze", "dbt", "databricks"],
)
def silver_to_databricks():

    # ── Trades ──────────────────────────────────────────────────────────────

    @task(task_id="push_trades", retries=2, retry_delay=timedelta(seconds=30))
    def push_trades() -> str | None:
        return _push_bronze_table(BRONZE_TRADES_DIR, "btc_trades", "trades")

    @task(task_id="copy_trades", retries=2, retry_delay=timedelta(seconds=30))
    def copy_trades(staged_path: str | None) -> str | None:
        return _copy_into_landing(
            staged_path,
            "landing_btc_trades",
            f"""
            CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.landing_btc_trades (
                symbol      STRING,
                price       DOUBLE,
                volume      DOUBLE,
                event_time  TIMESTAMP,
                ingested_at TIMESTAMP
            ) USING DELTA
            """,
        )

    @task(task_id="run_dbt_silver", retries=2, retry_delay=timedelta(seconds=30))
    def run_dbt_silver(staged_path: str | None) -> str | None:
        if staged_path is None:
            print("No new trade records — skipping dbt silver.")
            return None
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR, "--select", "silver"],
            capture_output=True, text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt silver failed (exit {result.returncode})")
        return staged_path

    # ── Klines ──────────────────────────────────────────────────────────────

    @task(task_id="push_klines", retries=2, retry_delay=timedelta(seconds=30))
    def push_klines() -> str | None:
        return _push_bronze_table(BRONZE_KLINES_DIR, "btc_klines", "klines")

    @task(task_id="copy_klines", retries=2, retry_delay=timedelta(seconds=30))
    def copy_klines(staged_path: str | None) -> str | None:
        return _copy_into_landing(
            staged_path,
            "landing_btc_klines",
            f"""
            CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.landing_btc_klines (
                symbol                 STRING,
                open_time              TIMESTAMP,
                close_time             TIMESTAMP,
                open_price             DOUBLE,
                high_price             DOUBLE,
                low_price              DOUBLE,
                close_price            DOUBLE,
                volume                 DOUBLE,
                quote_volume           DOUBLE,
                trade_count            BIGINT,
                taker_buy_volume       DOUBLE,
                taker_buy_quote_volume DOUBLE,
                ingested_at            TIMESTAMP
            ) USING DELTA
            """,
        )

    # ── Ticker ──────────────────────────────────────────────────────────────

    @task(task_id="push_ticker", retries=2, retry_delay=timedelta(seconds=30))
    def push_ticker() -> str | None:
        return _push_bronze_table(BRONZE_TICKER_DIR, "btc_ticker", "ticker")

    @task(task_id="copy_ticker", retries=2, retry_delay=timedelta(seconds=30))
    def copy_ticker(staged_path: str | None) -> str | None:
        return _copy_into_landing(
            staged_path,
            "landing_btc_ticker",
            f"""
            CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.landing_btc_ticker (
                symbol             STRING,
                event_time         TIMESTAMP,
                price_change       DOUBLE,
                price_change_pct   DOUBLE,
                weighted_avg_price DOUBLE,
                open_price         DOUBLE,
                high_price         DOUBLE,
                low_price          DOUBLE,
                volume_24h         DOUBLE,
                quote_volume_24h   DOUBLE,
                trade_count_24h    BIGINT,
                ingested_at        TIMESTAMP
            ) USING DELTA
            """,
        )

    # ── Orderbook ───────────────────────────────────────────────────────────

    @task(task_id="push_orderbook", retries=2, retry_delay=timedelta(seconds=30))
    def push_orderbook() -> str | None:
        return _push_bronze_table(BRONZE_ORDERBOOK_DIR, "btc_orderbook", "orderbook")

    @task(task_id="copy_orderbook", retries=2, retry_delay=timedelta(seconds=30))
    def copy_orderbook(staged_path: str | None) -> str | None:
        return _copy_into_landing(
            staged_path,
            "landing_btc_orderbook",
            f"""
            CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.landing_btc_orderbook (
                symbol         STRING,
                event_time     TIMESTAMP,
                best_bid_price DOUBLE,
                best_bid_qty   DOUBLE,
                best_ask_price DOUBLE,
                best_ask_qty   DOUBLE,
                spread         DOUBLE,
                spread_pct     DOUBLE,
                ingested_at    TIMESTAMP
            ) USING DELTA
            """,
        )

    # ── Gold dbt (runs after all 3 new tables are loaded) ───────────────────

    @task(task_id="run_dbt_gold", retries=2, retry_delay=timedelta(seconds=30))
    def run_dbt_gold(
        klines_path: str | None,
        ticker_path: str | None,
        orderbook_path: str | None,
        silver_path: str | None,
    ):
        if all(p is None for p in (klines_path, ticker_path, orderbook_path, silver_path)):
            print("No new data in any table — skipping dbt gold.")
            return
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR, "--select", "gold"],
            capture_output=True, text=True,
        )
        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise RuntimeError(f"dbt gold failed (exit {result.returncode})")

    # ── Wire dependencies ────────────────────────────────────────────────────

    # Trades branch: silver must finish before gold runs (gold_market_pulse joins silver_btc_aggregates)
    t_push  = push_trades()
    t_copy  = copy_trades(t_push)
    silver_result = run_dbt_silver(t_copy)

    # Enrichment branches (run in parallel with trades branch)
    k_copy  = copy_klines(push_klines())
    t_copy2 = copy_ticker(push_ticker())
    o_copy  = copy_orderbook(push_orderbook())

    # gold waits for silver to ensure silver_btc_aggregates is up-to-date
    run_dbt_gold(k_copy, t_copy2, o_copy, silver_result)


silver_to_databricks()
