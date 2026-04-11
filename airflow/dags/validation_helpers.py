"""
airflow/dags/validation_helpers.py

Tier-2 near-real-time data quality helper for Bronze Parquet files.

validate_bronze() is called by Airflow tasks that run BEFORE each push_*
task.  It reads the local Bronze Parquet directory into pandas, then applies
Great Expectations validations using the ge.from_pandas() API.

Failure modes:
  - Critical (null PKs, schema drift): raises RuntimeError → Airflow task
    fails and blocks the upstream push task (data never reaches Databricks).
  - Soft warning (anomalous values): logs warning, task succeeds, push proceeds.
"""
from __future__ import annotations

import glob as _glob
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def validate_bronze(file_dir: str, expectations: list[dict]) -> None:
    """
    Read all Parquet files in ``file_dir`` into a single DataFrame and run
    the provided GE expectations using an ephemeral (in-memory) context.

    Parameters
    ----------
    file_dir : str
        Local path to a Bronze directory (e.g. /opt/airflow/data/bronze/btc_trades).
    expectations : list[dict]
        Each dict has the following keys:

        - ``"expectation"``  : str  — GE method name on PandasDataset
                               (e.g. "expect_column_values_to_not_be_null")
        - ``"kwargs"``       : dict — keyword arguments forwarded verbatim
        - ``"critical"``     : bool — True → RuntimeError on failure (blocks upload)
        - ``"description"``  : str  — human-readable label used in log messages

    Raises
    ------
    RuntimeError
        If one or more expectations marked ``critical=True`` fail.
    """
    import great_expectations as ge

    parquet_files = _glob.glob(f"{file_dir}/*.parquet")
    if not parquet_files:
        logger.info("validate_bronze: %s — no Parquet files yet, skipping.", file_dir)
        return

    df = pd.read_parquet(file_dir, engine="pyarrow")
    if df.empty:
        logger.info("validate_bronze: %s — DataFrame is empty, skipping.", file_dir)
        return

    ge_df = ge.from_pandas(df)
    critical_failures: list[str] = []

    for exp in expectations:
        method_name = exp["expectation"]
        kwargs      = exp.get("kwargs", {})
        is_critical = exp.get("critical", False)
        desc        = exp.get("description", method_name)

        method = getattr(ge_df, method_name, None)
        if method is None:
            logger.warning("validate_bronze: unknown GE expectation '%s' — skipping.", method_name)
            continue

        result  = method(**kwargs)
        success = result.get("success", False)

        if not success:
            stats = result.get("result", {})
            level = "CRITICAL" if is_critical else "WARN"
            msg   = f"[GE][{level}] {desc} FAILED — {stats}"
            if is_critical:
                logger.error(msg)
                critical_failures.append(desc)
            else:
                logger.warning(msg)
        else:
            logger.info("[GE][OK] %s", desc)

    if critical_failures:
        raise RuntimeError(
            f"validate_bronze: {len(critical_failures)} critical expectation(s) failed "
            f"in {file_dir}: {critical_failures}"
        )
