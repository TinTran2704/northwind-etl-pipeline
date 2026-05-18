"""
Spark Clean Job — distributed replacement for src/clean/pipeline.py.

Ports the Kimball Subsystem #4-6 quality screens to PySpark DataFrame API.
Reads raw CSVs, applies column property screens, writes error events to
PostgreSQL staging.error_events via JDBC, and outputs cleaned Parquet files.

Submit:
    spark-submit --master spark://spark-master:7077 \\
        --jars /opt/spark/jars/postgresql-42.7.3.jar \\
        /opt/etl/src/spark_jobs/clean_job.py \\
        <batch_id> <raw_dir> <staging_dir>
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.spark_jobs.common import get_pg_jdbc_properties, get_pg_jdbc_url, get_spark_session

logger = logging.getLogger(__name__)

# PK column per entity — mirrors src/clean/pipeline.py
_ENTITY_PK: dict[str, Optional[str]] = {
    "customers":          "CustomerID",
    "orders":             "OrderID",
    "order-details":      None,
    "products":           "ProductID",
    "categories":         "CategoryID",
    "suppliers":          "SupplierID",
    "employees":          "EmployeeID",
}

# Process order respects referential dependencies
_PROCESSING_ORDER = [
    "categories", "suppliers", "customers", "employees",
    "products", "orders", "order-details",
]

# Parquet subdir name per entity (avoids hyphens in directory names)
_ENTITY_DIR: dict[str, str] = {
    "customers":     "cleaned_customers",
    "orders":        "cleaned_orders",
    "order-details": "cleaned_order_details",
    "products":      "cleaned_products",
    "categories":    "cleaned_categories",
    "suppliers":     "cleaned_suppliers",
    "employees":     "cleaned_employees",
}

_ERROR_EVENTS_TABLE = "staging.error_events"
_CONFIG_PATH = "/opt/etl/config/quality_rules.yaml"


class CleanJobError(Exception):
    """Raised on FATAL violation stopping the batch."""


def _load_rules(config_path: str) -> dict:
    """Load quality_rules.yaml. Returns empty dict if file missing."""
    path = config_path if os.path.exists(config_path) else "config/quality_rules.yaml"
    if not os.path.exists(path):
        logger.warning("[CLEAN-SPARK] quality_rules.yaml not found at %s", path)
        return {}
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def _build_error_row(
    batch_id: str,
    source_table: str,
    record_pk: str,
    screen_name: str,
    severity: str,
    column_name: Optional[str],
    expected: Optional[str],
    actual: Optional[str],
    message: str,
) -> dict:
    return {
        "etl_batch_id":    batch_id,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_system":   "northwind",
        "source_table":    source_table,
        "source_record_pk": record_pk,
        "screen_name":     screen_name,
        "screen_severity": severity,
        "column_name":     column_name,
        "expected_value":  expected,
        "actual_value":    actual,
        "message":         message,
    }


def _apply_not_null_screen(
    df: DataFrame,
    entity: str,
    col_name: str,
    severity: str,
    batch_id: str,
    pk_col: Optional[str],
) -> tuple[DataFrame, list[dict]]:
    """Filter rows where col_name IS NULL. Returns (passing_df, error_rows)."""
    null_mask = F.col(col_name).isNull()
    bad = df.filter(null_mask)
    good = df.filter(~null_mask)

    errors = []
    for row in bad.collect():
        pk_val = str(row[pk_col]) if pk_col and pk_col in row.asDict() else "unknown"
        errors.append(_build_error_row(
            batch_id, entity, pk_val,
            f"not_null:{col_name}", severity, col_name,
            "NOT NULL", "NULL",
            f"Column {col_name} must not be null",
        ))
    return good, errors


def _apply_max_length_screen(
    df: DataFrame,
    entity: str,
    col_name: str,
    max_len: int,
    severity: str,
    batch_id: str,
    pk_col: Optional[str],
) -> tuple[DataFrame, list[dict]]:
    """Filter rows where len(col_name) > max_len."""
    if col_name not in df.columns:
        return df, []

    bad_mask = F.length(F.col(col_name)) > max_len
    bad = df.filter(bad_mask)
    good = df.filter(~bad_mask | F.col(col_name).isNull())

    errors = []
    for row in bad.collect():
        pk_val = str(row[pk_col]) if pk_col and pk_col in row.asDict() else "unknown"
        actual_val = str(row[col_name]) if row[col_name] else ""
        errors.append(_build_error_row(
            batch_id, entity, pk_val,
            f"max_length:{col_name}", severity, col_name,
            f"len <= {max_len}", f"len={len(actual_val)}",
            f"Column {col_name} exceeds max length {max_len}",
        ))
    return good, errors


def _apply_numeric_range_screen(
    df: DataFrame,
    entity: str,
    col_name: str,
    min_val: float,
    max_val: float,
    severity: str,
    batch_id: str,
    pk_col: Optional[str],
) -> tuple[DataFrame, list[dict]]:
    """Filter rows where col_name is outside [min_val, max_val]."""
    if col_name not in df.columns:
        return df, []

    numeric_col = F.col(col_name).cast(T.DoubleType())
    bad_mask = (numeric_col < min_val) | (numeric_col > max_val)
    bad = df.filter(bad_mask)
    # Rows with NULL pass (null is not out-of-range)
    good = df.filter(~bad_mask | F.col(col_name).isNull())

    errors = []
    for row in bad.collect():
        pk_val = str(row[pk_col]) if pk_col and pk_col in row.asDict() else "unknown"
        errors.append(_build_error_row(
            batch_id, entity, pk_val,
            f"numeric_range:{col_name}", severity, col_name,
            f"[{min_val}, {max_val}]", str(row[col_name]),
            f"Column {col_name} out of range [{min_val}, {max_val}]",
        ))
    return good, errors


def _apply_unique_screen(
    df: DataFrame,
    entity: str,
    col_name: str,
    severity: str,
    batch_id: str,
    pk_col: Optional[str],
) -> tuple[DataFrame, list[dict]]:
    """Keep only the first occurrence of each col_name value; flag duplicates."""
    if col_name not in df.columns:
        return df, []

    from pyspark.sql.window import Window
    w = Window.partitionBy(col_name).orderBy(F.monotonically_increasing_id())
    ranked = df.withColumn("_rank", F.row_number().over(w))
    bad = ranked.filter(F.col("_rank") > 1).drop("_rank")
    good = ranked.filter(F.col("_rank") == 1).drop("_rank")

    errors = []
    for row in bad.collect():
        pk_val = str(row[pk_col]) if pk_col and pk_col in row.asDict() else "unknown"
        errors.append(_build_error_row(
            batch_id, entity, pk_val,
            f"unique:{col_name}", severity, col_name,
            "UNIQUE", str(row[col_name]),
            f"Duplicate value in column {col_name}: {row[col_name]}",
        ))
    return good, errors


def _apply_date_range_screen(
    df: DataFrame,
    entity: str,
    col_name: str,
    min_date: str,
    max_date: str,
    severity: str,
    batch_id: str,
    pk_col: Optional[str],
) -> tuple[DataFrame, list[dict]]:
    """Filter rows where col_name is outside [min_date, max_date]."""
    if col_name not in df.columns:
        return df, []

    resolved_max = str(datetime.now(timezone.utc).date()) if max_date == "today" else max_date
    parsed = F.to_date(F.col(col_name))
    bad_mask = (parsed < F.lit(min_date)) | (parsed > F.lit(resolved_max))
    bad = df.filter(bad_mask & F.col(col_name).isNotNull())
    good = df.filter(~bad_mask | F.col(col_name).isNull())

    errors = []
    for row in bad.collect():
        pk_val = str(row[pk_col]) if pk_col and pk_col in row.asDict() else "unknown"
        errors.append(_build_error_row(
            batch_id, entity, pk_val,
            f"date_range:{col_name}", severity, col_name,
            f"[{min_date}, {resolved_max}]", str(row[col_name]),
            f"Column {col_name} out of date range",
        ))
    return good, errors


def _write_error_events(
    spark: SparkSession,
    errors: list[dict],
    batch_id: str,
) -> None:
    """Write error event rows to PostgreSQL staging.error_events via JDBC."""
    if not errors:
        return

    error_schema = T.StructType([
        T.StructField("etl_batch_id",    T.StringType(), True),
        T.StructField("event_timestamp", T.StringType(), True),
        T.StructField("source_system",   T.StringType(), True),
        T.StructField("source_table",    T.StringType(), True),
        T.StructField("source_record_pk", T.StringType(), True),
        T.StructField("screen_name",     T.StringType(), True),
        T.StructField("screen_severity", T.StringType(), True),
        T.StructField("column_name",     T.StringType(), True),
        T.StructField("expected_value",  T.StringType(), True),
        T.StructField("actual_value",    T.StringType(), True),
        T.StructField("message",         T.StringType(), True),
    ])
    err_df = spark.createDataFrame(errors, schema=error_schema)

    jdbc_url = get_pg_jdbc_url()
    props = get_pg_jdbc_properties()
    try:
        err_df.write.jdbc(
            url=jdbc_url,
            table=_ERROR_EVENTS_TABLE,
            mode="append",
            properties=props,
        )
        logger.info("[CLEAN-SPARK] batch=%s wrote %d error events", batch_id, len(errors))
    except Exception as exc:
        # Non-fatal: log and continue — don't lose cleaned data over error event write failure
        logger.warning("[CLEAN-SPARK] batch=%s error_events write failed: %s", batch_id, exc)


def run_clean_job(
    batch_id: str,
    raw_dir: str,
    staging_dir: str,
    config_path: str = _CONFIG_PATH,
    write_error_events: bool = True,
) -> dict[str, int]:
    """Run distributed quality screens for all entities in raw_dir.

    Applies column property screens (not_null, max_length, numeric_range,
    unique, date_range) using PySpark DataFrame API.  Error rows are written
    to PostgreSQL staging.error_events; passing rows are written as Parquet.

    Args:
        batch_id:          ETL batch identifier.
        raw_dir:           Directory containing raw CSV files.
        staging_dir:       Output root for cleaned Parquet directories.
        config_path:       Path to quality_rules.yaml.
        write_error_events: Write error rows to PostgreSQL (disable in tests).

    Returns:
        Dict {entity_name: clean_row_count} for each entity processed.

    Raises:
        CleanJobError: When a FATAL violation is encountered.
    """
    spark = get_spark_session(f"ETL-Clean-{batch_id}")
    rules = _load_rules(config_path)
    screen_rules = rules.get("screens", {})

    clean_counts: dict[str, int] = {}

    for entity in _PROCESSING_ORDER:
        csv_path = f"{raw_dir}/{entity}.csv"

        # Skip entities not present in raw_dir
        if not os.path.exists(csv_path.replace("/opt/etl/", "")):
            # Try the path as-is (Docker) then relative
            try:
                df = spark.read.csv(csv_path, header=True, inferSchema=False)
                if df.rdd.isEmpty():
                    logger.info("[CLEAN-SPARK] batch=%s entity=%s — CSV empty, skipping", batch_id, entity)
                    continue
            except Exception:
                logger.info("[CLEAN-SPARK] batch=%s entity=%s — CSV not found, skipping", batch_id, entity)
                continue
        else:
            df = spark.read.csv(csv_path, header=True, inferSchema=False)

        pk_col = _ENTITY_PK.get(entity)
        entity_rules = screen_rules.get(entity, {})
        col_rules = entity_rules.get("column_property", [])

        all_errors: list[dict] = []
        logger.info("[CLEAN-SPARK] batch=%s entity=%s rows=%s — applying screens",
                    batch_id, entity, df.count())

        for rule_cfg in col_rules:
            col = rule_cfg.get("column")
            rule = rule_cfg.get("rule")
            severity = rule_cfg.get("severity", "WARN")

            if col not in df.columns:
                continue

            if rule == "not_null":
                df, errors = _apply_not_null_screen(df, entity, col, severity, batch_id, pk_col)
                if severity == "FATAL" and errors:
                    if write_error_events:
                        _write_error_events(spark, errors, batch_id)
                    raise CleanJobError(
                        f"[batch={batch_id}] FATAL violation in {entity}.{col}: "
                        f"{len(errors)} null PKs — batch stopped"
                    )
                all_errors.extend(errors)

            elif rule == "max_length":
                max_len = int(rule_cfg.get("value", 255))
                df, errors = _apply_max_length_screen(
                    df, entity, col, max_len, severity, batch_id, pk_col
                )
                all_errors.extend(errors)

            elif rule == "numeric_range":
                min_v = float(rule_cfg.get("min", 0))
                max_v = float(rule_cfg.get("max", 1e9))
                df, errors = _apply_numeric_range_screen(
                    df, entity, col, min_v, max_v, severity, batch_id, pk_col
                )
                all_errors.extend(errors)

            elif rule == "unique":
                df, errors = _apply_unique_screen(df, entity, col, severity, batch_id, pk_col)
                if severity == "FATAL" and errors:
                    if write_error_events:
                        _write_error_events(spark, errors, batch_id)
                    raise CleanJobError(
                        f"[batch={batch_id}] FATAL unique violation in {entity}.{col} — batch stopped"
                    )
                all_errors.extend(errors)

            elif rule == "date_range":
                min_d = rule_cfg.get("min", "1900-01-01")
                max_d = rule_cfg.get("max", "today")
                df, errors = _apply_date_range_screen(
                    df, entity, col, str(min_d), str(max_d), severity, batch_id, pk_col
                )
                all_errors.extend(errors)

        # Write error events to PostgreSQL
        if write_error_events and all_errors:
            _write_error_events(spark, all_errors, batch_id)

        # Write cleaned data to staging as Parquet
        out_dir = f"{staging_dir}/{_ENTITY_DIR[entity]}"
        df.write.parquet(out_dir, mode="overwrite")

        row_count = df.count()
        clean_counts[entity] = row_count
        logger.info(
            "[CLEAN-SPARK] batch=%s entity=%s done — violations=%d clean_rows=%d",
            batch_id, entity, len(all_errors), row_count,
        )

    spark.stop()
    return clean_counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 4:
        print("Usage: clean_job.py <batch_id> <raw_dir> <staging_dir>")
        sys.exit(1)
    run_clean_job(sys.argv[1], sys.argv[2], sys.argv[3])
