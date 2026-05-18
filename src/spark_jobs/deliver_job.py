"""
Spark Deliver Job — distributed replacement for src/deliver/pipeline.py.

Ports Kimball Subsystems #10 (SK generation) and #13-14 (fact + SK pipeline)
to PySpark.  Reads cleaned Parquet from staging_dir, loads dims from
PostgreSQL via JDBC, resolves surrogate keys via broadcast join (point-in-time),
and writes fact_sales back to PostgreSQL.

SK generation avoids monotonically_increasing_id() — instead queries max(SK)
from the DB and uses zipWithIndex() + offset (spec §12.8 / docs/08-deliver-phase.md §8.3).

Submit:
    spark-submit --master spark://spark-master:7077 \\
        --jars /opt/spark/jars/postgresql-42.7.3.jar \\
        /opt/etl/src/spark_jobs/deliver_job.py \\
        <batch_id> <staging_dir> <warehouse_db_url>
"""

import logging
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.spark_jobs.common import get_pg_jdbc_properties, get_pg_jdbc_url, get_spark_session

logger = logging.getLogger(__name__)

_UNKNOWN_SK = -1
_UNKNOWN_DATE_SK = 19000101
_FACT_TABLE = "warehouse.fact_sales"


class DeliverJobError(Exception):
    """Raised on non-recoverable deliver-phase failure."""


# ---------------------------------------------------------------------------
# SK generation
# ---------------------------------------------------------------------------

def _query_max_sk(spark: SparkSession, table: str, sk_col: str) -> int:
    """Query current max surrogate key from PostgreSQL.

    Args:
        spark:  Active SparkSession.
        table:  Fully-qualified table name (e.g. 'warehouse.dim_customer').
        sk_col: SK column name.

    Returns:
        Current max SK, or 0 if table is empty.
    """
    jdbc_url = get_pg_jdbc_url()
    props = get_pg_jdbc_properties()
    try:
        row = (
            spark.read.jdbc(url=jdbc_url, table=f"(SELECT MAX({sk_col}) AS max_sk FROM {table}) t",
                            properties=props)
            .collect()
        )
        val = row[0]["max_sk"] if row and row[0]["max_sk"] is not None else 0
        return int(val)
    except Exception as exc:
        logger.warning("[DELIVER-SPARK] Could not query max SK from %s: %s — using 0", table, exc)
        return 0


def _assign_surrogate_keys(df: DataFrame, dim_name: str, sk_col: str, offset: int) -> DataFrame:
    """Assign new surrogate keys to rows that need them.

    Uses zipWithIndex() + offset so SKs are globally unique without
    depending on auto-increment (which doesn't work distributed).

    Args:
        df:       DataFrame of new rows needing SKs.
        dim_name: Dimension name (for logging).
        sk_col:   Name of the SK column to add.
        offset:   Current max SK in DB; new SKs start at offset+1.

    Returns:
        DataFrame with sk_col added.
    """
    if df.rdd.isEmpty():
        return df.withColumn(sk_col, F.lit(_UNKNOWN_SK).cast(T.LongType()))

    schema = T.StructType(df.schema.fields + [T.StructField(sk_col, T.LongType(), False)])
    offset_val = int(offset)

    def add_sk(row_with_index):
        row, idx = row_with_index
        return tuple(row) + (offset_val + idx + 1,)

    rdd_with_sk = df.rdd.zipWithIndex().map(add_sk)
    result = df.sparkSession.createDataFrame(rdd_with_sk, schema=schema)
    logger.info("[DELIVER-SPARK] %s: assigned %d new SKs starting at %d",
                dim_name, result.count(), offset_val + 1)
    return result


# ---------------------------------------------------------------------------
# Dim loading
# ---------------------------------------------------------------------------

def _load_dim(spark: SparkSession, table: str) -> DataFrame:
    """Load a dimension table from PostgreSQL via JDBC.

    Args:
        spark: Active SparkSession.
        table: Fully-qualified table name.

    Returns:
        DataFrame (possibly empty if table missing/unreachable).
    """
    jdbc_url = get_pg_jdbc_url()
    props = get_pg_jdbc_properties()
    try:
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=props)
        logger.info("[DELIVER-SPARK] Loaded %s (%d rows)", table, df.count())
        return df
    except Exception as exc:
        logger.warning("[DELIVER-SPARK] Could not load %s: %s — using empty", table, exc)
        return spark.createDataFrame([], T.StructType([]))


# ---------------------------------------------------------------------------
# Point-in-time SK resolution
# ---------------------------------------------------------------------------

def _resolve_sk_point_in_time(
    fact_df: DataFrame,
    dim_df: DataFrame,
    fact_nk_col: str,
    fact_date_col: str,
    dim_nk_col: str,
    dim_sk_col: str,
    output_col: str,
    has_scd2: bool = True,
) -> DataFrame:
    """Resolve surrogate keys via broadcast join with point-in-time logic.

    For SCD2 dims: joins on nk + effective_date <= event_date <= expiration_date.
    For Type-1 dims (has_scd2=False): simple equality join on nk.

    Unresolved rows get _UNKNOWN_SK (-1).

    Args:
        fact_df:       Fact DataFrame.
        dim_df:        Dimension DataFrame.
        fact_nk_col:   Column in fact holding the natural key.
        fact_date_col: Column in fact holding the event date (for point-in-time).
        dim_nk_col:    Column in dim holding the natural key.
        dim_sk_col:    Column in dim holding the surrogate key.
        output_col:    Column name to add to fact_df with the resolved SK.
        has_scd2:      True if dim has effective_date/expiration_date columns.

    Returns:
        fact_df with output_col added.
    """
    if dim_df is None or dim_df.rdd.isEmpty():
        return fact_df.withColumn(output_col, F.lit(_UNKNOWN_SK).cast(T.LongType()))

    dim_renamed = dim_df.select(
        F.col(dim_nk_col).alias("_dim_nk"),
        F.col(dim_sk_col).alias("_dim_sk"),
        *([
            F.col("effective_date").alias("_dim_eff"),
            F.col("expiration_date").alias("_dim_exp"),
        ] if has_scd2 and "effective_date" in dim_df.columns else []),
    )

    fact_date = F.to_date(F.col(fact_date_col))
    fact_nk = F.col(fact_nk_col).cast(T.StringType())

    if has_scd2 and "effective_date" in dim_df.columns:
        join_cond = (
            (fact_nk == F.col("_dim_nk")) &
            (fact_date >= F.to_date(F.col("_dim_eff"))) &
            (
                F.col("_dim_exp").isNull() |
                (fact_date <= F.to_date(F.col("_dim_exp")))
            )
        )
    else:
        join_cond = fact_nk == F.col("_dim_nk")

    joined = fact_df.join(F.broadcast(dim_renamed), join_cond, "left")
    result = joined.withColumn(
        output_col,
        F.coalesce(F.col("_dim_sk"), F.lit(_UNKNOWN_SK)).cast(T.LongType()),
    )

    drop_cols = ["_dim_nk", "_dim_sk"]
    if has_scd2 and "_dim_eff" in joined.columns:
        drop_cols += ["_dim_eff", "_dim_exp"]

    return result.drop(*drop_cols)


def _date_to_sk(date_str_col: "Column") -> "Column":  # type: ignore[name-defined]
    """Convert a date column to YYYYMMDD integer SK."""
    return F.coalesce(
        F.date_format(F.to_date(date_str_col), "yyyyMMdd").cast(T.IntegerType()),
        F.lit(_UNKNOWN_DATE_SK),
    )


# ---------------------------------------------------------------------------
# Fact builder
# ---------------------------------------------------------------------------

def _build_fact_sales(
    spark: SparkSession,
    staging_dir: str,
    dims: dict[str, DataFrame],
    batch_id: str,
    audit_sk: int = _UNKNOWN_SK,
) -> DataFrame:
    """Build fact_sales DataFrame from cleaned Parquet.

    Reads orders and order-details parquets, merges them, applies SK pipeline
    via broadcast joins, computes derived measures.

    Args:
        spark:       Active SparkSession.
        staging_dir: Root of cleaned Parquet output from clean_job.
        dims:        Dict {dim_name: DataFrame} of loaded dimension tables.
        batch_id:    ETL batch ID (for logging).
        audit_sk:    Audit dimension SK to stamp on every fact row.

    Returns:
        fact_sales DataFrame matching warehouse.fact_sales schema.
    """
    try:
        orders = spark.read.parquet(f"{staging_dir}/cleaned_orders")
        details = spark.read.parquet(f"{staging_dir}/cleaned_order_details")
    except Exception as exc:
        raise DeliverJobError(f"Could not read cleaned parquet: {exc}") from exc

    # Normalise column names (CSV has mixed case)
    orders = orders.toDF(*[c.strip() for c in orders.columns])
    details = details.toDF(*[c.strip() for c in details.columns])

    # Merge on orderID
    detail_cols = [F.col(c).alias(f"{c}_det") if c == "unitPrice" else F.col(c)
                   for c in details.columns]
    merged = details.select(*detail_cols).join(orders, on="orderID", how="inner")

    # line_number within each order
    from pyspark.sql.window import Window
    w = Window.partitionBy("orderID").orderBy(F.monotonically_increasing_id())
    merged = merged.withColumn("line_number", F.row_number().over(w).cast(T.ShortType()))

    # Date SKs
    merged = (
        merged
        .withColumn("order_date_sk",    _date_to_sk(F.col("orderDate")))
        .withColumn("required_date_sk", _date_to_sk(F.col("requiredDate")))
        .withColumn("shipped_date_sk",  F.coalesce(
            _date_to_sk(F.col("shippedDate")), F.lit(_UNKNOWN_DATE_SK)
        ))
    )

    # Surrogate key resolution via broadcast joins
    dim_customer = dims.get("dim_customer")
    if dim_customer is not None and "customer_nk" in (dim_customer.columns if dim_customer else []):
        merged = _resolve_sk_point_in_time(
            merged, dim_customer, "customerID", "orderDate",
            "customer_nk", "customer_sk", "customer_sk", has_scd2=True,
        )
    else:
        merged = merged.withColumn("customer_sk", F.lit(_UNKNOWN_SK).cast(T.LongType()))

    dim_employee = dims.get("dim_employee")
    if dim_employee is not None and "employee_nk" in (dim_employee.columns if dim_employee else []):
        merged = _resolve_sk_point_in_time(
            merged, dim_employee, "employeeID", "orderDate",
            "employee_nk", "employee_sk", "employee_sk", has_scd2=True,
        )
    else:
        merged = merged.withColumn("employee_sk", F.lit(_UNKNOWN_SK).cast(T.LongType()))

    dim_product = dims.get("dim_product")
    if dim_product is not None and "product_nk" in (dim_product.columns if dim_product else []):
        merged = merged.withColumn("productID", F.col("productID").cast(T.LongType()))
        merged = _resolve_sk_point_in_time(
            merged, dim_product, "productID", "orderDate",
            "product_nk", "product_sk", "product_sk", has_scd2=False,
        )
    else:
        merged = merged.withColumn("product_sk", F.lit(_UNKNOWN_SK).cast(T.LongType()))

    dim_shipper = dims.get("dim_shipper")
    if dim_shipper is not None and "shipper_nk" in (dim_shipper.columns if dim_shipper else []):
        merged = merged.withColumn("shipVia", F.col("shipVia").cast(T.LongType()))
        merged = _resolve_sk_point_in_time(
            merged, dim_shipper, "shipVia", "orderDate",
            "shipper_nk", "shipper_sk", "shipper_sk", has_scd2=False,
        )
    else:
        merged = merged.withColumn("shipper_sk", F.lit(_UNKNOWN_SK).cast(T.LongType()))

    dim_geography = dims.get("dim_geography")
    if dim_geography is not None and "country_code" in (dim_geography.columns if dim_geography else []):
        merged = _resolve_sk_point_in_time(
            merged, dim_geography, "shipCountry", "orderDate",
            "country_code", "geography_sk", "ship_geography_sk", has_scd2=False,
        )
    else:
        merged = merged.withColumn("ship_geography_sk", F.lit(_UNKNOWN_SK).cast(T.LongType()))

    # Derived measures
    unit_price_col = "unitPrice_det" if "unitPrice_det" in merged.columns else "unitPrice"
    merged = (
        merged
        .withColumn("unit_price",    F.col(unit_price_col).cast(T.DoubleType()).fillna(0.0))
        .withColumn("quantity",      F.col("quantity").cast(T.IntegerType()).fillna(0))
        .withColumn("discount",      F.col("discount").cast(T.DoubleType()).fillna(0.0))
        .withColumn("extended_price",
                    F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("discount_amount",
                    F.round(F.col("extended_price") * F.col("discount"), 2))
        .withColumn("net_amount",
                    F.round(F.col("extended_price") - F.col("discount_amount"), 2))
    )

    # Freight allocation: order.freight * (line_extended / order_total_extended)
    order_total = merged.groupBy("orderID").agg(
        F.sum("extended_price").alias("_order_total_ext")
    )
    merged = merged.join(order_total, on="orderID", how="left")
    merged = merged.withColumn(
        "freight_allocated",
        F.round(
            F.col("freight").cast(T.DoubleType()).fillna(0.0) *
            F.col("extended_price") /
            F.when(F.col("_order_total_ext") == 0, F.lit(1.0)).otherwise(F.col("_order_total_ext")),
            2,
        ),
    ).drop("_order_total_ext")

    merged = merged.withColumn("audit_sk", F.lit(audit_sk).cast(T.LongType()))

    # Filter out non-positive quantity rows
    merged = merged.filter(F.col("quantity") > 0)

    fact = merged.select(
        F.col("orderID").cast(T.IntegerType()).alias("order_id"),
        "line_number",
        "order_date_sk", "required_date_sk", "shipped_date_sk",
        "customer_sk", "employee_sk", "product_sk", "shipper_sk",
        "ship_geography_sk", "audit_sk",
        "quantity", "unit_price", "discount",
        "extended_price", "discount_amount", "net_amount", "freight_allocated",
    )

    logger.info("[DELIVER-SPARK] batch=%s fact_sales built: %d rows", batch_id, fact.count())
    return fact


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_deliver_job(
    batch_id: str,
    staging_dir: str,
    warehouse_db_url: Optional[str] = None,
) -> int:
    """Run distributed deliver phase: load dims, resolve SKs, write fact_sales.

    Args:
        batch_id:          ETL batch identifier.
        staging_dir:       Root of cleaned Parquet from clean_job.
        warehouse_db_url:  Ignored — JDBC URL is built from env vars via common.py.

    Returns:
        Number of fact_sales rows written.
    """
    spark = get_spark_session(f"ETL-Deliver-{batch_id}")

    # Load all dims from PostgreSQL via JDBC
    dim_tables = {
        "dim_customer":  "warehouse.dim_customer",
        "dim_employee":  "warehouse.dim_employee",
        "dim_product":   "warehouse.dim_product",
        "dim_shipper":   "warehouse.dim_shipper",
        "dim_geography": "warehouse.dim_geography",
    }
    dims: dict[str, DataFrame] = {}
    for dim_name, table in dim_tables.items():
        df = _load_dim(spark, table)
        dims[dim_name] = df if not df.rdd.isEmpty() else None

    # Build fact_sales
    fact_df = _build_fact_sales(spark, staging_dir, dims, batch_id)

    if fact_df.rdd.isEmpty():
        logger.warning("[DELIVER-SPARK] batch=%s — fact_sales is empty, nothing to write", batch_id)
        spark.stop()
        return 0

    # Write to PostgreSQL (append — idempotency handled by ON CONFLICT in table DDL)
    jdbc_url = get_pg_jdbc_url()
    props = get_pg_jdbc_properties()
    props["batchsize"] = "1000"

    fact_df.write.jdbc(
        url=jdbc_url,
        table=_FACT_TABLE,
        mode="append",
        properties=props,
    )

    row_count = fact_df.count()
    logger.info("[DELIVER-SPARK] batch=%s — wrote %d rows to %s", batch_id, row_count, _FACT_TABLE)
    spark.stop()
    return row_count


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) < 3:
        print("Usage: deliver_job.py <batch_id> <staging_dir> [warehouse_db_url]")
        sys.exit(1)
    db_url = sys.argv[3] if len(sys.argv) > 3 else None
    rows = run_deliver_job(sys.argv[1], sys.argv[2], db_url)
    print(f"Done — {rows} rows written to fact_sales")
