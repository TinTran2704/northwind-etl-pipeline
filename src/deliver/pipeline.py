"""
Deliver Phase Pipeline — orchestrator for Subsystems #9, #10, #13, #14, #19.

run_deliver_phase(batch_id, staging_dir, engine) → DeliverResult

Order:
  load dims → build fact_sales → load fact_sales → build agg → load agg
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import Engine, text

from src.deliver.aggregate_builder import AggregateBuilder
from src.deliver.dim_builder import DimBuilder, _find_latest_raw, _read_csv
from src.deliver.fact_builder import FactBuilder
from src.deliver.surrogate_key_generator import SurrogateKeyGenerator

logger = logging.getLogger(__name__)

_DEFAULT_CONFORMED_DIR = Path("data/staging/conform/published")


class DeliverError(Exception):
    """Raised on non-recoverable deliver-phase failure."""


@dataclass
class DeliverResult:
    """Outcome of run_deliver_phase().

    Attributes:
        batch_id:    ETL batch identifier.
        dims_loaded: Dict of {dim_name: rows_loaded}.
        fact_rows:   Rows inserted into fact_sales.
        agg_rows:    Rows upserted into agg_sales_monthly.
        errors:      List of non-fatal error messages.
    """

    batch_id: str
    dims_loaded: dict[str, int] = field(default_factory=dict)
    fact_rows: int = 0
    agg_rows: int = 0
    errors: list[str] = field(default_factory=list)


def run_deliver_phase(
    batch_id: str,
    staging_dir: Path,
    engine: Engine,
    config_dir: Path = Path("config"),
    conformed_dir: Optional[Path] = None,
    raw_dir: Optional[Path] = None,
) -> DeliverResult:
    """Run the full deliver phase for one ETL batch.

    Reads conformed dims from ``conformed_dir`` (defaults to
    ``data/staging/conform/published``).  Falls back to the latest raw
    snapshot for entities not yet published through the conform phase
    (products, employees, shippers).

    Args:
        batch_id:      ETL batch identifier.
        staging_dir:   Root staging directory (unused directly, kept for API
                       consistency with other phases).
        engine:        SQLAlchemy engine connected to northwind_dw.
        config_dir:    Config directory for Standardizer.
        conformed_dir: Root of DimensionManager published dims.
        raw_dir:       Raw CSV snapshot directory (auto-detected if None).

    Returns:
        DeliverResult describing what happened.
    """
    result = DeliverResult(batch_id=batch_id)
    conf_dir = conformed_dir or _DEFAULT_CONFORMED_DIR
    raw = raw_dir or _find_latest_raw()

    sk_gen   = SurrogateKeyGenerator()
    dim_bld  = DimBuilder(sk_gen=sk_gen, config_dir=config_dir)
    fact_bld = FactBuilder()
    agg_bld  = AggregateBuilder()

    # ------------------------------------------------------------------
    # 1. Build and load all dimension tables
    # ------------------------------------------------------------------
    logger.info("[DELIVER] batch=%s — loading dimensions", batch_id)
    try:
        result.dims_loaded = dim_bld.load_all_dims(
            batch_id=batch_id,
            conformed_dir=conf_dir,
            engine=engine,
            raw_dir=raw,
        )
    except Exception as exc:
        msg = f"load_all_dims failed: {exc}"
        logger.error("[DELIVER] batch=%s %s", batch_id, msg)
        result.errors.append(msg)
        return result

    # ------------------------------------------------------------------
    # 2. Read dimension tables back from DB for in-memory SK resolution
    # ------------------------------------------------------------------
    all_dims = _load_dims_from_db(engine)

    # ------------------------------------------------------------------
    # 3. Build and load fact_sales
    # ------------------------------------------------------------------
    logger.info("[DELIVER] batch=%s — building fact_sales", batch_id)
    fact_df = pd.DataFrame()
    orders_df  = _read_csv(raw, "orders.csv")
    details_df = _read_csv(raw, "order-details.csv")

    if orders_df.empty or details_df.empty:
        logger.warning(
            "[DELIVER] batch=%s — orders or order-details not found; fact_sales skipped",
            batch_id,
        )
    else:
        try:
            fact_df = fact_bld.build_fact_sales(
                orders_df, details_df, all_dims, batch_id, audit_sk=-1
            )
            result.fact_rows = fact_bld.load_fact_to_postgres(fact_df, engine)
        except Exception as exc:
            msg = f"fact_sales build/load failed: {exc}"
            logger.error("[DELIVER] batch=%s %s", batch_id, msg)
            result.errors.append(msg)

    # ------------------------------------------------------------------
    # 4. Build and load agg_sales_monthly
    # ------------------------------------------------------------------
    logger.info("[DELIVER] batch=%s — building agg_sales_monthly", batch_id)
    try:
        agg_df = agg_bld.build_agg_sales_monthly(
            fact_df,
            all_dims.get("dim_date", pd.DataFrame()),
            all_dims.get("dim_product", pd.DataFrame()),
            all_dims.get("dim_customer", pd.DataFrame()),
            audit_sk=-1,
        )
        result.agg_rows = agg_bld.load_agg_to_postgres(agg_df, engine)
    except Exception as exc:
        msg = f"agg_sales_monthly failed: {exc}"
        logger.error("[DELIVER] batch=%s %s", batch_id, msg)
        result.errors.append(msg)

    # ------------------------------------------------------------------
    # 5. Mark batch as SUCCESS
    # ------------------------------------------------------------------
    _mark_etl_run(
        batch_id, engine,
        rows_loaded=result.fact_rows + sum(result.dims_loaded.values()),
    )

    logger.info(
        "[DELIVER] batch=%s done — dims=%s fact=%d agg=%d errors=%d",
        batch_id, list(result.dims_loaded.keys()),
        result.fact_rows, result.agg_rows, len(result.errors),
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_dims_from_db(engine: Engine) -> dict[str, pd.DataFrame]:
    """Read all dimension tables back from PostgreSQL for SK resolution."""
    dims: dict[str, pd.DataFrame] = {}
    tables = {
        "dim_date":      "warehouse.dim_date",
        "dim_geography": "warehouse.dim_geography",
        "dim_customer":  "warehouse.dim_customer",
        "dim_product":   "warehouse.dim_product",
        "dim_employee":  "warehouse.dim_employee",
        "dim_shipper":   "warehouse.dim_shipper",
    }
    with engine.connect() as conn:
        for name, table in tables.items():
            try:
                dims[name] = pd.read_sql(f"SELECT * FROM {table}", conn)
            except Exception as exc:
                logger.warning("[DELIVER] could not load %s from DB: %s", table, exc)
                dims[name] = pd.DataFrame()
    return dims


def _mark_etl_run(batch_id: str, engine: Engine, rows_loaded: int) -> None:
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE metadata.etl_runs
            SET status = 'SUCCESS', ended_at = NOW(), rows_loaded = :rows_loaded
            WHERE batch_id = :batch_id
        """), {"batch_id": batch_id, "rows_loaded": rows_loaded})
