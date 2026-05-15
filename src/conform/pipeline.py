"""
Conform Phase Pipeline — orchestrator for Subsystems #7, #8, #17, #21.

run_conform_phase(batch_id, staging_dir) → ConformResult

Order of operations per entity:
  standardize → deduplicate → survivorship → publish

dim_geography is built and published first; all other dims depend on it
for country_code FK closure.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

from src.conform.deduplicator import Deduplicator
from src.conform.dimension_manager import DimensionManager, DimensionManagerError
from src.conform.standardizer import Standardizer
from src.conform.survivor_selector import SurvivorSelector

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_DIR = Path("config")
_DEFAULT_PUBLISH_DIR = Path("data/staging/conform/published")
_DEFAULT_CLUSTERS_DIR = Path("data/staging/conform")
# Used when no cleaned parquets exist (dev fallback).
_RAW_NORTHWIND_DIR = Path("data/raw/northwind")


class ConformError(Exception):
    """Raised when a non-recoverable conform-phase failure occurs."""


@dataclass
class ConformResult:
    """Outcome of run_conform_phase().

    Attributes:
        batch_id:           ETL batch identifier.
        entities_conformed: Dimensions successfully published.
        duplicates_found:   Total duplicate records detected across all entities.
        golden_records:     dim_name → count of golden records published.
        qa_passed:          True iff all QA checks passed.
        qa_failures:        List of QA check failure messages.
    """

    batch_id: str
    entities_conformed: list[str] = field(default_factory=list)
    duplicates_found: int = 0
    golden_records: dict[str, int] = field(default_factory=dict)
    qa_passed: bool = True
    qa_failures: list[str] = field(default_factory=list)


def run_conform_phase(
    batch_id: str,
    staging_dir: Path,
    config_dir: Path = _DEFAULT_CONFIG_DIR,
    publish_dir: Path = _DEFAULT_PUBLISH_DIR,
    clusters_dir: Optional[Path] = None,
    threshold: float = 0.85,
) -> ConformResult:
    """Run the full conform phase for all available cleaned entities.

    Reads cleaned Parquet files from ``staging_dir/cleaned/``.  If none are
    found, falls back to the latest raw-CSV snapshot in ``data/raw/northwind/``
    (development convenience).

    Args:
        batch_id:     ETL run identifier.
        staging_dir:  Root staging directory (expects a ``cleaned/`` sub-dir).
        config_dir:   Directory containing standardization config.
        publish_dir:  Root directory for published dimension files.
        clusters_dir: Directory to write cluster Parquet files.
        threshold:    Jaro-Winkler threshold for fuzzy deduplication.

    Returns:
        ConformResult describing what happened.
    """
    result = ConformResult(batch_id=batch_id)
    _clusters_dir = clusters_dir or Path("data/staging/conform")

    standardizer = Standardizer(config_dir=config_dir)
    deduplicator = Deduplicator(threshold=threshold)
    survivor = SurvivorSelector(config_path=config_dir / "survivorship_rules.yaml")
    dim_mgr = DimensionManager(base_dir=publish_dir)

    _clusters_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Load source DataFrames
    # ------------------------------------------------------------------
    entity_dfs = _load_entities(staging_dir)
    if not entity_dfs:
        logger.warning("[CONFORM] batch=%s — no source data found in %s", batch_id, staging_dir)
        return result

    logger.info(
        "[CONFORM] batch=%s — loaded entities: %s",
        batch_id, list(entity_dfs.keys()),
    )

    # ------------------------------------------------------------------
    # 2. Build dim_geography first (no dedup/survivorship needed)
    # ------------------------------------------------------------------
    geo_df = _build_dim_geography(standardizer)
    if not geo_df.empty:
        try:
            dim_mgr.publish("dim_geography", geo_df, batch_id)
            result.entities_conformed.append("dim_geography")
            result.golden_records["dim_geography"] = len(geo_df)
            logger.info("[CONFORM] batch=%s dim_geography published (%d rows)", batch_id, len(geo_df))
        except DimensionManagerError as exc:
            logger.error("[CONFORM] dim_geography publish failed: %s", exc)

    # ------------------------------------------------------------------
    # 3. Standardize → deduplicate → survivorship → publish per entity
    # ------------------------------------------------------------------
    _ENTITY_TO_DIM = {
        "customers":  ("customers",  "dim_customer",  "customerID"),
        "employees":  ("employees",  "dim_employee",  "employeeID"),
        "products":   ("products",   "dim_product",   "productID"),
        "suppliers":  ("suppliers",  "dim_supplier",  "supplierID"),
        "shippers":   ("shippers",   "dim_shipper",   "shipperID"),
        "categories": ("categories", "dim_category",  "categoryID"),
    }

    for entity_key, (entity_name, dim_name, nk_col) in _ENTITY_TO_DIM.items():
        if entity_name not in entity_dfs:
            logger.debug("[CONFORM] %r not in source data — skipping", entity_name)
            continue

        raw_df = entity_dfs[entity_name]
        logger.info(
            "[CONFORM] batch=%s entity=%s rows=%d — standardize/dedup/survivorship",
            batch_id, entity_name, len(raw_df),
        )

        # 3a. Standardize
        std_df = standardizer.standardize_df(raw_df, entity_name)

        # 3b. Deduplicate → cluster map
        cluster_map = deduplicator.find_clusters(std_df, entity_name)
        n_clusters = cluster_map["cluster_id"].nunique()
        n_records = len(cluster_map)
        dups_here = n_records - n_clusters
        result.duplicates_found += dups_here
        if dups_here:
            logger.info("[CONFORM] batch=%s %s: %d duplicates detected", batch_id, entity_name, dups_here)

        # Save cluster map
        cluster_path = _clusters_dir / f"{entity_name}_clusters.parquet"
        cluster_map.to_parquet(cluster_path, index=False)

        # 3c. Merge cluster_id back onto standardized df for survivorship
        if nk_col in std_df.columns and nk_col in cluster_map.columns:
            merged = std_df.merge(cluster_map[["cluster_id", nk_col]], on=nk_col, how="left")
        else:
            std_df["cluster_id"] = [f"cluster_{i+1:06d}" for i in range(len(std_df))]
            merged = std_df

        # 3d. Survivorship → golden records
        golden_df = survivor.select(merged, entity_name)

        # Attach SCD metadata for downstream Deliver phase
        today = date.today().isoformat()
        golden_df["effective_date"] = today
        golden_df["expiration_date"] = None
        golden_df["is_current"] = True

        # 3e. Publish
        try:
            dim_mgr.publish(dim_name, golden_df, batch_id)
            result.entities_conformed.append(dim_name)
            result.golden_records[dim_name] = len(golden_df)
        except DimensionManagerError as exc:
            logger.error("[CONFORM] batch=%s %s publish failed: %s", batch_id, dim_name, exc)

    # ------------------------------------------------------------------
    # 4. QA checks
    # ------------------------------------------------------------------
    _run_qa(result, dim_mgr, batch_id)

    logger.info(
        "[CONFORM] batch=%s done — entities=%s duplicates=%d qa_passed=%s",
        batch_id, result.entities_conformed, result.duplicates_found, result.qa_passed,
    )
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_entities(staging_dir: Path) -> dict[str, pd.DataFrame]:
    """Load cleaned Parquet files; fallback to latest raw CSV snapshot."""
    cleaned_dir = staging_dir / "cleaned"
    dfs: dict[str, pd.DataFrame] = {}

    if cleaned_dir.exists():
        for pq in sorted(cleaned_dir.glob("*.parquet")):
            entity = pq.stem
            try:
                dfs[entity] = pd.read_parquet(pq)
                logger.debug("loaded %s from cleaned parquet (%d rows)", entity, len(dfs[entity]))
            except Exception as exc:
                logger.error("failed to read %s: %s", pq, exc)

    if not dfs:
        # Dev fallback: read from latest raw CSV snapshot
        logger.info("[CONFORM] no cleaned parquets found — trying raw CSV fallback")
        dfs = _load_from_raw_csv()

    return dfs


def _load_from_raw_csv() -> dict[str, pd.DataFrame]:
    """Read the most-recent raw snapshot from data/raw/northwind/."""
    raw_root = _RAW_NORTHWIND_DIR
    if not raw_root.exists():
        return {}
    snapshots = sorted(raw_root.iterdir())
    if not snapshots:
        return {}
    latest = snapshots[-1]
    dfs: dict[str, pd.DataFrame] = {}
    for csv_path in latest.glob("*.csv"):
        entity = csv_path.stem
        try:
            dfs[entity] = pd.read_csv(csv_path, on_bad_lines="skip")
            logger.debug("raw-fallback loaded %s (%d rows)", entity, len(dfs[entity]))
        except Exception as exc:
            logger.error("raw-fallback: failed to read %s: %s", csv_path, exc)
    return dfs


def _build_dim_geography(standardizer: Standardizer) -> pd.DataFrame:
    """Build dim_geography from country_info in country_aliases.yaml."""
    country_info = standardizer.get_country_info()
    if not country_info:
        logger.warning("[CONFORM] no country_info found — dim_geography will be empty")
        return pd.DataFrame()

    rows = []
    for idx, (code, info) in enumerate(sorted(country_info.items()), start=1):
        rows.append({
            "geography_sk":      idx,
            "country_code":      code,
            "country_name":      info.get("country_name", ""),
            "region":            info.get("region", ""),
            "subregion":         info.get("subregion", ""),
            "primary_currency":  info.get("primary_currency", ""),
        })
    return pd.DataFrame(rows)


def _run_qa(result: ConformResult, dim_mgr: DimensionManager, batch_id: str) -> None:
    """Run post-publish QA checks; populate result.qa_passed / qa_failures."""

    # QA 1: country_code in dim_customer must exist in dim_geography
    try:
        geo_df = dim_mgr.get_latest("dim_geography")
        valid_codes: set[str] = set(geo_df["country_code"].dropna())
    except DimensionManagerError:
        geo_df = pd.DataFrame()
        valid_codes = set()

    if "dim_customer" in result.entities_conformed:
        try:
            cust_df = dim_mgr.get_latest("dim_customer")
            if "country_code" in cust_df.columns:
                unknown = set(cust_df["country_code"].dropna()) - valid_codes
                if unknown:
                    msg = f"QA: dim_customer.country_code has values not in dim_geography: {sorted(unknown)}"
                    result.qa_failures.append(msg)
                    logger.warning("[CONFORM] batch=%s %s", batch_id, msg)
        except DimensionManagerError:
            pass

    # QA 2: each NK has exactly 1 golden record in dim_customer
    if "dim_customer" in result.entities_conformed:
        try:
            cust_df = dim_mgr.get_latest("dim_customer")
            if "customerID" in cust_df.columns:
                dupes = cust_df["customerID"].duplicated().sum()
                if dupes:
                    msg = f"QA: dim_customer has {dupes} duplicate customerID values"
                    result.qa_failures.append(msg)
        except DimensionManagerError:
            pass

    # QA 3: no NULL in PK columns
    for dim_name, nk_col in [("dim_customer", "customerID"), ("dim_geography", "country_code")]:
        if dim_name in result.entities_conformed:
            try:
                df = dim_mgr.get_latest(dim_name)
                if nk_col in df.columns:
                    null_count = df[nk_col].isna().sum()
                    if null_count:
                        msg = f"QA: {dim_name}.{nk_col} has {null_count} NULL values"
                        result.qa_failures.append(msg)
            except DimensionManagerError:
                pass

    result.qa_passed = len(result.qa_failures) == 0
