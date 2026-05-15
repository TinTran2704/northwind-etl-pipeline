"""
Clean Phase Pipeline — orchestrator for Subsystems #4, #5, #6.

run_clean_phase(batch_id, raw_dir) → CleanResult

Severity policy (docs/06-clean-phase.md §6.6):
  FATAL → stop batch immediately
  ERROR → quarantine row to data/error/quarantine/
  WARN  → pass row, set has_anomalies=True in audit
  INFO  → log only
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import Engine

from src.clean.audit_dimension_builder import AuditDimensionBuilder, AuditRecord
from src.clean.error_event_logger import ErrorEventLogger
from src.clean.screens.base_screen import ScreenResult, Severity
from src.clean.screens.column_property_screen import ColumnPropertyScreen
from src.clean.screens.data_rule_screen import DataRuleScreen
from src.clean.screens.reasonability_screen import ReasonabilityScreen
from src.clean.screens.structure_screen import StructureScreen

logger = logging.getLogger(__name__)


class CleanError(Exception):
    """Raised when a FATAL violation stops the batch."""


@dataclass
class CleanResult:
    """Outcome of run_clean_phase().

    Attributes:
        batch_id:           ETL batch identifier.
        entities_processed: Entities that completed successfully.
        total_violations:   Sum of all violations across all entities.
        fatal_encountered:  True if a FATAL violation stopped the batch.
        stopped_at:         Entity name where FATAL was found (or None).
        audit_records:      One AuditRecord per processed entity.
        clean_row_counts:   Entity → number of clean rows written.
    """

    batch_id: str
    entities_processed: list[str] = field(default_factory=list)
    total_violations: int = 0
    fatal_encountered: bool = False
    stopped_at: Optional[str] = None
    audit_records: list[AuditRecord] = field(default_factory=list)
    clean_row_counts: dict[str, int] = field(default_factory=dict)


# PK column per entity — used for record_id in ScreenResult.
_ENTITY_PK: dict[str, Optional[str]] = {
    "customers":          "CustomerID",
    "orders":             "OrderID",
    "order-details":      None,
    "products":           "ProductID",
    "categories":         "CategoryID",
    "suppliers":          "SupplierID",
    "employees":          "EmployeeID",
    "territories":        "TerritoryID",
    "employee-territories": None,
}

# Processing order respects referential dependencies.
_PROCESSING_ORDER = [
    "categories", "suppliers", "customers", "employees", "territories",
    "products", "orders", "order-details", "employee-territories",
]


def run_clean_phase(
    batch_id: str,
    raw_dir: Path,
    staging_dir: Path = Path("data/staging/cleaned"),
    error_dir: Path = Path("data/error"),
    baseline_dir: Path = Path("data/staging/_baselines"),
    config_path: Path = Path("config/quality_rules.yaml"),
    engine: Optional[Engine] = None,
    source_system: str = "northwind",
) -> CleanResult:
    """Run all 4 quality screens for every CSV in *raw_dir*.

    Args:
        batch_id:     ETL run identifier (e.g. ``"etl-2024-06-25-103015"``).
        raw_dir:      Snapshot directory containing raw CSV files.
        staging_dir:  Destination for cleaned parquet files.
        error_dir:    Destination for quarantined rows.
        baseline_dir: Reasonability baseline JSON files.
        config_path:  Path to quality_rules.yaml.
        engine:       SQLAlchemy engine (defaults to get_engine()).
        source_system: Logical source name.

    Returns:
        CleanResult describing what happened.
    """
    result = CleanResult(batch_id=batch_id)
    event_logger = ErrorEventLogger(engine=engine)
    audit_builder = AuditDimensionBuilder(engine=engine)

    staging_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir = error_dir / "quarantine"
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # Load all available CSVs first (needed for structure screens).
    loaded_dfs: dict[str, pd.DataFrame] = {}
    for csv_path in sorted(raw_dir.glob("*.csv")):
        entity = csv_path.stem
        try:
            loaded_dfs[entity] = pd.read_csv(csv_path, on_bad_lines="skip")
            logger.info("[CLEAN] batch=%s loaded %s (%d rows)",
                        batch_id, entity, len(loaded_dfs[entity]))
        except Exception as exc:
            logger.error("[CLEAN] Failed to read %s: %s", csv_path, exc)

    # Process in dependency order; skip entities not present in raw_dir.
    processing_order = [e for e in _PROCESSING_ORDER if e in loaded_dfs]
    for entity_name in processing_order:
        df = loaded_dfs[entity_name]
        pk_col = _ENTITY_PK.get(entity_name)
        violations: list[ScreenResult] = []

        logger.info("[CLEAN] batch=%s entity=%s rows=%d — running screens",
                    batch_id, entity_name, len(df))

        # 1. Column property screen
        col_screen = ColumnPropertyScreen.from_config(
            entity_name, config_path=config_path, pk_column=pk_col
        )
        violations.extend(col_screen.check(df))

        # 2. Structure screen (referential integrity)
        struct_screen = StructureScreen.from_config(
            entity_name, reference_dfs=loaded_dfs,
            config_path=config_path, pk_column=pk_col,
        )
        violations.extend(struct_screen.check(df))

        # 3. Data rule screen (cross-column rules)
        data_screen = DataRuleScreen.from_config(
            entity_name, config_path=config_path, pk_column=pk_col
        )
        violations.extend(data_screen.check(df))

        # 4. Reasonability screen (statistical drift)
        reason_screen = ReasonabilityScreen.from_config(
            entity_name, config_path=config_path,
            baseline_dir=baseline_dir, pk_column=pk_col,
        )
        violations.extend(reason_screen.check(df))

        result.total_violations += len(violations)

        # Check for FATAL → stop batch
        fatal = [v for v in violations if v.severity == Severity.FATAL]
        if fatal:
            logger.error(
                "[CLEAN] batch=%s FATAL violation in entity=%s — stopping batch",
                batch_id, entity_name,
            )
            event_logger.persist(violations, batch_id, source_system, entity_name)
            result.fatal_encountered = True
            result.stopped_at = entity_name
            return result

        # Persist error events for WARN / ERROR
        non_info = [v for v in violations if v.severity != Severity.INFO]
        if non_info:
            event_logger.persist(non_info, batch_id, source_system, entity_name)

        # Quarantine ERROR rows
        error_pks = {
            v.record_id
            for v in violations
            if v.severity == Severity.ERROR and v.record_id is not None
        }
        if error_pks and pk_col and pk_col in df.columns:
            quarantine_df = df[df[pk_col].isin(error_pks)]
            q_path = quarantine_dir / f"{entity_name}.parquet"
            quarantine_df.to_parquet(q_path, index=False)
            logger.info("[CLEAN] batch=%s quarantined %d rows → %s",
                        batch_id, len(quarantine_df), q_path)
            clean_df = df[~df[pk_col].isin(error_pks)]
        else:
            clean_df = df

        # Write cleaned data to staging
        clean_path = staging_dir / f"{entity_name}.parquet"
        clean_df.to_parquet(clean_path, index=False)

        result.clean_row_counts[entity_name] = len(clean_df)
        result.entities_processed.append(entity_name)

        # Build and persist audit record
        audit_rec = audit_builder.build(
            batch_id=batch_id,
            source_system=source_system,
            source_file=entity_name,
            total_rows=len(df),
            violations=violations,
        )
        result.audit_records.append(audit_rec)

        logger.info(
            "[CLEAN] batch=%s entity=%s done — violations=%d quality_score=%.4f",
            batch_id, entity_name, len(violations), audit_rec.quality_score,
        )

    return result
