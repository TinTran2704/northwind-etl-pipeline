"""
Audit Dimension Builder — Kimball Subsystem #6.

Builds and persists AuditRecord rows to warehouse.dim_audit in PostgreSQL.
quality_score formula: 1 - (errors + warns*0.3) / max(total_rows, 1).
See docs/03-logical-data-map.md §3.7 and docs/06-clean-phase.md §6.8.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlalchemy import Engine, text

from src.clean.screens.base_screen import ScreenResult, Severity
from src.common.db import get_engine

logger = logging.getLogger(__name__)

_DDL = """
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_audit (
    audit_sk          BIGSERIAL PRIMARY KEY,
    etl_batch_id      VARCHAR(60)   NOT NULL,
    etl_run_timestamp TIMESTAMP     NOT NULL,
    source_system     VARCHAR(40)   NOT NULL,
    source_file       VARCHAR(120)  NOT NULL,
    extract_row_count INT           NOT NULL DEFAULT 0,
    reject_row_count  INT           NOT NULL DEFAULT 0,
    quality_score     DECIMAL(5,4)  NOT NULL DEFAULT 1.0,
    has_anomalies     BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at        TIMESTAMP     NOT NULL
);
"""


@dataclass
class AuditRecord:
    """One audit record per (batch, entity) pair.

    Attributes:
        etl_batch_id:      ETL run identifier.
        etl_run_timestamp: When this run started.
        source_system:     Logical source name.
        source_file:       Entity / file name.
        extract_row_count: Total rows in the raw snapshot.
        reject_row_count:  Rows quarantined (ERROR + FATAL violations).
        quality_score:     0.00–1.00 quality indicator.
        has_anomalies:     True if any WARN, ERROR or FATAL violations exist.
        created_at:        Timestamp of audit record creation.
        audit_sk:          Populated after persisting to PostgreSQL.
    """

    etl_batch_id: str
    etl_run_timestamp: datetime
    source_system: str
    source_file: str
    extract_row_count: int
    reject_row_count: int
    quality_score: float
    has_anomalies: bool
    created_at: datetime
    audit_sk: Optional[int] = None


class AuditDimensionBuilder:
    """Compute quality metrics and INSERT into warehouse.dim_audit.

    Args:
        engine: SQLAlchemy engine. Defaults to get_engine() if not provided.
    """

    def __init__(self, engine: Optional[Engine] = None) -> None:
        self._engine = engine or get_engine()
        self._ensure_table()

    def build(
        self,
        batch_id: str,
        source_system: str,
        source_file: str,
        total_rows: int,
        violations: List[ScreenResult],
        run_timestamp: Optional[datetime] = None,
    ) -> AuditRecord:
        """Compute metrics, persist to DB, return populated AuditRecord.

        Args:
            batch_id:       ETL run identifier.
            source_system:  Logical source name.
            source_file:    Entity / file name.
            total_rows:     Total rows in the raw snapshot.
            violations:     All ScreenResult objects for this entity/batch.
            run_timestamp:  Defaults to utcnow().

        Returns:
            AuditRecord with audit_sk populated from DB.
        """
        now = datetime.utcnow()
        ts = run_timestamp or now

        rejected = sum(
            1 for v in violations if v.severity in (Severity.ERROR, Severity.FATAL)
        )
        warned = sum(1 for v in violations if v.severity == Severity.WARN)
        raw_score = 1.0 - (rejected + warned * 0.3) / max(total_rows, 1)
        quality_score = round(max(0.0, raw_score), 4)

        record = AuditRecord(
            etl_batch_id=batch_id,
            etl_run_timestamp=ts,
            source_system=source_system,
            source_file=source_file,
            extract_row_count=total_rows,
            reject_row_count=rejected,
            quality_score=quality_score,
            has_anomalies=(rejected + warned) > 0,
            created_at=now,
        )

        sql = text("""
            INSERT INTO warehouse.dim_audit (
                etl_batch_id, etl_run_timestamp, source_system, source_file,
                extract_row_count, reject_row_count, quality_score,
                has_anomalies, created_at
            ) VALUES (
                :etl_batch_id, :etl_run_timestamp, :source_system, :source_file,
                :extract_row_count, :reject_row_count, :quality_score,
                :has_anomalies, :created_at
            ) RETURNING audit_sk
        """)

        with self._engine.begin() as conn:
            row = conn.execute(sql, {
                "etl_batch_id":      record.etl_batch_id,
                "etl_run_timestamp": record.etl_run_timestamp,
                "source_system":     record.source_system,
                "source_file":       record.source_file,
                "extract_row_count": record.extract_row_count,
                "reject_row_count":  record.reject_row_count,
                "quality_score":     record.quality_score,
                "has_anomalies":     record.has_anomalies,
                "created_at":        record.created_at,
            }).fetchone()
            record.audit_sk = row[0]

        logger.info(
            "[AUDIT] batch=%s %s/%s audit_sk=%s quality_score=%.4f",
            batch_id, source_system, source_file, record.audit_sk, quality_score,
        )
        return record

    def _ensure_table(self) -> None:
        with self._engine.begin() as conn:
            for stmt in _DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
