"""
Error Event Logger — Kimball Subsystem #5.

Persists data-quality violations to staging.error_events in PostgreSQL.
Schema follows docs/06-clean-phase.md §6.7.
"""

import logging
from datetime import datetime
from typing import List, Optional

from sqlalchemy import Engine, text

from src.clean.screens.base_screen import ScreenResult
from src.common.db import get_engine

logger = logging.getLogger(__name__)

_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.error_events (
    error_event_id   BIGSERIAL PRIMARY KEY,
    etl_batch_id     VARCHAR(60)  NOT NULL,
    event_timestamp  TIMESTAMP    NOT NULL,
    source_system    VARCHAR(40)  NOT NULL,
    source_table     VARCHAR(80)  NOT NULL,
    source_record_pk VARCHAR(200),
    screen_name      VARCHAR(100) NOT NULL,
    screen_severity  VARCHAR(10)  NOT NULL,
    column_name      VARCHAR(80),
    expected_value   TEXT,
    actual_value     TEXT,
    message          TEXT
);
"""


class ErrorEventLogger:
    """Insert ScreenResult violations into staging.error_events.

    Args:
        engine: SQLAlchemy engine. Defaults to get_engine() if not provided.
    """

    def __init__(self, engine: Optional[Engine] = None) -> None:
        self._engine = engine or get_engine()
        self._ensure_table()

    def persist(
        self,
        violations: List[ScreenResult],
        batch_id: str,
        source_system: str,
        source_table: str,
    ) -> int:
        """INSERT all violations into staging.error_events.

        Args:
            violations:    List of ScreenResult objects to persist.
            batch_id:      ETL batch identifier.
            source_system: Logical source name, e.g. ``"northwind"``.
            source_table:  Entity/table name, e.g. ``"customers"``.

        Returns:
            Number of rows inserted.
        """
        if not violations:
            return 0

        now = datetime.utcnow()
        rows = [
            {
                "etl_batch_id":     batch_id,
                "event_timestamp":  now,
                "source_system":    source_system,
                "source_table":     source_table,
                "source_record_pk": str(v.record_id) if v.record_id is not None else None,
                "screen_name":      v.screen_name,
                "screen_severity":  v.severity.value,
                "column_name":      v.column_name,
                "expected_value":   v.expected,
                "actual_value":     v.actual,
                "message":          v.message,
            }
            for v in violations
        ]

        sql = text("""
            INSERT INTO staging.error_events (
                etl_batch_id, event_timestamp, source_system, source_table,
                source_record_pk, screen_name, screen_severity,
                column_name, expected_value, actual_value, message
            ) VALUES (
                :etl_batch_id, :event_timestamp, :source_system, :source_table,
                :source_record_pk, :screen_name, :screen_severity,
                :column_name, :expected_value, :actual_value, :message
            )
        """)

        with self._engine.begin() as conn:
            conn.execute(sql, rows)

        logger.info(
            "[AUDIT] batch=%s source=%s/%s — persisted %d error events",
            batch_id, source_system, source_table, len(rows),
        )
        return len(rows)

    def get_summary(self, batch_id: str) -> dict:
        """Return violation counts by severity for *batch_id*.

        Returns:
            Dict with keys ``INFO``, ``WARN``, ``ERROR``, ``FATAL``, ``total``.
        """
        sql = text("""
            SELECT screen_severity, COUNT(*) AS cnt
            FROM staging.error_events
            WHERE etl_batch_id = :batch_id
            GROUP BY screen_severity
        """)
        with self._engine.connect() as conn:
            rows = conn.execute(sql, {"batch_id": batch_id}).fetchall()

        summary: dict = {"INFO": 0, "WARN": 0, "ERROR": 0, "FATAL": 0}
        for row in rows:
            summary[row[0]] = int(row[1])
        summary["total"] = sum(summary.values())
        return summary

    def _ensure_table(self) -> None:
        with self._engine.begin() as conn:
            for stmt in _DDL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
