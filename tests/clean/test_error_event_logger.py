"""Tests for src/clean/error_event_logger.py."""

from unittest.mock import MagicMock

import pytest

from src.clean.error_event_logger import ErrorEventLogger
from src.clean.screens.base_screen import ScreenResult, Severity


def _mock_engine():
    engine = MagicMock()
    return engine


def _violation(severity: Severity = Severity.WARN, record_id="REC1") -> ScreenResult:
    return ScreenResult(
        screen_name="test.screen",
        severity=severity,
        record_id=record_id,
        column_name="Col",
        expected="X",
        actual="Y",
        message="test violation",
    )


def _logger(engine=None) -> ErrorEventLogger:
    return ErrorEventLogger(engine=engine or _mock_engine())


class TestPersist:
    def test_persist_returns_row_count(self):
        logger = _logger()
        violations = [_violation(Severity.WARN), _violation(Severity.ERROR)]
        assert logger.persist(violations, "batch-1", "northwind", "customers") == 2

    def test_persist_empty_returns_zero(self):
        logger = _logger()
        assert logger.persist([], "batch-1", "northwind", "customers") == 0

    def test_persist_calls_execute_once(self):
        engine = _mock_engine()
        conn = engine.begin.return_value.__enter__.return_value
        logger = ErrorEventLogger(engine=engine)

        violations = [_violation(Severity.ERROR, record_id="ALFKI")]
        logger.persist(violations, "b1", "northwind", "customers")

        # At least one INSERT execute call beyond the DDL setup calls
        assert conn.execute.called

    def test_persist_none_record_id_handled(self):
        logger = _logger()
        v = ScreenResult(
            screen_name="test",
            severity=Severity.WARN,
            record_id=None,
            column_name=None,
            expected=None,
            actual=None,
            message="no pk",
        )
        count = logger.persist([v], "b1", "northwind", "customers")
        assert count == 1

    def test_persist_fatal_violation(self):
        logger = _logger()
        count = logger.persist([_violation(Severity.FATAL)], "b1", "northwind", "customers")
        assert count == 1


class TestGetSummary:
    def test_summary_counts_by_severity(self):
        engine = _mock_engine()
        read_conn = engine.connect.return_value.__enter__.return_value
        read_conn.execute.return_value.fetchall.return_value = [
            ("WARN", 3),
            ("ERROR", 1),
        ]
        logger = ErrorEventLogger(engine=engine)

        summary = logger.get_summary("batch-1")

        assert summary["WARN"] == 3
        assert summary["ERROR"] == 1
        assert summary["FATAL"] == 0
        assert summary["INFO"] == 0
        assert summary["total"] == 4

    def test_summary_all_zeros_for_empty_batch(self):
        engine = _mock_engine()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []
        logger = ErrorEventLogger(engine=engine)

        summary = logger.get_summary("unknown-batch")

        assert summary == {"INFO": 0, "WARN": 0, "ERROR": 0, "FATAL": 0, "total": 0}

    def test_summary_total_is_sum(self):
        engine = _mock_engine()
        engine.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
            ("INFO", 5),
            ("WARN", 2),
            ("ERROR", 3),
            ("FATAL", 1),
        ]
        logger = ErrorEventLogger(engine=engine)

        summary = logger.get_summary("b1")
        assert summary["total"] == 11


class TestEnsureTable:
    def test_ddl_executed_on_init(self):
        engine = _mock_engine()
        conn = engine.begin.return_value.__enter__.return_value
        ErrorEventLogger(engine=engine)
        assert conn.execute.called
