"""Tests for src/clean/audit_dimension_builder.py."""

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from src.clean.audit_dimension_builder import AuditDimensionBuilder, AuditRecord
from src.clean.screens.base_screen import ScreenResult, Severity


def _mock_engine(audit_sk: int = 1):
    engine = MagicMock()
    row = (audit_sk,)
    engine.begin.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = row
    return engine


def _violation(severity: Severity) -> ScreenResult:
    return ScreenResult(
        screen_name="test",
        severity=severity,
        record_id="R1",
        column_name=None,
        expected=None,
        actual=None,
        message="v",
    )


def _builder(audit_sk: int = 1) -> AuditDimensionBuilder:
    return AuditDimensionBuilder(engine=_mock_engine(audit_sk))


def _build(builder, violations, total_rows=10) -> AuditRecord:
    return builder.build(
        batch_id="batch-1",
        source_system="northwind",
        source_file="customers",
        total_rows=total_rows,
        violations=violations,
    )


class TestQualityScore:
    def test_no_violations_score_is_one(self):
        rec = _build(_builder(), [])
        assert rec.quality_score == 1.0

    def test_all_errors_reduces_score(self):
        # 5 ERRORs out of 10 rows → 1 - 5/10 = 0.5
        violations = [_violation(Severity.ERROR)] * 5
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.quality_score == pytest.approx(0.5, abs=1e-4)

    def test_warns_discounted_at_30pct(self):
        # 10 WARNs out of 10 rows → 1 - (10*0.3)/10 = 0.7
        violations = [_violation(Severity.WARN)] * 10
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.quality_score == pytest.approx(0.7, abs=1e-4)

    def test_mixed_errors_and_warns(self):
        # 2 ERRORs + 3 WARNs out of 10 → 1 - (2 + 3*0.3)/10 = 1 - 2.9/10 = 0.71
        violations = [_violation(Severity.ERROR)] * 2 + [_violation(Severity.WARN)] * 3
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.quality_score == pytest.approx(0.71, abs=1e-4)

    def test_score_clamped_to_zero(self):
        # 20 ERRORs out of 5 rows → raw = 1 - 20/5 = -3 → clamped to 0
        violations = [_violation(Severity.ERROR)] * 20
        rec = _build(_builder(), violations, total_rows=5)
        assert rec.quality_score == 0.0

    def test_fatal_counts_as_error_in_score(self):
        # 5 FATALs out of 10 → 1 - 5/10 = 0.5
        violations = [_violation(Severity.FATAL)] * 5
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.quality_score == pytest.approx(0.5, abs=1e-4)

    def test_info_violations_not_counted(self):
        # INFO violations do not reduce score
        violations = [_violation(Severity.INFO)] * 100
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.quality_score == 1.0


class TestHasAnomalies:
    def test_no_violations_no_anomalies(self):
        rec = _build(_builder(), [])
        assert rec.has_anomalies is False

    def test_warn_sets_anomalies(self):
        rec = _build(_builder(), [_violation(Severity.WARN)])
        assert rec.has_anomalies is True

    def test_error_sets_anomalies(self):
        rec = _build(_builder(), [_violation(Severity.ERROR)])
        assert rec.has_anomalies is True

    def test_fatal_sets_anomalies(self):
        rec = _build(_builder(), [_violation(Severity.FATAL)])
        assert rec.has_anomalies is True

    def test_info_only_no_anomalies(self):
        rec = _build(_builder(), [_violation(Severity.INFO)])
        assert rec.has_anomalies is False


class TestAuditRecord:
    def test_audit_sk_populated_from_db(self):
        rec = _build(_builder(audit_sk=42), [])
        assert rec.audit_sk == 42

    def test_reject_count_is_error_plus_fatal(self):
        violations = [_violation(Severity.ERROR), _violation(Severity.FATAL), _violation(Severity.WARN)]
        rec = _build(_builder(), violations, total_rows=10)
        assert rec.reject_row_count == 2  # ERROR + FATAL only

    def test_fields_populated_correctly(self):
        ts = datetime(2024, 6, 25, 10, 30, 0)
        rec = _builder().build(
            batch_id="b1",
            source_system="northwind",
            source_file="orders",
            total_rows=50,
            violations=[],
            run_timestamp=ts,
        )
        assert rec.etl_batch_id == "b1"
        assert rec.source_system == "northwind"
        assert rec.source_file == "orders"
        assert rec.extract_row_count == 50
        assert rec.etl_run_timestamp == ts

    def test_ddl_executed_on_init(self):
        engine = _mock_engine()
        conn = engine.begin.return_value.__enter__.return_value
        AuditDimensionBuilder(engine=engine)
        assert conn.execute.called
