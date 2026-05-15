"""Tests for src/clean/pipeline.py."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.clean.pipeline import CleanResult, run_clean_phase
from src.clean.screens.base_screen import ScreenResult, Severity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _violation(severity: Severity, record_id=None, screen="col.not_null") -> ScreenResult:
    return ScreenResult(
        screen_name=screen,
        severity=severity,
        record_id=record_id,
        column_name="CustomerID",
        expected="not null",
        actual=None,
        message=f"{severity} violation",
    )


def _mock_engine(audit_sk: int = 1):
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = (audit_sk,)
    return engine


def _write_customers_csv(raw_dir: Path, rows=("ALFKI,Alfreds", "ANATR,Ana Trujillo")):
    csv = raw_dir / "customers.csv"
    lines = ["CustomerID,CompanyName"] + list(rows)
    csv.write_text("\n".join(lines))


def _run(tmp_path, violations_by_screen=None, rows=("ALFKI,Alfreds", "ANATR,Ana Trujillo")):
    """Helper: run clean phase with patched screens returning given violations."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    _write_customers_csv(raw_dir, rows)

    cfg = tmp_path / "quality_rules.yaml"
    cfg.write_text("screens: {}")

    violations_by_screen = violations_by_screen or {}

    with patch("src.clean.pipeline.ColumnPropertyScreen") as mc, \
         patch("src.clean.pipeline.StructureScreen") as ms, \
         patch("src.clean.pipeline.DataRuleScreen") as md, \
         patch("src.clean.pipeline.ReasonabilityScreen") as mr:

        mc.from_config.return_value.check.return_value = violations_by_screen.get("col", [])
        ms.from_config.return_value.check.return_value = violations_by_screen.get("struct", [])
        md.from_config.return_value.check.return_value = violations_by_screen.get("data", [])
        mr.from_config.return_value.check.return_value = violations_by_screen.get("reason", [])

        return run_clean_phase(
            batch_id="test-batch",
            raw_dir=raw_dir,
            staging_dir=tmp_path / "staging",
            error_dir=tmp_path / "error",
            baseline_dir=tmp_path / "baselines",
            config_path=cfg,
            engine=_mock_engine(),
        )


# ---------------------------------------------------------------------------
# FATAL severity
# ---------------------------------------------------------------------------

class TestFatalStopsBatch:
    def test_fatal_sets_fatal_encountered(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.FATAL, record_id="ALFKI")]})
        assert result.fatal_encountered is True

    def test_fatal_sets_stopped_at(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.FATAL)]})
        assert result.stopped_at == "customers"

    def test_fatal_entity_not_in_processed(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.FATAL)]})
        assert "customers" not in result.entities_processed

    def test_fatal_no_audit_records(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.FATAL)]})
        assert result.audit_records == []

    def test_no_clean_file_written_on_fatal(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.FATAL)]})
        assert not (tmp_path / "staging" / "customers.parquet").exists()


# ---------------------------------------------------------------------------
# WARN severity
# ---------------------------------------------------------------------------

class TestWarnPassesRow:
    def test_warn_not_fatal(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.WARN)]})
        assert result.fatal_encountered is False

    def test_warn_entity_is_processed(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.WARN)]})
        assert "customers" in result.entities_processed

    def test_warn_audit_has_anomalies(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.WARN)]})
        assert len(result.audit_records) == 1
        assert result.audit_records[0].has_anomalies is True

    def test_warn_rows_not_quarantined(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.WARN, record_id="ALFKI")]})
        q_path = tmp_path / "error" / "quarantine" / "customers.parquet"
        assert not q_path.exists()

    def test_warn_all_rows_in_clean_file(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.WARN)]})
        clean_df = pd.read_parquet(tmp_path / "staging" / "customers.parquet")
        assert len(clean_df) == 2


# ---------------------------------------------------------------------------
# ERROR severity — quarantine
# ---------------------------------------------------------------------------

class TestErrorQuarantine:
    def test_error_row_in_quarantine_file(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        q_path = tmp_path / "error" / "quarantine" / "customers.parquet"
        assert q_path.exists()
        quarantined = pd.read_parquet(q_path)
        assert "ALFKI" in quarantined["CustomerID"].values

    def test_error_row_not_in_clean_file(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        clean_df = pd.read_parquet(tmp_path / "staging" / "customers.parquet")
        assert "ALFKI" not in clean_df["CustomerID"].values

    def test_non_error_row_remains_clean(self, tmp_path):
        _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        clean_df = pd.read_parquet(tmp_path / "staging" / "customers.parquet")
        assert "ANATR" in clean_df["CustomerID"].values

    def test_error_entity_is_processed(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        assert "customers" in result.entities_processed

    def test_error_audit_has_anomalies(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        assert result.audit_records[0].has_anomalies is True

    def test_clean_row_count_excludes_quarantined(self, tmp_path):
        result = _run(tmp_path, {"col": [_violation(Severity.ERROR, record_id="ALFKI")]})
        assert result.clean_row_counts.get("customers") == 1  # 2 total - 1 quarantined


# ---------------------------------------------------------------------------
# Clean run (no violations)
# ---------------------------------------------------------------------------

class TestCleanRun:
    def test_no_violations_all_rows_in_staging(self, tmp_path):
        result = _run(tmp_path)
        clean_df = pd.read_parquet(tmp_path / "staging" / "customers.parquet")
        assert len(clean_df) == 2

    def test_no_violations_no_anomalies(self, tmp_path):
        result = _run(tmp_path)
        assert result.audit_records[0].has_anomalies is False

    def test_no_violations_quality_score_is_one(self, tmp_path):
        result = _run(tmp_path)
        assert result.audit_records[0].quality_score == 1.0

    def test_entity_in_processed_list(self, tmp_path):
        result = _run(tmp_path)
        assert "customers" in result.entities_processed

    def test_no_quarantine_file_when_clean(self, tmp_path):
        _run(tmp_path)
        assert not (tmp_path / "error" / "quarantine" / "customers.parquet").exists()


# ---------------------------------------------------------------------------
# Violation counting
# ---------------------------------------------------------------------------

class TestViolationCounting:
    def test_total_violations_summed_across_screens(self, tmp_path):
        result = _run(tmp_path, {
            "col": [_violation(Severity.WARN)],
            "struct": [_violation(Severity.WARN)],
            "data": [_violation(Severity.ERROR, record_id="ALFKI")],
        })
        assert result.total_violations == 3

    def test_missing_csv_entity_skipped_gracefully(self, tmp_path):
        # Only customers.csv present; orders/products/etc. are all absent
        result = _run(tmp_path)
        # Should still complete without error
        assert isinstance(result, CleanResult)


# ---------------------------------------------------------------------------
# Result fields
# ---------------------------------------------------------------------------

class TestCleanResult:
    def test_batch_id_propagated(self, tmp_path):
        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        _write_customers_csv(raw_dir)
        cfg = tmp_path / "q.yaml"
        cfg.write_text("screens: {}")

        with patch("src.clean.pipeline.ColumnPropertyScreen") as mc, \
             patch("src.clean.pipeline.StructureScreen") as ms, \
             patch("src.clean.pipeline.DataRuleScreen") as md, \
             patch("src.clean.pipeline.ReasonabilityScreen") as mr:
            mc.from_config.return_value.check.return_value = []
            ms.from_config.return_value.check.return_value = []
            md.from_config.return_value.check.return_value = []
            mr.from_config.return_value.check.return_value = []

            result = run_clean_phase(
                batch_id="my-batch-123",
                raw_dir=raw_dir,
                staging_dir=tmp_path / "staging",
                error_dir=tmp_path / "error",
                baseline_dir=tmp_path / "baselines",
                config_path=cfg,
                engine=_mock_engine(),
            )

        assert result.batch_id == "my-batch-123"
