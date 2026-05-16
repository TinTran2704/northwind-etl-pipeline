"""Tests for src/orchestration/pipeline.py — Subsystem #22."""

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.orchestration.pipeline import PhaseResult, Pipeline, PipelineResult


# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

def _make_engine():
    """Return a mock SQLAlchemy engine whose context-managers work."""
    engine = MagicMock()
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = conn
    engine.begin.return_value = conn
    return engine


@dataclass
class _FakeDeliverResult:
    batch_id: str = "b"
    dims_loaded: dict = field(default_factory=lambda: {"dim_customer": 10})
    fact_rows: int = 100
    agg_rows: int = 5
    errors: list = field(default_factory=list)


@dataclass
class _FakeCleanResult:
    batch_id: str = "b"
    clean_row_counts: dict = field(default_factory=lambda: {"customers": 91})
    total_violations: int = 2


@dataclass
class _FakeConformResult:
    batch_id: str = "b"
    golden_records: dict = field(default_factory=lambda: {"customers": 91})


# ---------------------------------------------------------------------------
# test_full_pipeline_success
# ---------------------------------------------------------------------------

class TestFullPipelineSuccess:
    def test_all_phases_run_and_metadata_updated(self, tmp_path):
        engine = _make_engine()

        # Status check returns None (not yet run)
        fetch_conn = MagicMock()
        fetch_conn.__enter__ = MagicMock(return_value=fetch_conn)
        fetch_conn.__exit__ = MagicMock(return_value=False)
        fetch_conn.execute.return_value.fetchone.return_value = None
        engine.connect.return_value = fetch_conn

        pipeline = Pipeline(engine=engine, staging_dir=tmp_path / "staging")

        extract_pr = PhaseResult(phase="extract", success=True, rows_out=500)
        clean_pr   = PhaseResult(phase="clean",   success=True, rows_out=480)
        conform_pr = PhaseResult(phase="conform",  success=True, rows_out=480)
        deliver_pr = PhaseResult(phase="deliver",  success=True, rows_out=100,
                                 details={"fact_rows": 100})

        with (
            patch.object(pipeline, "_run_extract", return_value=extract_pr),
            patch.object(pipeline, "_run_clean",   return_value=clean_pr),
            patch.object(pipeline, "_run_conform",  return_value=conform_pr),
            patch.object(pipeline, "_run_deliver",  return_value=deliver_pr),
            patch.object(pipeline, "_write_process_metadata"),
            patch.object(pipeline, "_insert_etl_run"),
            patch.object(pipeline, "_update_etl_run") as mock_update,
        ):
            result = pipeline.run(batch_id="batch-test-001")

        assert result.status == "SUCCESS"
        assert len(result.phases) == 4
        assert {p.phase for p in result.phases} == {"extract", "clean", "conform", "deliver"}
        mock_update.assert_called_once_with(result)


# ---------------------------------------------------------------------------
# test_pipeline_idempotent
# ---------------------------------------------------------------------------

class TestPipelineIdempotent:
    def test_second_run_same_batch_id_skipped(self, tmp_path):
        engine = _make_engine()

        # _is_batch_success → True on first call
        fetch_conn = MagicMock()
        fetch_conn.__enter__ = MagicMock(return_value=fetch_conn)
        fetch_conn.__exit__ = MagicMock(return_value=False)
        fetch_conn.execute.return_value.fetchone.return_value = ("SUCCESS",)
        engine.connect.return_value = fetch_conn

        pipeline = Pipeline(engine=engine, staging_dir=tmp_path / "staging")
        result = pipeline.run(batch_id="batch-already-done")

        assert result.status == "SKIPPED"
        assert result.phases == []

    def test_first_run_not_skipped_when_no_prior_record(self, tmp_path):
        engine = _make_engine()

        fetch_conn = MagicMock()
        fetch_conn.__enter__ = MagicMock(return_value=fetch_conn)
        fetch_conn.__exit__ = MagicMock(return_value=False)
        fetch_conn.execute.return_value.fetchone.return_value = None
        engine.connect.return_value = fetch_conn

        pipeline = Pipeline(engine=engine, staging_dir=tmp_path / "staging")

        with (
            patch.object(pipeline, "_run_extract",
                         return_value=PhaseResult(phase="extract", success=True)),
            patch.object(pipeline, "_run_clean",
                         return_value=PhaseResult(phase="clean", success=True)),
            patch.object(pipeline, "_run_conform",
                         return_value=PhaseResult(phase="conform", success=True)),
            patch.object(pipeline, "_run_deliver",
                         return_value=PhaseResult(phase="deliver", success=True)),
            patch.object(pipeline, "_write_process_metadata"),
            patch.object(pipeline, "_insert_etl_run"),
            patch.object(pipeline, "_update_etl_run"),
        ):
            result = pipeline.run(batch_id="batch-new")

        assert result.status == "SUCCESS"


# ---------------------------------------------------------------------------
# test_pipeline_phase_failure
# ---------------------------------------------------------------------------

class TestPipelinePhaseFailure:
    def test_clean_fails_pipeline_status_failed(self, tmp_path):
        engine = _make_engine()

        fetch_conn = MagicMock()
        fetch_conn.__enter__ = MagicMock(return_value=fetch_conn)
        fetch_conn.__exit__ = MagicMock(return_value=False)
        fetch_conn.execute.return_value.fetchone.return_value = None
        engine.connect.return_value = fetch_conn

        pipeline = Pipeline(engine=engine, staging_dir=tmp_path / "staging")

        failed_pr = PhaseResult(phase="clean", success=False, error="DiskFull")

        with (
            patch.object(pipeline, "_run_extract",
                         return_value=PhaseResult(phase="extract", success=True)),
            patch.object(pipeline, "_run_clean", return_value=failed_pr),
            patch.object(pipeline, "_run_conform") as mock_conform,
            patch.object(pipeline, "_run_deliver") as mock_deliver,
            patch.object(pipeline, "_write_process_metadata"),
            patch.object(pipeline, "_insert_etl_run"),
            patch.object(pipeline, "_update_etl_run"),
        ):
            result = pipeline.run(batch_id="batch-fail")

        assert result.status == "FAILED"
        assert result.error == "DiskFull"
        # conform and deliver must NOT have been called
        mock_conform.assert_not_called()
        mock_deliver.assert_not_called()

    def test_failed_phase_still_updates_etl_run(self, tmp_path):
        engine = _make_engine()

        fetch_conn = MagicMock()
        fetch_conn.__enter__ = MagicMock(return_value=fetch_conn)
        fetch_conn.__exit__ = MagicMock(return_value=False)
        fetch_conn.execute.return_value.fetchone.return_value = None
        engine.connect.return_value = fetch_conn

        pipeline = Pipeline(engine=engine, staging_dir=tmp_path / "staging")

        with (
            patch.object(pipeline, "_run_extract",
                         return_value=PhaseResult(phase="extract", success=False,
                                                  error="Network timeout")),
            patch.object(pipeline, "_write_process_metadata"),
            patch.object(pipeline, "_insert_etl_run"),
            patch.object(pipeline, "_update_etl_run") as mock_update,
        ):
            result = pipeline.run(batch_id="batch-fail2")

        assert result.status == "FAILED"
        mock_update.assert_called_once()


# ---------------------------------------------------------------------------
# test_cli_run
# ---------------------------------------------------------------------------

class TestCliRun:
    def test_cli_run_prints_help_without_subcommand(self):
        result = subprocess.run(
            [sys.executable, "-m", "src.orchestration.pipeline"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        combined = result.stdout + result.stderr
        assert "usage" in combined.lower() or "run" in combined.lower()

    def test_cli_status_subcommand_accepted(self):
        """status subcommand should be accepted (may fail to connect but not due to syntax)."""
        result = subprocess.run(
            [sys.executable, "-m", "src.orchestration.pipeline", "--help"],
            capture_output=True, text=True,
            cwd=str(Path(__file__).resolve().parents[2]),
        )
        assert result.returncode == 0
        assert "run" in result.stdout
        assert "status" in result.stdout
