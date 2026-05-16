"""
Orchestration Pipeline — Kimball Subsystem #22.

Coordinates Extract → Clean → Conform → Deliver in sequence.
Tracks process metadata in metadata.etl_runs and
data/warehouse/_meta/runs/{batch_id}.json.

Usage (CLI):
    python -m src.orchestration.pipeline run
    python -m src.orchestration.pipeline run --batch-id custom-id
    python -m src.orchestration.pipeline run --phase extract
    python -m src.orchestration.pipeline status
"""

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import yaml
from sqlalchemy import Engine, text

logger = logging.getLogger(__name__)

_DEFAULT_STAGING_DIR = Path("data/staging")
_DEFAULT_RAW_ROOT = Path("data/raw/northwind")
_META_RUNS_DIR = Path("data/warehouse/_meta/runs")
_SOURCES_CONFIG = Path("config/sources.yaml")


class PipelineError(Exception):
    """Raised when a phase fails and the pipeline cannot continue."""


@dataclass
class PhaseResult:
    """Outcome of a single pipeline phase.

    Attributes:
        phase:        Phase name (extract/clean/conform/deliver).
        success:      True if the phase completed without error.
        duration_sec: Wall-clock seconds.
        rows_in:      Records entering this phase.
        rows_out:     Records leaving this phase (after filtering/errors).
        details:      Phase-specific metadata dict.
        error:        Exception message if success is False.
    """

    phase: str
    success: bool
    duration_sec: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class PipelineResult:
    """Outcome of Pipeline.run().

    Attributes:
        batch_id:    ETL batch identifier.
        status:      SUCCESS, FAILED, or SKIPPED.
        started_at:  UTC ISO timestamp.
        ended_at:    UTC ISO timestamp (or None if not yet finished).
        phases:      PhaseResult for each executed phase.
        error:       Top-level error message (if status == FAILED).
    """

    batch_id: str
    status: str = "RUNNING"
    started_at: str = ""
    ended_at: Optional[str] = None
    phases: list[PhaseResult] = field(default_factory=list)
    error: Optional[str] = None


class Pipeline:
    """Orchestrate the full ETL pipeline.

    Args:
        engine:      SQLAlchemy engine connected to northwind_dw.
        staging_dir: Root staging directory.
        raw_root:    Root raw data directory (for extract + clean).
        config_dir:  Config directory (for conformance rules, etc.).
    """

    def __init__(
        self,
        engine: Engine,
        staging_dir: Path = _DEFAULT_STAGING_DIR,
        raw_root: Path = _DEFAULT_RAW_ROOT,
        config_dir: Path = Path("config"),
    ) -> None:
        self._engine = engine
        self._staging_dir = staging_dir
        self._raw_root = raw_root
        self._config_dir = config_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        batch_id: Optional[str] = None,
        phase_only: Optional[str] = None,
    ) -> PipelineResult:
        """Run the full pipeline (or a single phase).

        Args:
            batch_id:   Explicit batch identifier. Auto-generated when None.
            phase_only: If set, run only this phase (extract/clean/conform/deliver).

        Returns:
            PipelineResult with status and per-phase details.
        """
        if not batch_id:
            batch_id = "etl-" + datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

        started_at = datetime.now(timezone.utc).isoformat()
        result = PipelineResult(batch_id=batch_id, started_at=started_at)

        # Idempotency: skip already-successful batches
        if self._is_batch_success(batch_id):
            logger.warning(
                "[PIPELINE] batch=%s already SUCCESS — skipping", batch_id
            )
            result.status = "SKIPPED"
            return result

        self._insert_etl_run(batch_id)
        logger.info("[PIPELINE] batch=%s started at %s", batch_id, started_at)

        try:
            if phase_only:
                self._run_single_phase(batch_id, phase_only, result)
            else:
                self._run_all_phases(batch_id, result)
        except PipelineError as exc:
            result.status = "FAILED"
            result.error = str(exc)
            logger.error("[PIPELINE] batch=%s FAILED: %s", batch_id, exc)
        except Exception as exc:
            result.status = "FAILED"
            result.error = f"Unexpected error: {exc}"
            logger.error("[PIPELINE] batch=%s unexpected error: %s", batch_id, exc, exc_info=True)

        if result.status == "RUNNING":
            result.status = "SUCCESS"

        result.ended_at = datetime.now(timezone.utc).isoformat()
        self._update_etl_run(result)
        self._write_process_metadata(result)

        logger.info(
            "[PIPELINE] batch=%s %s — phases=%d duration=%.1fs",
            batch_id, result.status, len(result.phases),
            sum(p.duration_sec for p in result.phases),
        )
        return result

    def run_phase(
        self,
        phase_name: str,
        func: Callable,
        *args: Any,
        rows_in: int = 0,
        **kwargs: Any,
    ) -> PhaseResult:
        """Wrap a phase function with timing, logging, and error handling.

        Args:
            phase_name: Human-readable phase name.
            func:       Callable that executes the phase.
            *args:      Positional arguments for *func*.
            rows_in:    Input row count (informational).
            **kwargs:   Keyword arguments for *func*.

        Returns:
            PhaseResult.

        Raises:
            PipelineError: If *func* raises any exception.
        """
        logger.info("[PIPELINE] phase=%s START rows_in=%d", phase_name, rows_in)
        t0 = time.monotonic()
        pr = PhaseResult(phase=phase_name, success=False, rows_in=rows_in)
        try:
            phase_result = func(*args, **kwargs)
            pr.duration_sec = time.monotonic() - t0
            pr.success = True
            pr.rows_out = self._extract_rows_out(phase_result)
            pr.details = self._extract_details(phase_result)
            logger.info(
                "[PIPELINE] phase=%s END rows_out=%d duration=%.2fs",
                phase_name, pr.rows_out, pr.duration_sec,
            )
        except Exception as exc:
            pr.duration_sec = time.monotonic() - t0
            pr.error = str(exc)
            logger.error(
                "[PIPELINE] phase=%s FAILED after %.2fs: %s",
                phase_name, pr.duration_sec, exc,
            )
            raise PipelineError(f"{phase_name} failed: {exc}") from exc
        return pr

    # ------------------------------------------------------------------
    # Internal helpers — phase runners
    # ------------------------------------------------------------------

    def _run_all_phases(self, batch_id: str, result: PipelineResult) -> None:
        phases_map = {
            "extract": self._run_extract,
            "clean":   self._run_clean,
            "conform": self._run_conform,
            "deliver": self._run_deliver,
        }
        for phase_name, runner in phases_map.items():
            pr = runner(batch_id)
            result.phases.append(pr)
            if not pr.success:
                result.status = "FAILED"
                result.error = pr.error
                return

    def _run_single_phase(self, batch_id: str, phase: str, result: PipelineResult) -> None:
        runners = {
            "extract": self._run_extract,
            "clean":   self._run_clean,
            "conform": self._run_conform,
            "deliver": self._run_deliver,
        }
        runner = runners.get(phase)
        if not runner:
            raise PipelineError(f"Unknown phase: {phase!r}. Choose from {list(runners)}")
        pr = runner(batch_id)
        result.phases.append(pr)
        if not pr.success:
            result.status = "FAILED"
            result.error = pr.error

    def _run_extract(self, batch_id: str) -> PhaseResult:
        from src.extract.http_csv_extractor import HttpCsvExtractor
        from src.extract.rest_json_extractor import RestJsonExtractor

        sources_conf = self._load_sources_config()
        total_rows = 0
        details: dict[str, Any] = {}

        # One fixed timestamp for the whole batch — all files share the same
        # snapshot directory so _find_latest_raw() returns a complete set.
        batch_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")

        def _do_extract() -> dict:
            nonlocal total_rows
            for src_name, src_cfg in sources_conf.get("sources", {}).items():
                src_type = src_cfg.get("type")
                target = Path(src_cfg.get("target_dir", f"data/raw/{src_name}"))

                if src_type == "http_csv":
                    base_url = src_cfg["base_url"]
                    # Pre-create the shared snapshot dir for this batch
                    batch_snapshot_dir = target / batch_ts
                    batch_snapshot_dir.mkdir(parents=True, exist_ok=True)
                    for fname in src_cfg.get("files", []):
                        ext = HttpCsvExtractor(
                            source_name=src_name,
                            base_url=base_url,
                            file_name=fname.rsplit(".", 1)[0],
                            target_dir=target,
                        )
                        # Force all files into the same batch snapshot directory
                        ext.get_snapshot_path = lambda _d=batch_snapshot_dir: _d
                        try:
                            r = ext.extract()
                            total_rows += r.row_count
                            details.setdefault(src_name, {})[fname] = {
                                "rows": r.row_count, "bytes": r.byte_size,
                            }
                            logger.info(
                                "[EXTRACT] source=%s file=%s rows=%d bytes=%d",
                                src_name, fname, r.row_count, r.byte_size,
                            )
                        except Exception as exc:
                            logger.warning("[EXTRACT] source=%s file=%s error: %s", src_name, fname, exc)

                elif src_type == "rest_json":
                    url = src_cfg["url"]
                    file_name = src_cfg.get("file_name", src_name)
                    ext_json = RestJsonExtractor(
                        source_name=src_name,
                        url=url,
                        file_name=file_name,
                        target_dir=target,
                    )
                    try:
                        r = ext_json.extract()
                        total_rows += r.row_count
                        details[src_name] = {"rows": r.row_count, "bytes": r.byte_size}
                        logger.info("[EXTRACT] source=%s rows=%d bytes=%d", src_name, r.row_count, r.byte_size)
                    except Exception as exc:
                        logger.warning("[EXTRACT] source=%s error: %s", src_name, exc)

            return details

        return self.run_phase("extract", _do_extract, rows_in=0)

    def _run_clean(self, batch_id: str) -> PhaseResult:
        from src.clean.pipeline import run_clean_phase
        from src.deliver.dim_builder import _find_latest_raw

        raw_dir = _find_latest_raw()
        if raw_dir is None:
            raw_dir = Path("data/raw/northwind")
            if not raw_dir.exists():
                raise PipelineError("No raw data directory found — run extract first")

        rows_in = len(list(raw_dir.glob("*.csv")))

        def _do_clean():
            return run_clean_phase(
                batch_id=batch_id,
                raw_dir=raw_dir,
                staging_dir=self._staging_dir / "cleaned",
                error_dir=Path("data/error"),
                engine=self._engine,
            )

        return self.run_phase("clean", _do_clean, rows_in=rows_in)

    def _run_conform(self, batch_id: str) -> PhaseResult:
        from src.conform.pipeline import run_conform_phase

        def _do_conform():
            return run_conform_phase(
                batch_id=batch_id,
                staging_dir=self._staging_dir,
                config_dir=self._config_dir,
            )

        return self.run_phase("conform", _do_conform, rows_in=0)

    def _run_deliver(self, batch_id: str) -> PhaseResult:
        from src.deliver.pipeline import run_deliver_phase

        def _do_deliver():
            return run_deliver_phase(
                batch_id=batch_id,
                staging_dir=self._staging_dir,
                engine=self._engine,
                config_dir=self._config_dir,
            )

        return self.run_phase("deliver", _do_deliver, rows_in=0)

    # ------------------------------------------------------------------
    # Internal helpers — metadata
    # ------------------------------------------------------------------

    def _is_batch_success(self, batch_id: str) -> bool:
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text("SELECT status FROM metadata.etl_runs WHERE batch_id = :bid"),
                    {"bid": batch_id},
                ).fetchone()
            return row is not None and row[0] == "SUCCESS"
        except Exception:
            return False

    def _insert_etl_run(self, batch_id: str) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO metadata.etl_runs (batch_id, status)
                    VALUES (:bid, 'RUNNING')
                    ON CONFLICT (batch_id) DO UPDATE SET status = 'RUNNING', ended_at = NULL
                """), {"bid": batch_id})
        except Exception as exc:
            logger.warning("[PIPELINE] Could not insert etl_run: %s", exc)

    def _update_etl_run(self, result: PipelineResult) -> None:
        deliver_phase = next((p for p in result.phases if p.phase == "deliver"), None)
        rows_loaded = deliver_phase.details.get("fact_rows", 0) if deliver_phase else 0
        rows_extracted = sum(
            p.rows_out for p in result.phases if p.phase == "extract"
        )

        try:
            with self._engine.begin() as conn:
                conn.execute(text("""
                    UPDATE metadata.etl_runs
                    SET status        = :status,
                        ended_at      = NOW(),
                        rows_extracted= :rows_extracted,
                        rows_loaded   = :rows_loaded,
                        error_summary = :error_summary
                    WHERE batch_id = :bid
                """), {
                    "bid": result.batch_id,
                    "status": result.status,
                    "rows_extracted": rows_extracted,
                    "rows_loaded": rows_loaded,
                    "error_summary": result.error,
                })
        except Exception as exc:
            logger.warning("[PIPELINE] Could not update etl_run: %s", exc)

    def _write_process_metadata(self, result: PipelineResult) -> None:
        _META_RUNS_DIR.mkdir(parents=True, exist_ok=True)
        doc = {
            "batch_id": result.batch_id,
            "started_at": result.started_at,
            "ended_at": result.ended_at,
            "status": result.status,
            "error": result.error,
            "phases": {
                p.phase: {
                    "duration_sec": round(p.duration_sec, 2),
                    "rows_in": p.rows_in,
                    "rows_out": p.rows_out,
                    "success": p.success,
                    "details": p.details,
                    "error": p.error,
                }
                for p in result.phases
            },
        }
        out_path = _META_RUNS_DIR / f"{result.batch_id}.json"
        out_path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        logger.info("[PIPELINE] Process metadata written to %s", out_path)

    @staticmethod
    def _load_sources_config() -> dict:
        if _SOURCES_CONFIG.exists():
            return yaml.safe_load(_SOURCES_CONFIG.read_text(encoding="utf-8"))
        return {}

    @staticmethod
    def _extract_rows_out(phase_result: Any) -> int:
        if phase_result is None:
            return 0
        # deliver → fact_rows
        if hasattr(phase_result, "fact_rows"):
            return phase_result.fact_rows
        # clean → sum of clean_row_counts
        if hasattr(phase_result, "clean_row_counts"):
            return sum(phase_result.clean_row_counts.values())
        # conform → sum of golden_records
        if hasattr(phase_result, "golden_records"):
            return sum(phase_result.golden_records.values())
        # extract → dict of details
        if isinstance(phase_result, dict):
            return sum(
                v.get("rows", 0) if isinstance(v, dict) else 0
                for src in phase_result.values()
                for v in (src.values() if isinstance(src, dict) else [src])
            )
        return 0

    @staticmethod
    def _extract_details(phase_result: Any) -> dict:
        if phase_result is None:
            return {}
        if isinstance(phase_result, dict):
            return phase_result
        d: dict = {}
        for attr in (
            "dims_loaded", "fact_rows", "agg_rows", "errors",
            "entities_processed", "total_violations", "clean_row_counts",
            "entities_conformed", "duplicates_found", "golden_records",
        ):
            val = getattr(phase_result, attr, None)
            if val is not None:
                d[attr] = val
        return d


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cmd_run(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    from src.common.db import get_engine
    engine = get_engine()
    pipeline = Pipeline(engine=engine)
    result = pipeline.run(
        batch_id=args.batch_id or None,
        phase_only=args.phase or None,
    )
    print(f"\nbatch_id  : {result.batch_id}")
    print(f"status    : {result.status}")
    print(f"started_at: {result.started_at}")
    print(f"ended_at  : {result.ended_at}")
    if result.error:
        print(f"error     : {result.error}")
    print("\nPhase summary:")
    for pr in result.phases:
        status_str = "OK " if pr.success else "ERR"
        print(
            f"  [{status_str}] {pr.phase:<10} "
            f"rows_in={pr.rows_in:<6} rows_out={pr.rows_out:<6} "
            f"duration={pr.duration_sec:.1f}s"
        )
        if pr.error:
            print(f"          error: {pr.error}")


def _cmd_status(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.WARNING)
    from src.common.db import get_engine
    import pandas as pd
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT batch_id, status, started_at, ended_at,
                   rows_extracted, rows_loaded, error_summary
            FROM metadata.etl_runs
            ORDER BY started_at DESC
            LIMIT 5
        """, conn)
    if df.empty:
        print("No ETL runs found.")
        return
    print("\n=== Last 5 ETL runs ===")
    print(df.to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description="Northwind ETL Pipeline")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Execute the ETL pipeline")
    run_p.add_argument("--batch-id", dest="batch_id", default=None,
                       help="Explicit batch identifier (auto-generated if omitted)")
    run_p.add_argument("--phase", default=None,
                       choices=["extract", "clean", "conform", "deliver"],
                       help="Run only a specific phase")

    sub.add_parser("status", help="Show last 5 ETL runs")

    args = parser.parse_args()
    if args.command == "run":
        _cmd_run(args)
    elif args.command == "status":
        _cmd_status(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
