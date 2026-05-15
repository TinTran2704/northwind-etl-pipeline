"""
Reasonability Screen — Kimball Subsystem #4.

Statistical drift detection:
  - Row count vs baseline (± tolerance %).
  - Numeric column mean drift vs baseline.

Baseline stored at data/staging/_baselines/{entity}.json.
First run: creates baseline and emits no violations.
See docs/06-clean-phase.md §6.3 level-4.
"""

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import yaml

from src.clean.screens.base_screen import BaseScreen, ScreenResult, Severity

logger = logging.getLogger(__name__)


@dataclass
class ReasonabilityRule:
    """A single reasonability rule from config YAML."""

    metric: str            # "row_count" | "column_mean"
    severity: str
    baseline: Optional[float] = None
    tolerance_pct: float = 20.0
    column: Optional[str] = None   # for column_mean metric


class ReasonabilityScreen(BaseScreen):
    """Detect statistical drift relative to a stored baseline.

    Args:
        entity:       Entity name.
        rules:        List of ReasonabilityRule.
        baseline_dir: Directory for baseline JSON files.
        pk_column:    PK column (used in results).
    """

    name = "reasonability"

    def __init__(
        self,
        entity: str,
        rules: list[ReasonabilityRule],
        baseline_dir: Path = Path("data/staging/_baselines"),
        pk_column: Optional[str] = None,
    ) -> None:
        self.entity = entity
        self.rules = rules
        self.baseline_dir = baseline_dir
        self.pk_column = pk_column

    @classmethod
    def from_config(
        cls,
        entity_name: str,
        config_path: Path = Path("config/quality_rules.yaml"),
        baseline_dir: Path = Path("data/staging/_baselines"),
        pk_column: Optional[str] = None,
    ) -> "ReasonabilityScreen":
        """Build screen from quality_rules.yaml."""
        try:
            with config_path.open(encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            cfg = {}

        raw = cfg.get("screens", {}).get(entity_name, {}).get("reasonability", [])
        rules = [
            ReasonabilityRule(
                metric=r["metric"],
                severity=r.get("severity", "WARN"),
                baseline=r.get("baseline"),
                tolerance_pct=float(r.get("tolerance_pct", 20.0)),
                column=r.get("column"),
            )
            for r in (raw or [])
        ]
        return cls(entity=entity_name, rules=rules,
                   baseline_dir=baseline_dir, pk_column=pk_column)

    def check(self, df: pd.DataFrame) -> List[ScreenResult]:
        """Compare current stats to baseline; update baseline on first run."""
        baseline = self._load_baseline()
        if baseline is None:
            self._save_baseline(df)
            logger.info(
                "[ReasonabilityScreen] entity=%s first run — baseline created, no checks",
                self.entity,
            )
            return []

        results: list[ScreenResult] = []
        for rule in self.rules:
            if rule.metric == "row_count":
                results.extend(self._check_row_count(df, rule, baseline))
            elif rule.metric == "column_mean":
                results.extend(self._check_column_mean(df, rule, baseline))
            else:
                logger.warning("[ReasonabilityScreen] Unknown metric '%s'", rule.metric)

        self._save_baseline(df)
        return results

    # ── Metric checks ─────────────────────────────────────────────────────

    def _check_row_count(
        self, df: pd.DataFrame, rule: ReasonabilityRule, baseline: dict
    ) -> list[ScreenResult]:
        expected = rule.baseline or baseline.get("row_count")
        if expected is None:
            return []
        current = len(df)
        pct_diff = abs(current - expected) / max(expected, 1) * 100
        if pct_diff <= rule.tolerance_pct:
            return []
        return [ScreenResult(
            screen_name="reasonability.row_count",
            severity=Severity[rule.severity],
            record_id="DATASET",
            column_name=None,
            expected=f"{expected} ± {rule.tolerance_pct}%",
            actual=str(current),
            message=(
                f"[{self.entity}] row_count={current} deviates "
                f"{pct_diff:.1f}% from baseline {expected} "
                f"(tolerance {rule.tolerance_pct}%)"
            ),
        )]

    def _check_column_mean(
        self, df: pd.DataFrame, rule: ReasonabilityRule, baseline: dict
    ) -> list[ScreenResult]:
        col = rule.column
        if col is None or col not in df.columns:
            return []
        col_stats = baseline.get("column_stats", {}).get(col)
        if col_stats is None:
            return []
        expected_mean = col_stats.get("mean")
        if expected_mean is None:
            return []
        current_mean = pd.to_numeric(df[col], errors="coerce").mean()
        if pd.isna(current_mean):
            return []
        pct_diff = abs(current_mean - expected_mean) / max(abs(expected_mean), 1) * 100
        if pct_diff <= rule.tolerance_pct:
            return []
        return [ScreenResult(
            screen_name="reasonability.column_mean",
            severity=Severity[rule.severity],
            record_id="DATASET",
            column_name=col,
            expected=f"mean ≈ {expected_mean:.4f} ± {rule.tolerance_pct}%",
            actual=f"{current_mean:.4f}",
            message=(
                f"[{self.entity}] {col} mean={current_mean:.4f} deviates "
                f"{pct_diff:.1f}% from baseline {expected_mean:.4f}"
            ),
        )]

    # ── Baseline persistence ──────────────────────────────────────────────

    def _baseline_path(self) -> Path:
        return self.baseline_dir / f"{self.entity}.json"

    def _load_baseline(self) -> Optional[dict]:
        path = self._baseline_path()
        if not path.exists():
            return None
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)

    def _save_baseline(self, df: pd.DataFrame) -> None:
        self.baseline_dir.mkdir(parents=True, exist_ok=True)
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        col_stats = {}
        for col in numeric_cols:
            s = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(s):
                col_stats[col] = {
                    "mean": round(float(s.mean()), 6),
                    "std":  round(float(s.std()), 6),
                    "min":  round(float(s.min()), 6),
                    "max":  round(float(s.max()), 6),
                }
        baseline = {"row_count": len(df), "column_stats": col_stats}
        with self._baseline_path().open("w", encoding="utf-8") as fh:
            json.dump(baseline, fh, indent=2)
        logger.info("[ReasonabilityScreen] baseline saved → %s", self._baseline_path())
