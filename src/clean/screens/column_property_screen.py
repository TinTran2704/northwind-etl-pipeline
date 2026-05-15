"""
Column Property Screen — Kimball Subsystem #4.

Validates per-column constraints loaded from config/quality_rules.yaml:
not_null, unique, max_length, min_length, regex, numeric_range,
date_range, in_list.
See docs/06-clean-phase.md §6.3 level-1.
"""

import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, List, Optional

import pandas as pd
import yaml

from src.clean.screens.base_screen import BaseScreen, ScreenResult, Severity

logger = logging.getLogger(__name__)


@dataclass
class ColumnRule:
    """A single column-level quality rule from config YAML."""

    column: str
    rule: str
    severity: str
    value: Optional[Any] = None       # max_length / min_length threshold
    pattern: Optional[str] = None     # regex pattern
    min: Optional[Any] = None         # numeric_range / date_range lower bound
    max: Optional[Any] = None         # numeric_range / date_range upper bound
    source: Optional[str] = None      # in_list: DB lookup reference (future)
    items: Optional[list] = None      # in_list: static allowed values


class ColumnPropertyScreen(BaseScreen):
    """Validate per-column constraints against a list of ColumnRule objects.

    Args:
        entity:    Entity name, e.g. ``"customers"``.
        rules:     Configured rules to apply.
        pk_column: Column used for record_id in ScreenResult.
    """

    name = "column_property"

    def __init__(
        self,
        entity: str,
        rules: list[ColumnRule],
        pk_column: Optional[str] = None,
    ) -> None:
        self.entity = entity
        self.rules = rules
        self.pk_column = pk_column

    @classmethod
    def from_config(
        cls,
        entity_name: str,
        config_path: Path = Path("config/quality_rules.yaml"),
        pk_column: Optional[str] = None,
    ) -> "ColumnPropertyScreen":
        """Build a screen from quality_rules.yaml.

        Returns a screen with zero rules if the entity has no entry.
        """
        try:
            with config_path.open(encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            logger.warning("quality_rules.yaml not found at %s", config_path)
            cfg = {}

        raw = cfg.get("screens", {}).get(entity_name, {}).get("column_property", [])
        rules = [
            ColumnRule(
                column=r["column"],
                rule=r["rule"],
                severity=r.get("severity", "WARN"),
                value=r.get("value"),
                pattern=r.get("pattern"),
                min=r.get("min"),
                max=r.get("max"),
                source=r.get("source"),
                items=r.get("items"),
            )
            for r in (raw or [])
        ]
        return cls(entity=entity_name, rules=rules, pk_column=pk_column)

    def check(self, df: pd.DataFrame) -> List[ScreenResult]:
        """Apply all configured rules; return collected violations."""
        results: list[ScreenResult] = []
        for rule in self.rules:
            handler: Optional[Callable] = _RULE_HANDLERS.get(rule.rule)
            if handler is None:
                logger.warning("[ColumnPropertyScreen] Unknown rule '%s'", rule.rule)
                continue
            results.extend(handler(self, df, rule))
        return results

    # ── Rule implementations ──────────────────────────────────────────────

    def _check_not_null(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns:
            return []
        mask = df[rule.column].isna()
        return [
            self._make(df, i, rule, "not_null", "NOT NULL", "NULL",
                       f"[{self.entity}] {rule.column} must not be null")
            for i in df.index[mask]
        ]

    def _check_unique(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns:
            return []
        dup_mask = df[rule.column].duplicated(keep=False)
        return [
            self._make(df, i, rule, "unique", "UNIQUE", str(df.at[i, rule.column]),
                       f"[{self.entity}] {rule.column} duplicate: '{df.at[i, rule.column]}'")
            for i in df.index[dup_mask]
        ]

    def _check_max_length(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns or rule.value is None:
            return []
        col = df[rule.column].dropna().astype(str)
        mask = col.str.len() > int(rule.value)
        return [
            self._make(df, i, rule, "max_length",
                       f"len <= {rule.value}", str(len(col[i])),
                       f"[{self.entity}] {rule.column} length {len(col[i])} exceeds {rule.value}")
            for i in col.index[mask]
        ]

    def _check_min_length(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns or rule.value is None:
            return []
        col = df[rule.column].dropna().astype(str)
        mask = col.str.len() < int(rule.value)
        return [
            self._make(df, i, rule, "min_length",
                       f"len >= {rule.value}", str(len(col[i])),
                       f"[{self.entity}] {rule.column} length {len(col[i])} below {rule.value}")
            for i in col.index[mask]
        ]

    def _check_regex(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns or not rule.pattern:
            return []
        col = df[rule.column].dropna().astype(str)
        mask = ~col.str.match(rule.pattern)
        return [
            self._make(df, i, rule, "regex",
                       f"matches /{rule.pattern}/", col[i],
                       f"[{self.entity}] {rule.column} does not match pattern")
            for i in col.index[mask]
        ]

    def _check_numeric_range(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns:
            return []
        numeric = pd.to_numeric(df[rule.column], errors="coerce").dropna()
        results: list[ScreenResult] = []
        if rule.min is not None:
            for i in numeric.index[numeric < float(rule.min)]:
                results.append(self._make(
                    df, i, rule, "numeric_range",
                    f">= {rule.min}", str(numeric[i]),
                    f"[{self.entity}] {rule.column}={numeric[i]} below min {rule.min}",
                ))
        if rule.max is not None:
            for i in numeric.index[numeric > float(rule.max)]:
                results.append(self._make(
                    df, i, rule, "numeric_range",
                    f"<= {rule.max}", str(numeric[i]),
                    f"[{self.entity}] {rule.column}={numeric[i]} exceeds max {rule.max}",
                ))
        return results

    def _check_date_range(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns:
            return []
        dates = pd.to_datetime(df[rule.column], errors="coerce").dt.date.dropna()
        results: list[ScreenResult] = []

        def _parse(val: Any) -> date:
            return date.today() if str(val) == "today" else date.fromisoformat(str(val))

        if rule.min is not None:
            lo = _parse(rule.min)
            for i in dates.index[dates < lo]:
                results.append(self._make(
                    df, i, rule, "date_range", f">= {lo}", str(dates[i]),
                    f"[{self.entity}] {rule.column}={dates[i]} before {lo}",
                ))
        if rule.max is not None:
            hi = _parse(rule.max)
            for i in dates.index[dates > hi]:
                results.append(self._make(
                    df, i, rule, "date_range", f"<= {hi}", str(dates[i]),
                    f"[{self.entity}] {rule.column}={dates[i]} after {hi}",
                ))
        return results

    def _check_in_list(self, df: pd.DataFrame, rule: ColumnRule) -> list[ScreenResult]:
        if rule.column not in df.columns:
            return []
        if rule.items is None:
            if rule.source:
                logger.debug("in_list source='%s' not yet implemented — skipping", rule.source)
            return []
        allowed = {str(v) for v in rule.items}
        col = df[rule.column].dropna().astype(str)
        mask = ~col.isin(allowed)
        return [
            self._make(df, i, rule, "in_list",
                       f"one of {sorted(allowed)}", col[i],
                       f"[{self.entity}] {rule.column}='{col[i]}' not in allowed list")
            for i in col.index[mask]
        ]

    # ── Helpers ───────────────────────────────────────────────────────────

    def _record_id(self, df: pd.DataFrame, idx: Any) -> Any:
        if self.pk_column and self.pk_column in df.columns:
            val = df.at[idx, self.pk_column]
            return None if pd.isna(val) else val
        return idx

    def _make(
        self,
        df: pd.DataFrame,
        idx: Any,
        rule: ColumnRule,
        sub: str,
        expected: str,
        actual: str,
        msg: str,
    ) -> ScreenResult:
        return ScreenResult(
            screen_name=f"column_property.{sub}",
            severity=Severity[rule.severity],
            record_id=self._record_id(df, idx),
            column_name=rule.column,
            expected=expected,
            actual=actual,
            message=msg,
        )


_RULE_HANDLERS: dict[str, Callable] = {
    "not_null":      ColumnPropertyScreen._check_not_null,
    "unique":        ColumnPropertyScreen._check_unique,
    "max_length":    ColumnPropertyScreen._check_max_length,
    "min_length":    ColumnPropertyScreen._check_min_length,
    "regex":         ColumnPropertyScreen._check_regex,
    "numeric_range": ColumnPropertyScreen._check_numeric_range,
    "date_range":    ColumnPropertyScreen._check_date_range,
    "in_list":       ColumnPropertyScreen._check_in_list,
}
