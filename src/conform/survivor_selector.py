"""
SurvivorSelector — Kimball Subsystem #8.

For each cluster produced by the Deduplicator, selects the best value for
every column according to survivorship rules defined in
config/survivorship_rules.yaml.

Supported rules:
  longest_non_null : pick the non-null string with the most characters.
  most_recent      : pick from the row whose ``_extract_ts`` is latest.
  prefer_source    : pick from the row whose ``_source`` matches the first
                     entry in ``priority`` list.
"""

import logging
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path("config/survivorship_rules.yaml")


class SurvivorSelectorError(Exception):
    """Raised when survivorship cannot be applied."""


class SurvivorSelector:
    """Select one golden record per cluster.

    Args:
        config_path: Path to survivorship_rules.yaml.
    """

    def __init__(self, config_path: Path = _DEFAULT_CONFIG_PATH) -> None:
        self._config_path = config_path
        self._rules: dict[str, dict[str, dict]] = {}
        self._default_rule: str = "longest_non_null"
        self._load_config()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def select(self, cluster_df: pd.DataFrame, entity: str) -> pd.DataFrame:
        """Return one golden record per cluster_id in *cluster_df*.

        *cluster_df* must contain a ``cluster_id`` column.  For single-member
        clusters the record is returned as-is.  For multi-member clusters,
        each column is resolved column-by-column using the configured rule.

        Args:
            cluster_df: DataFrame with ``cluster_id`` plus entity columns.
            entity:     Entity key (e.g. ``"customers"``).

        Returns:
            DataFrame with one row per cluster — the golden record.
        """
        if "cluster_id" not in cluster_df.columns:
            raise SurvivorSelectorError("cluster_df must contain a 'cluster_id' column")

        entity_rules = self._rules.get(entity, {})
        data_cols = [c for c in cluster_df.columns if c != "cluster_id"]
        golden_rows: list[dict[str, Any]] = []

        for cluster_id, group in cluster_df.groupby("cluster_id"):
            if len(group) == 1:
                row = group.iloc[0].to_dict()
                golden_rows.append(row)
                continue

            # Multi-member cluster — apply rules column by column
            golden: dict[str, Any] = {"cluster_id": cluster_id}
            for col in data_cols:
                rule_cfg = entity_rules.get(col, {})
                rule = rule_cfg.get("rule", self._default_rule)
                priority = rule_cfg.get("priority", [])
                golden[col] = self._apply_rule(group, col, rule, priority)

            golden_rows.append(golden)

        return pd.DataFrame(golden_rows, columns=cluster_df.columns)

    # ------------------------------------------------------------------
    # Rule implementations
    # ------------------------------------------------------------------

    def _apply_rule(
        self,
        group: pd.DataFrame,
        column: str,
        rule: str,
        priority: list[str],
    ) -> Any:
        if rule == "longest_non_null":
            return self._longest_non_null(group, column)
        if rule == "most_recent":
            return self._most_recent(group, column)
        if rule == "prefer_source":
            return self._prefer_source(group, column, priority)
        logger.warning("unknown survivorship rule %r for column %r — fallback to longest_non_null", rule, column)
        return self._longest_non_null(group, column)

    @staticmethod
    def _longest_non_null(group: pd.DataFrame, column: str) -> Any:
        non_null = group[column].dropna()
        if non_null.empty:
            return None
        # For strings pick longest; for non-strings pick first non-null.
        if non_null.dtype == object:
            return max(non_null.astype(str), key=len)
        return non_null.iloc[0]

    @staticmethod
    def _most_recent(group: pd.DataFrame, column: str) -> Any:
        ts_col = "_extract_ts"
        if ts_col in group.columns:
            idx = group[ts_col].idxmax()
            return group.loc[idx, column]
        # No timestamp column — fall back
        non_null = group[column].dropna()
        return non_null.iloc[0] if not non_null.empty else None

    @staticmethod
    def _prefer_source(group: pd.DataFrame, column: str, priority: list[str]) -> Any:
        src_col = "_source"
        if src_col in group.columns and priority:
            for src in priority:
                match = group[group[src_col] == src]
                if not match.empty:
                    val = match.iloc[0][column]
                    if pd.notna(val):
                        return val
        # Fallback
        non_null = group[column].dropna()
        return non_null.iloc[0] if not non_null.empty else None

    # ------------------------------------------------------------------
    # Config loading
    # ------------------------------------------------------------------

    def _load_config(self) -> None:
        if not self._config_path.exists():
            logger.warning("survivorship_rules.yaml not found at %s", self._config_path)
            return
        raw = yaml.safe_load(self._config_path.read_text(encoding="utf-8"))
        if raw:
            default_section = raw.pop("default", {})
            self._default_rule = (
                default_section.get("_default", {}).get("rule", "longest_non_null")
            )
            self._rules = {k: v for k, v in raw.items() if isinstance(v, dict)}
