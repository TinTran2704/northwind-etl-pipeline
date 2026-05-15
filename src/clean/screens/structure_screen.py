"""
Structure Screen — Kimball Subsystem #4.

Checks referential integrity: values in a FK column must exist in the
referenced table's column.
Example: orders.CustomerID must be in customers.CustomerID.
See docs/06-clean-phase.md §6.3 level-2.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import yaml

from src.clean.screens.base_screen import BaseScreen, ScreenResult, Severity

logger = logging.getLogger(__name__)


@dataclass
class StructureRule:
    """A single referential-integrity rule."""

    column: str           # FK column in the entity being checked
    references: str       # "entity.column" that holds valid values
    severity: str


class StructureScreen(BaseScreen):
    """Validate FK columns against reference DataFrames.

    Args:
        entity:        Entity being validated, e.g. ``"orders"``.
        rules:         List of StructureRule.
        reference_dfs: Dict keyed by entity name → full DataFrame.
        pk_column:     PK column for record_id in results.
    """

    name = "structure"

    def __init__(
        self,
        entity: str,
        rules: list[StructureRule],
        reference_dfs: dict[str, pd.DataFrame],
        pk_column: Optional[str] = None,
    ) -> None:
        self.entity = entity
        self.rules = rules
        self.reference_dfs = reference_dfs
        self.pk_column = pk_column

    @classmethod
    def from_config(
        cls,
        entity_name: str,
        reference_dfs: dict[str, pd.DataFrame],
        config_path: Path = Path("config/quality_rules.yaml"),
        pk_column: Optional[str] = None,
    ) -> "StructureScreen":
        """Build screen from quality_rules.yaml for *entity_name*."""
        try:
            with config_path.open(encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            cfg = {}

        raw = cfg.get("screens", {}).get(entity_name, {}).get("structure", [])
        rules = [
            StructureRule(
                column=r["column"],
                references=r["references"],
                severity=r.get("severity", "ERROR"),
            )
            for r in (raw or [])
        ]
        return cls(entity=entity_name, rules=rules,
                   reference_dfs=reference_dfs, pk_column=pk_column)

    def check(self, df: pd.DataFrame) -> List[ScreenResult]:
        """Find FK values that have no matching record in the reference table."""
        results: list[ScreenResult] = []
        for rule in self.rules:
            parts = rule.references.split(".", 1)
            if len(parts) != 2:
                logger.warning("Invalid references format '%s'", rule.references)
                continue
            ref_entity, ref_column = parts
            ref_df = self.reference_dfs.get(ref_entity)
            if ref_df is None:
                logger.warning("[StructureScreen] reference entity '%s' not loaded", ref_entity)
                continue
            if ref_column not in ref_df.columns:
                logger.warning("[StructureScreen] reference column '%s' not in '%s'",
                               ref_column, ref_entity)
                continue
            if rule.column not in df.columns:
                continue

            valid = set(ref_df[ref_column].dropna().astype(str))
            fk_col = df[rule.column].dropna().astype(str)
            orphans = fk_col[~fk_col.isin(valid)]

            for i in orphans.index:
                results.append(ScreenResult(
                    screen_name="structure.referential_integrity",
                    severity=Severity[rule.severity],
                    record_id=self._record_id(df, i),
                    column_name=rule.column,
                    expected=f"value in {rule.references}",
                    actual=str(orphans[i]),
                    message=(
                        f"[{self.entity}] {rule.column}='{orphans[i]}' "
                        f"not found in {rule.references}"
                    ),
                ))
        return results

    def _record_id(self, df: pd.DataFrame, idx: Any) -> Any:
        if self.pk_column and self.pk_column in df.columns:
            val = df.at[idx, self.pk_column]
            return None if pd.isna(val) else val
        return idx
