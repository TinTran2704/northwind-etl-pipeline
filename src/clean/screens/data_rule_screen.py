"""
Data & Value Rule Screen — Kimball Subsystem #4.

Validates cross-column business rules. Currently implements:
  - postal_country_consistency: PostalCode format must match Country pattern.

See docs/06-clean-phase.md §6.3 level-3.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import yaml

from src.clean.screens.base_screen import BaseScreen, ScreenResult, Severity

logger = logging.getLogger(__name__)

# Known postal-code patterns keyed by country name as it appears in Northwind.
_POSTAL_PATTERNS: dict[str, str] = {
    "USA":            r"^\d{5}(-\d{4})?$",
    "United States":  r"^\d{5}(-\d{4})?$",
    "UK":             r"^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$",
    "United Kingdom": r"^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$",
    "Germany":        r"^\d{5}$",
    "France":         r"^\d{5}$",
    "Italy":          r"^\d{5}$",
    "Spain":          r"^\d{5}$",
    "Switzerland":    r"^\d{4}$",
    "Austria":        r"^\d{4}$",
    "Belgium":        r"^\d{4}$",
    "Netherlands":    r"^\d{4}\s?[A-Z]{2}$",
    "Sweden":         r"^\d{5}$",
    "Norway":         r"^\d{4}$",
    "Denmark":        r"^\d{4}$",
    "Finland":        r"^\d{5}$",
    "Canada":         r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$",
    "Brazil":         r"^\d{5}-?\d{3}$",
    "Argentina":      r"^\d{4}$",
    "Mexico":         r"^\d{5}$",
    "Portugal":       r"^\d{4}-\d{3}$",
    "Ireland":        r"^[A-Z]\d{2}\s?[A-Z\d]{4}$",
    "Poland":         r"^\d{2}-\d{3}$",
    "Hungary":        r"^\d{4}$",
    "Venezuela":      r"^\d{4}$",
}


@dataclass
class DataRule:
    """A single cross-column business rule from config YAML."""

    name: str
    severity: str
    postal_column: str = "PostalCode"
    country_column: str = "Country"


class DataRuleScreen(BaseScreen):
    """Validate cross-column business rules for an entity.

    Args:
        entity:    Entity name.
        rules:     List of DataRule configs.
        pk_column: Column used for record_id in results.
    """

    name = "data_rule"

    def __init__(
        self,
        entity: str,
        rules: list[DataRule],
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
    ) -> "DataRuleScreen":
        """Build screen from quality_rules.yaml."""
        try:
            with config_path.open(encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh) or {}
        except FileNotFoundError:
            cfg = {}

        raw = cfg.get("screens", {}).get(entity_name, {}).get("data_rule", [])
        rules = [
            DataRule(
                name=r["name"],
                severity=r.get("severity", "WARN"),
                postal_column=r.get("postal_column", "PostalCode"),
                country_column=r.get("country_column", "Country"),
            )
            for r in (raw or [])
        ]
        return cls(entity=entity_name, rules=rules, pk_column=pk_column)

    def check(self, df: pd.DataFrame) -> List[ScreenResult]:
        """Apply all data rules; return violations."""
        results: list[ScreenResult] = []
        for rule in self.rules:
            if rule.name == "postal_country_consistency":
                results.extend(self._check_postal_country(df, rule))
            else:
                logger.warning("[DataRuleScreen] Unknown rule '%s'", rule.name)
        return results

    # ── Rule implementations ──────────────────────────────────────────────

    def _check_postal_country(
        self, df: pd.DataFrame, rule: DataRule
    ) -> list[ScreenResult]:
        """Flag rows where PostalCode format doesn't match Country pattern."""
        postal_col = rule.postal_column
        country_col = rule.country_column

        if postal_col not in df.columns or country_col not in df.columns:
            return []

        results: list[ScreenResult] = []
        for idx, row in df.iterrows():
            country = str(row[country_col]) if pd.notna(row[country_col]) else None
            postal = str(row[postal_col]) if pd.notna(row[postal_col]) else None

            if country is None or postal is None:
                continue
            pattern = _POSTAL_PATTERNS.get(country)
            if pattern is None:
                continue  # Unknown country — skip, don't assume invalid.
            if not re.match(pattern, postal.strip()):
                results.append(ScreenResult(
                    screen_name="data_rule.postal_country_consistency",
                    severity=Severity[rule.severity],
                    record_id=self._record_id(df, idx),
                    column_name=postal_col,
                    expected=f"format for {country}: /{pattern}/",
                    actual=postal,
                    message=(
                        f"[{self.entity}] PostalCode='{postal}' does not match "
                        f"expected format for Country='{country}'"
                    ),
                ))
        return results

    def _record_id(self, df: pd.DataFrame, idx: Any) -> Any:
        if self.pk_column and self.pk_column in df.columns:
            val = df.at[idx, self.pk_column]
            return None if pd.isna(val) else val
        return idx
