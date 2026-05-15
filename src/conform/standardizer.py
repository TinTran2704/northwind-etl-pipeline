"""
Standardizer — Kimball Subsystem #21 (Data Integration Manager, part a).

Normalises free-text values (country names, phone numbers, casing) into a
conformed domain so all downstream dimensions share a single vocabulary.

Config files:
  config/standardization/country_aliases.yaml — name variants → ISO alpha-2
  config/standardization/entity_rules.yaml    — per-entity column rules
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Dial codes: ISO alpha-2 → ITU country calling code (str, no leading +).
_DIAL_CODES: dict[str, str] = {
    "AR": "54",  "AT": "43",  "BE": "32",  "BR": "55",
    "CA": "1",   "CH": "41",  "DE": "49",  "DK": "45",
    "ES": "34",  "FI": "358", "FR": "33",  "GB": "44",
    "IE": "353", "IT": "39",  "MX": "52",  "NL": "31",
    "NO": "47",  "PL": "48",  "PT": "351", "SE": "46",
    "US": "1",   "VE": "58",
}

_DEFAULT_CONFIG_DIR = Path("config")


class StandardizerError(Exception):
    """Raised when a standardization config or transform fails."""


class Standardizer:
    """Apply domain-conformance transforms to DataFrame columns.

    Args:
        config_dir: Directory containing the ``standardization/`` folder.
    """

    def __init__(self, config_dir: Path = _DEFAULT_CONFIG_DIR) -> None:
        self._config_dir = config_dir
        self._country_aliases: dict[str, str] = {}
        self._entity_rules: dict[str, list[dict]] = {}
        self._load_configs()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def standardize_country(self, value: Optional[str]) -> str:
        """Return ISO 3166-1 alpha-2 code for *value*, or original if unknown.

        Args:
            value: Country name variant (e.g. ``"USA"``, ``"UK"``).

        Returns:
            ISO alpha-2 code (e.g. ``"US"``, ``"GB"``) or *value* unchanged.
        """
        if not value or not isinstance(value, str):
            return value  # type: ignore[return-value]
        stripped = value.strip()
        code = self._country_aliases.get(stripped)
        if code:
            return code
        logger.debug("standardize_country: no mapping for %r", stripped)
        return stripped

    def standardize_phone(self, value: Optional[str], country_code: Optional[str]) -> str:
        """Format *value* as ``+{dial_code}-{digits}``.

        Strips all non-digit characters from *value* first.  If *country_code*
        is unknown, returns the digit-only string without a prefix.

        Args:
            value:        Raw phone string.
            country_code: ISO alpha-2 code used to look up the dial code.

        Returns:
            Formatted phone string or original if value is falsy.
        """
        if not value or not isinstance(value, str):
            return value  # type: ignore[return-value]
        digits = re.sub(r"[^\d]", "", value)
        if not digits:
            return value
        dial = _DIAL_CODES.get(country_code or "")
        if dial:
            return f"+{dial}-{digits}"
        return digits

    def title_case(self, value: Optional[str]) -> str:
        """Return *value* in Title Case.

        Args:
            value: Free-text string.

        Returns:
            Title-cased string, or original if not a string.
        """
        if not value or not isinstance(value, str):
            return value  # type: ignore[return-value]
        return value.strip().title()

    def standardize_df(self, df: pd.DataFrame, entity_name: str) -> pd.DataFrame:
        """Apply all configured transforms for *entity_name* to *df*.

        Transforms are applied in the order defined in entity_rules.yaml.
        The input DataFrame is not modified in-place; a copy is returned.

        Args:
            df:          Input DataFrame (cleaned staging data).
            entity_name: Entity key from entity_rules.yaml (e.g. ``"customers"``).

        Returns:
            New DataFrame with standardized columns.
        """
        rules = self._entity_rules.get(entity_name, [])
        if not rules:
            logger.debug("standardize_df: no rules for entity %r", entity_name)
            return df.copy()

        out = df.copy()
        for rule in rules:
            col = rule.get("column", "")
            transform = rule.get("transform", "")
            out_col = rule.get("output_column", col)

            if col not in out.columns:
                logger.debug("standardize_df: column %r not in df — skipping", col)
                continue

            if transform == "standardize_country":
                out[out_col] = out[col].apply(self.standardize_country)
                logger.debug("standardize_df: %r → %r via standardize_country", col, out_col)

            elif transform == "title_case":
                out[out_col] = out[col].apply(self.title_case)

            elif transform == "standardize_phone":
                cc_col = rule.get("country_column", "country_code")
                if cc_col in out.columns:
                    out[out_col] = out.apply(
                        lambda row, c=col, cc=cc_col: self.standardize_phone(
                            row.get(c), row.get(cc)
                        ),
                        axis=1,
                    )
                else:
                    out[out_col] = out[col].apply(
                        lambda v: self.standardize_phone(v, None)
                    )

            else:
                logger.warning("standardize_df: unknown transform %r for %r", transform, col)

        return out

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_configs(self) -> None:
        std_dir = self._config_dir / "standardization"
        self._country_aliases = self._load_country_aliases(std_dir)
        self._entity_rules = self._load_entity_rules(std_dir)

    def _load_country_aliases(self, std_dir: Path) -> dict[str, str]:
        path = std_dir / "country_aliases.yaml"
        if not path.exists():
            logger.warning("country_aliases.yaml not found at %s", path)
            return {}
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        # str() guards against YAML boolean coercion (e.g. NO → False, YES → True).
        return {str(k): str(v) for k, v in raw.get("country_aliases", {}).items()}

    def _load_entity_rules(self, std_dir: Path) -> dict[str, list[dict]]:
        path = std_dir / "entity_rules.yaml"
        if not path.exists():
            logger.warning("entity_rules.yaml not found at %s", path)
            return {}
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        entities = raw.get("entities", {})
        return {k: (v or []) for k, v in entities.items()}

    def get_country_info(self) -> dict[str, dict]:
        """Return the full country_info section from country_aliases.yaml.

        Returns:
            Dict mapping ISO alpha-2 → {country_name, region, subregion, primary_currency}.
        """
        path = self._config_dir / "standardization" / "country_aliases.yaml"
        if not path.exists():
            return {}
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        return {str(k): v for k, v in raw.get("country_info", {}).items()}
