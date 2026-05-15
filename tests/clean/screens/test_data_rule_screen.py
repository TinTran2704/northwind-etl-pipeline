"""Tests for src/clean/screens/data_rule_screen.py."""

import pandas as pd
import pytest
import yaml

from src.clean.screens.base_screen import Severity
from src.clean.screens.data_rule_screen import DataRule, DataRuleScreen


def _screen(rules=None, pk="CustomerID") -> DataRuleScreen:
    return DataRuleScreen(
        entity="customers",
        rules=rules or [],
        pk_column=pk,
    )


def _postal_rule(severity="WARN") -> DataRule:
    return DataRule(
        name="postal_country_consistency",
        severity=severity,
        postal_column="PostalCode",
        country_column="Country",
    )


class TestPostalCountryConsistency:
    def test_valid_german_postal_code_passes(self):
        df = pd.DataFrame({
            "CustomerID": ["ALFKI"],
            "PostalCode": ["12209"],
            "Country": ["Germany"],
        })
        screen = _screen([_postal_rule()])
        assert screen.check(df) == []

    def test_invalid_german_postal_code_detected(self):
        df = pd.DataFrame({
            "CustomerID": ["ALFKI"],
            "PostalCode": ["1234"],   # Germany needs 5 digits
            "Country": ["Germany"],
        })
        screen = _screen([_postal_rule()])
        results = screen.check(df)
        assert len(results) == 1
        assert results[0].severity == Severity.WARN
        assert results[0].record_id == "ALFKI"
        assert "Germany" in results[0].expected

    def test_valid_us_postal_code_passes(self):
        df = pd.DataFrame({"CustomerID": ["C1"], "PostalCode": ["12345"], "Country": ["USA"]})
        assert _screen([_postal_rule()]).check(df) == []

    def test_invalid_us_postal_code_detected(self):
        df = pd.DataFrame({"CustomerID": ["C1"], "PostalCode": ["ABCDE"], "Country": ["USA"]})
        results = _screen([_postal_rule()]).check(df)
        assert len(results) == 1

    def test_unknown_country_is_skipped(self):
        df = pd.DataFrame({
            "CustomerID": ["C1"],
            "PostalCode": ["whatever-123"],
            "Country": ["Freedonia"],  # not in patterns
        })
        assert _screen([_postal_rule()]).check(df) == []

    def test_null_postal_code_is_skipped(self):
        df = pd.DataFrame({
            "CustomerID": ["C1"],
            "PostalCode": [None],
            "Country": ["Germany"],
        })
        assert _screen([_postal_rule()]).check(df) == []

    def test_null_country_is_skipped(self):
        df = pd.DataFrame({
            "CustomerID": ["C1"],
            "PostalCode": ["12345"],
            "Country": [None],
        })
        assert _screen([_postal_rule()]).check(df) == []

    def test_missing_postal_column_returns_empty(self):
        df = pd.DataFrame({"CustomerID": ["C1"], "Country": ["Germany"]})
        assert _screen([_postal_rule()]).check(df) == []

    def test_does_not_modify_input_df(self):
        df = pd.DataFrame({
            "CustomerID": ["C1"],
            "PostalCode": ["WRONG"],
            "Country": ["Germany"],
        })
        cols_before = list(df.columns)
        _screen([_postal_rule()]).check(df)
        assert list(df.columns) == cols_before

    def test_multiple_violations_all_returned(self):
        df = pd.DataFrame({
            "CustomerID": ["C1", "C2"],
            "PostalCode": ["WRONG1", "WRONG2"],
            "Country": ["Germany", "France"],
        })
        results = _screen([_postal_rule()]).check(df)
        assert len(results) == 2


class TestFromConfig:
    def test_loads_data_rules_from_yaml(self, tmp_path):
        config = {
            "screens": {
                "customers": {
                    "data_rule": [
                        {"name": "postal_country_consistency", "severity": "WARN"}
                    ]
                }
            }
        }
        cfg = tmp_path / "quality_rules.yaml"
        cfg.write_text(yaml.dump(config))
        screen = DataRuleScreen.from_config("customers", config_path=cfg)
        assert len(screen.rules) == 1

    def test_unknown_rule_name_still_loads(self, tmp_path):
        config = {
            "screens": {"test": {"data_rule": [{"name": "unknown_rule", "severity": "WARN"}]}}
        }
        cfg = tmp_path / "quality_rules.yaml"
        cfg.write_text(yaml.dump(config))
        screen = DataRuleScreen.from_config("test", config_path=cfg)
        assert screen.check(pd.DataFrame({"A": [1]})) == []
