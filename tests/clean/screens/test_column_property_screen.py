"""Tests for src/clean/screens/column_property_screen.py."""

import pandas as pd
import pytest
import yaml

from src.clean.screens.base_screen import Severity
from src.clean.screens.column_property_screen import ColumnPropertyScreen, ColumnRule


def _screen(*rules: ColumnRule, pk: str = "ID") -> ColumnPropertyScreen:
    return ColumnPropertyScreen(entity="test", rules=list(rules), pk_column=pk)


def _rule(**kwargs) -> ColumnRule:
    defaults = {"column": "Col", "rule": "not_null", "severity": "ERROR"}
    defaults.update(kwargs)
    return ColumnRule(**defaults)


class TestNotNull:
    def test_null_pk_is_detected(self):
        df = pd.DataFrame({"ID": [None, "A2"], "Name": ["X", "Y"]})
        screen = _screen(_rule(column="ID", rule="not_null", severity="FATAL"))
        results = screen.check(df)
        assert len(results) == 1
        assert results[0].severity == Severity.FATAL

    def test_no_violations_when_all_filled(self):
        df = pd.DataFrame({"ID": ["A1", "A2"]})
        screen = _screen(_rule(column="ID", rule="not_null", severity="FATAL"))
        assert screen.check(df) == []

    def test_missing_column_returns_empty(self):
        df = pd.DataFrame({"Other": [1]})
        screen = _screen(_rule(column="ID", rule="not_null", severity="ERROR"))
        assert screen.check(df) == []

    def test_record_id_from_pk_column(self):
        df = pd.DataFrame({"ID": ["A1"], "Name": [None]})
        screen = _screen(_rule(column="Name", rule="not_null", severity="WARN"), pk="ID")
        results = screen.check(df)
        assert results[0].record_id == "A1"


class TestUnique:
    def test_duplicate_detected(self):
        df = pd.DataFrame({"ID": ["A1", "A1", "A2"]})
        screen = _screen(_rule(column="ID", rule="unique", severity="FATAL"))
        results = screen.check(df)
        assert len(results) == 2  # both duplicate rows flagged
        assert all(r.severity == Severity.FATAL for r in results)

    def test_all_unique_returns_empty(self):
        df = pd.DataFrame({"ID": ["A1", "A2", "A3"]})
        screen = _screen(_rule(column="ID", rule="unique", severity="FATAL"))
        assert screen.check(df) == []


class TestMaxLength:
    def test_over_limit_detected(self):
        df = pd.DataFrame({"Code": ["ABCDEF"]})  # 6 chars, limit 5
        screen = _screen(_rule(column="Code", rule="max_length", value=5, severity="ERROR"))
        assert len(screen.check(df)) == 1

    def test_within_limit_clean(self):
        df = pd.DataFrame({"Code": ["ABCDE"]})
        screen = _screen(_rule(column="Code", rule="max_length", value=5, severity="ERROR"))
        assert screen.check(df) == []


class TestMinLength:
    def test_below_min_detected(self):
        df = pd.DataFrame({"Code": ["AB"]})  # 2 chars, min 3
        screen = _screen(_rule(column="Code", rule="min_length", value=3, severity="WARN"))
        assert len(screen.check(df)) == 1

    def test_at_min_is_clean(self):
        df = pd.DataFrame({"Code": ["ABC"]})
        screen = _screen(_rule(column="Code", rule="min_length", value=3, severity="WARN"))
        assert screen.check(df) == []


class TestRegex:
    def test_invalid_postal_code_detected(self):
        df = pd.DataFrame({"PostalCode": ["12!34", "56789"]})
        screen = _screen(_rule(
            column="PostalCode", rule="regex",
            pattern=r"^[A-Za-z0-9 \-]+$", severity="WARN",
        ))
        results = screen.check(df)
        assert len(results) == 1
        assert results[0].actual == "12!34"

    def test_valid_postal_codes_clean(self):
        df = pd.DataFrame({"PostalCode": ["12345", "SW1A 1AA", "D-12345"]})
        screen = _screen(_rule(
            column="PostalCode", rule="regex",
            pattern=r"^[A-Za-z0-9 \-]+$", severity="WARN",
        ))
        assert screen.check(df) == []


class TestNumericRange:
    def test_below_min_detected(self):
        df = pd.DataFrame({"Price": [-1.0, 5.0]})
        screen = _screen(_rule(column="Price", rule="numeric_range", min=0, severity="ERROR"))
        assert len(screen.check(df)) == 1

    def test_above_max_detected(self):
        df = pd.DataFrame({"Qty": [1001]})
        screen = _screen(_rule(column="Qty", rule="numeric_range", min=1, max=1000, severity="ERROR"))
        assert len(screen.check(df)) == 1

    def test_in_range_clean(self):
        df = pd.DataFrame({"Qty": [1, 500, 1000]})
        screen = _screen(_rule(column="Qty", rule="numeric_range", min=1, max=1000, severity="ERROR"))
        assert screen.check(df) == []

    def test_non_numeric_values_skipped(self):
        df = pd.DataFrame({"Price": ["N/A", "5.0"]})
        screen = _screen(_rule(column="Price", rule="numeric_range", min=0, severity="WARN"))
        assert screen.check(df) == []


class TestDateRange:
    def test_future_date_detected(self):
        df = pd.DataFrame({"OrderDate": ["2099-01-01"]})
        screen = _screen(_rule(
            column="OrderDate", rule="date_range",
            min="1990-01-01", max="today", severity="ERROR",
        ))
        assert len(screen.check(df)) == 1

    def test_valid_date_clean(self):
        df = pd.DataFrame({"OrderDate": ["2020-06-15"]})
        screen = _screen(_rule(
            column="OrderDate", rule="date_range",
            min="1990-01-01", max="today", severity="ERROR",
        ))
        assert screen.check(df) == []


class TestInList:
    def test_value_not_in_list_detected(self):
        df = pd.DataFrame({"Country": ["Neverland", "France"]})
        screen = _screen(_rule(
            column="Country", rule="in_list",
            items=["France", "Germany"], severity="WARN",
        ))
        results = screen.check(df)
        assert len(results) == 1
        assert results[0].actual == "Neverland"

    def test_value_in_list_clean(self):
        df = pd.DataFrame({"Country": ["France", "Germany"]})
        screen = _screen(_rule(
            column="Country", rule="in_list",
            items=["France", "Germany"], severity="WARN",
        ))
        assert screen.check(df) == []

    def test_source_without_items_skipped(self):
        df = pd.DataFrame({"Country": ["Anywhere"]})
        screen = _screen(_rule(
            column="Country", rule="in_list",
            source="dim_geography.country_name", severity="WARN",
        ))
        assert screen.check(df) == []


class TestFromConfig:
    def test_loads_rules_from_yaml(self, tmp_path):
        config = {
            "screens": {
                "customers": {
                    "column_property": [
                        {"column": "CustomerID", "rule": "not_null", "severity": "FATAL"}
                    ]
                }
            }
        }
        cfg_file = tmp_path / "quality_rules.yaml"
        cfg_file.write_text(yaml.dump(config))
        screen = ColumnPropertyScreen.from_config("customers", config_path=cfg_file)
        assert len(screen.rules) == 1
        assert screen.rules[0].rule == "not_null"

    def test_missing_entity_returns_zero_rules(self, tmp_path):
        cfg_file = tmp_path / "quality_rules.yaml"
        cfg_file.write_text(yaml.dump({"screens": {}}))
        screen = ColumnPropertyScreen.from_config("unknown_entity", config_path=cfg_file)
        assert screen.rules == []

    def test_missing_config_file_returns_zero_rules(self, tmp_path):
        screen = ColumnPropertyScreen.from_config(
            "customers", config_path=tmp_path / "nonexistent.yaml"
        )
        assert screen.rules == []


class TestNoSideEffects:
    def test_does_not_modify_input_df(self):
        df = pd.DataFrame({"ID": [None, "A2"]})
        original_shape = df.shape
        screen = _screen(_rule(column="ID", rule="not_null", severity="ERROR"))
        screen.check(df)
        assert df.shape == original_shape
        assert df["ID"].iloc[0] is None
