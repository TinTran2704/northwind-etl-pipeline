"""Tests for src/clean/screens/reasonability_screen.py."""

import json

import pandas as pd
import pytest

from src.clean.screens.base_screen import Severity
from src.clean.screens.reasonability_screen import ReasonabilityRule, ReasonabilityScreen


def _screen(rules, tmp_path) -> ReasonabilityScreen:
    return ReasonabilityScreen(
        entity="customers", rules=rules, baseline_dir=tmp_path / "_baselines"
    )


def _row_count_rule(baseline=91, tol=20, severity="WARN") -> ReasonabilityRule:
    return ReasonabilityRule(
        metric="row_count", severity=severity,
        baseline=baseline, tolerance_pct=tol,
    )


class TestFirstRun:
    def test_first_run_returns_no_violations(self, tmp_path):
        df = pd.DataFrame({"ID": range(91)})
        screen = _screen([_row_count_rule()], tmp_path)
        assert screen.check(df) == []

    def test_first_run_creates_baseline_file(self, tmp_path):
        df = pd.DataFrame({"ID": range(91)})
        screen = _screen([_row_count_rule()], tmp_path)
        screen.check(df)
        baseline_path = tmp_path / "_baselines" / "customers.json"
        assert baseline_path.exists()

    def test_baseline_contains_row_count(self, tmp_path):
        df = pd.DataFrame({"ID": range(91)})
        screen = _screen([_row_count_rule()], tmp_path)
        screen.check(df)
        baseline = json.loads((tmp_path / "_baselines" / "customers.json").read_text())
        assert baseline["row_count"] == 91

    def test_baseline_contains_numeric_stats(self, tmp_path):
        df = pd.DataFrame({"ID": range(10), "Price": [float(i) for i in range(10)]})
        screen = _screen([_row_count_rule()], tmp_path)
        screen.check(df)
        baseline = json.loads((tmp_path / "_baselines" / "customers.json").read_text())
        assert "Price" in baseline["column_stats"]


class TestRowCountDrift:
    def _run_twice(self, df1, df2, rule, tmp_path):
        screen = _screen([rule], tmp_path)
        screen.check(df1)   # first run — creates baseline
        return screen.check(df2)   # second run — checks

    def test_large_row_count_drop_detected(self, tmp_path):
        df1 = pd.DataFrame({"ID": range(91)})
        df2 = pd.DataFrame({"ID": range(50)})  # 45% drop — exceeds 20%
        results = self._run_twice(df1, df2, _row_count_rule(tol=20), tmp_path)
        assert len(results) == 1
        assert results[0].severity == Severity.WARN
        assert results[0].screen_name == "reasonability.row_count"

    def test_small_row_count_drop_within_tolerance(self, tmp_path):
        df1 = pd.DataFrame({"ID": range(91)})
        df2 = pd.DataFrame({"ID": range(85)})  # ~6.5% drop — within 20%
        results = self._run_twice(df1, df2, _row_count_rule(tol=20), tmp_path)
        assert results == []

    def test_violation_message_contains_percentages(self, tmp_path):
        df1 = pd.DataFrame({"ID": range(100)})
        df2 = pd.DataFrame({"ID": range(50)})
        results = self._run_twice(df1, df2, _row_count_rule(baseline=100, tol=20), tmp_path)
        assert len(results) == 1
        assert "%" in results[0].message


class TestColumnMeanDrift:
    def test_mean_drift_detected(self, tmp_path):
        df1 = pd.DataFrame({"Freight": [10.0] * 10})
        df2 = pd.DataFrame({"Freight": [100.0] * 10})   # 900% drift
        rule = ReasonabilityRule(metric="column_mean", severity="WARN",
                                 tolerance_pct=20, column="Freight")
        screen = _screen([rule], tmp_path)
        screen.check(df1)
        results = screen.check(df2)
        assert len(results) == 1
        assert results[0].column_name == "Freight"

    def test_stable_mean_no_violation(self, tmp_path):
        df1 = pd.DataFrame({"Freight": [10.0, 12.0, 11.0]})
        df2 = pd.DataFrame({"Freight": [10.5, 11.5, 10.8]})  # ~5% drift
        rule = ReasonabilityRule(metric="column_mean", severity="WARN",
                                 tolerance_pct=20, column="Freight")
        screen = _screen([rule], tmp_path)
        screen.check(df1)
        results = screen.check(df2)
        assert results == []
