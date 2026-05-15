"""Tests for src/clean/screens/structure_screen.py."""

import pandas as pd
import pytest
import yaml

from src.clean.screens.base_screen import Severity
from src.clean.screens.structure_screen import StructureRule, StructureScreen

_CUSTOMERS = pd.DataFrame({"CustomerID": ["ALFKI", "ANATR", "BERGS"]})
_EMPLOYEES = pd.DataFrame({"EmployeeID": [1, 2, 3]})


def _screen(entity: str, rules, refs, pk=None) -> StructureScreen:
    return StructureScreen(entity=entity, rules=rules,
                           reference_dfs=refs, pk_column=pk)


class TestReferentialIntegrity:
    def test_orphan_customer_id_detected(self):
        orders = pd.DataFrame({
            "OrderID": [10248, 10249],
            "CustomerID": ["ALFKI", "GHOST"],  # GHOST not in customers
        })
        screen = _screen(
            "orders",
            [StructureRule(column="CustomerID", references="customers.CustomerID", severity="ERROR")],
            {"customers": _CUSTOMERS},
            pk="OrderID",
        )
        results = screen.check(orders)
        assert len(results) == 1
        assert results[0].actual == "GHOST"
        assert results[0].severity == Severity.ERROR
        assert results[0].record_id == 10249

    def test_all_valid_fks_no_violations(self):
        orders = pd.DataFrame({
            "OrderID": [10248],
            "CustomerID": ["ALFKI"],
        })
        screen = _screen(
            "orders",
            [StructureRule(column="CustomerID", references="customers.CustomerID", severity="ERROR")],
            {"customers": _CUSTOMERS},
            pk="OrderID",
        )
        assert screen.check(orders) == []

    def test_null_fk_values_are_skipped(self):
        orders = pd.DataFrame({
            "OrderID": [10248],
            "CustomerID": [None],
        })
        screen = _screen(
            "orders",
            [StructureRule(column="CustomerID", references="customers.CustomerID", severity="ERROR")],
            {"customers": _CUSTOMERS},
            pk="OrderID",
        )
        assert screen.check(orders) == []

    def test_missing_reference_entity_logs_warning_and_returns_empty(self):
        orders = pd.DataFrame({"OrderID": [1], "CustomerID": ["X"]})
        screen = _screen(
            "orders",
            [StructureRule(column="CustomerID", references="no_such_entity.ID", severity="ERROR")],
            {},
        )
        assert screen.check(orders) == []

    def test_multiple_rules_checked_independently(self):
        orders = pd.DataFrame({
            "OrderID": [10248],
            "CustomerID": ["GHOST"],   # orphan
            "EmployeeID": [999],       # orphan
        })
        screen = _screen(
            "orders",
            [
                StructureRule(column="CustomerID", references="customers.CustomerID", severity="ERROR"),
                StructureRule(column="EmployeeID", references="employees.EmployeeID", severity="ERROR"),
            ],
            {"customers": _CUSTOMERS, "employees": _EMPLOYEES},
            pk="OrderID",
        )
        results = screen.check(orders)
        assert len(results) == 2


class TestFromConfig:
    def test_loads_structure_rules_from_yaml(self, tmp_path):
        config = {
            "screens": {
                "orders": {
                    "structure": [
                        {"column": "CustomerID", "references": "customers.CustomerID",
                         "severity": "ERROR"}
                    ]
                }
            }
        }
        cfg = tmp_path / "quality_rules.yaml"
        cfg.write_text(yaml.dump(config))
        screen = StructureScreen.from_config(
            "orders", reference_dfs={"customers": _CUSTOMERS}, config_path=cfg
        )
        assert len(screen.rules) == 1

    def test_no_structure_config_returns_zero_rules(self, tmp_path):
        cfg = tmp_path / "quality_rules.yaml"
        cfg.write_text(yaml.dump({"screens": {"orders": {}}}))
        screen = StructureScreen.from_config("orders", reference_dfs={}, config_path=cfg)
        assert screen.rules == []
