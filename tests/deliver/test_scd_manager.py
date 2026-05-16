"""Tests for SCDManager (Kimball Subsystem #9)."""

from datetime import date

import pandas as pd
import pytest

from src.deliver.scd_manager import SCDManager
from src.deliver.surrogate_key_generator import SurrogateKeyGenerator


@pytest.fixture
def sk_gen(tmp_path):
    return SurrogateKeyGenerator(meta_dir=tmp_path)


@pytest.fixture
def scd(sk_gen):
    return SCDManager(sk_gen=sk_gen)


def _make_existing(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _new_source(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


class TestInitialLoad:
    def test_all_nks_inserted(self, scd):
        new = _new_source([
            {"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"},
            {"customer_nk": "ANATR", "company_name": "Ana Trujillo", "contact_name": "Ana"},
        ])
        existing = pd.DataFrame()
        dim, log = scd.apply_scd(
            new, existing, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        assert len(dim) == 2
        assert set(log["action"]) == {"INSERT"}
        assert len(log) == 2

    def test_sk_assigned_starting_at_one(self, scd):
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, log = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        assert dim.iloc[0]["customer_sk"] == 1

    def test_scd_columns_set(self, scd):
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, _ = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        row = dim.iloc[0]
        assert row["effective_date"] == "2024-01-01"
        assert row["expiration_date"] is None
        assert row["is_current"] == True


class TestType2Change:
    def _initial_dim(self, scd) -> pd.DataFrame:
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, _ = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        return dim

    def test_type2_change_expires_old_row(self, scd):
        existing = self._initial_dim(scd)
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds RENAMED", "contact_name": "Maria"}])
        dim, log = scd.apply_scd(
            new, existing, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        expire_actions = log[log["action"] == "UPDATE_T2_EXPIRE"]
        assert len(expire_actions) == 1
        old_row = dim[dim["customer_sk"] == expire_actions.iloc[0]["old_sk"]]
        assert old_row.iloc[0]["is_current"] == False
        assert old_row.iloc[0]["expiration_date"] == "2024-05-31"

    def test_type2_change_inserts_new_row(self, scd):
        existing = self._initial_dim(scd)
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds RENAMED", "contact_name": "Maria"}])
        dim, log = scd.apply_scd(
            new, existing, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        new_actions = log[log["action"] == "UPDATE_T2_NEW"]
        assert len(new_actions) == 1
        new_row = dim[dim["customer_sk"] == new_actions.iloc[0]["new_sk"]]
        assert new_row.iloc[0]["is_current"] == True
        assert new_row.iloc[0]["company_name"] == "Alfreds RENAMED"


class TestType1Change:
    def _initial_dim(self, scd) -> pd.DataFrame:
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, _ = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        return dim

    def test_type1_updates_in_place(self, scd):
        existing = self._initial_dim(scd)
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria Updated"}])
        dim, log = scd.apply_scd(
            new, existing, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        actions = log[log["action"] == "UPDATE_T1"]
        assert len(actions) == 1
        alfki_row = dim[dim["customer_nk"] == "ALFKI"]
        assert alfki_row.iloc[0]["contact_name"] == "Maria Updated"

    def test_type1_does_not_add_new_row(self, scd):
        existing = self._initial_dim(scd)
        initial_count = len(existing)
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria Updated"}])
        dim, _ = scd.apply_scd(
            new, existing, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        assert len(dim) == initial_count


class TestNoChange:
    def test_no_change_produces_no_log_entry(self, scd):
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, _ = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        new2 = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        _, log2 = scd.apply_scd(
            new2, dim, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        assert len(log2) == 0


class TestVanishedNK:
    def test_vanished_nk_gets_expired(self, scd):
        new = _new_source([
            {"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"},
            {"customer_nk": "ANATR", "company_name": "Ana Trujillo", "contact_name": "Ana"},
        ])
        dim, _ = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        # ANATR vanishes
        new2 = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim2, log2 = scd.apply_scd(
            new2, dim, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 6, 1), dim_name="dim_customer",
        )
        expire_actions = log2[log2["action"] == "EXPIRE"]
        assert len(expire_actions) == 1
        assert expire_actions.iloc[0]["nk"] == "ANATR"
        anatr_row = dim2[dim2["customer_nk"] == "ANATR"]
        assert anatr_row.iloc[0]["is_current"] == False


class TestUnknownMember:
    def test_unknown_row_preserved_in_output(self, scd):
        unknown = pd.DataFrame([{
            "customer_sk": -1, "customer_nk": "UNKNOWN",
            "company_name": "Unknown", "contact_name": None,
            "effective_date": "1900-01-01", "expiration_date": None,
            "is_current": True,
        }])
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        dim, _ = scd.apply_scd(
            new, unknown, "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        assert -1 in dim["customer_sk"].values


class TestChangeLogSchema:
    def test_change_log_has_required_columns(self, scd):
        new = _new_source([{"customer_nk": "ALFKI", "company_name": "Alfreds", "contact_name": "Maria"}])
        _, log = scd.apply_scd(
            new, pd.DataFrame(), "customer_nk", "customer_sk",
            type2_cols=["company_name"], type1_cols=["contact_name"],
            effective_date=date(2024, 1, 1), dim_name="dim_customer",
        )
        assert list(log.columns) == ["nk", "action", "old_sk", "new_sk"]
