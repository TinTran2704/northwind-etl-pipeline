"""Tests for src/extract/cdc/crc_cdc.py."""

import pandas as pd
import pytest

from src.extract.cdc.crc_cdc import CrcCdc

_PK = ["CustomerID"]


def _cdc(tmp_path, entity="customers"):
    return CrcCdc(entity=entity, pk_columns=_PK, crc_index_dir=tmp_path / "_crc")


def _df(*rows) -> pd.DataFrame:
    """Build a small customers DataFrame from (id, name, city) tuples."""
    return pd.DataFrame(rows, columns=["CustomerID", "CompanyName", "City"])


class TestComputeCrc:
    def test_returns_series_of_same_length(self, tmp_path):
        df = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        crcs = _cdc(tmp_path).compute_crc(df)
        assert len(crcs) == 2

    def test_same_row_same_crc(self, tmp_path):
        df1 = _df(("A1", "Foo", "HCM"))
        df2 = _df(("A1", "Foo", "HCM"))
        cdc = _cdc(tmp_path)
        assert cdc.compute_crc(df1).iloc[0] == cdc.compute_crc(df2).iloc[0]

    def test_different_row_different_crc(self, tmp_path):
        df1 = _df(("A1", "Foo", "HCM"))
        df2 = _df(("A1", "Foo", "HN"))  # City differs
        cdc = _cdc(tmp_path)
        assert cdc.compute_crc(df1).iloc[0] != cdc.compute_crc(df2).iloc[0]

    def test_crc_is_unsigned_32bit(self, tmp_path):
        df = _df(("A1", "Foo", "HCM"))
        crc = _cdc(tmp_path).compute_crc(df).iloc[0]
        assert 0 <= crc <= 0xFFFFFFFF


class TestDiffFirstRun:
    def test_all_rows_are_inserts(self, tmp_path):
        new_df = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        result = _cdc(tmp_path).diff(pd.DataFrame(), new_df)
        assert len(result["inserts"]) == 2
        assert len(result["updates"]) == 0
        assert len(result["deletes"]) == 0

    def test_inserts_contain_all_columns(self, tmp_path):
        new_df = _df(("A1", "Foo", "HCM"))
        result = _cdc(tmp_path).diff(pd.DataFrame(), new_df)
        assert list(result["inserts"].columns) == list(new_df.columns)


class TestDiffUnchanged:
    def test_no_changes_produces_empty_results(self, tmp_path):
        df = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        result = _cdc(tmp_path).diff(df, df)
        assert len(result["inserts"]) == 0
        assert len(result["updates"]) == 0
        assert len(result["deletes"]) == 0


class TestDiffInsert:
    def test_new_row_detected_as_insert(self, tmp_path):
        old = _df(("A1", "Foo", "HCM"))
        new = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        result = _cdc(tmp_path).diff(old, new)
        assert len(result["inserts"]) == 1
        assert result["inserts"]["CustomerID"].iloc[0] == "A2"
        assert len(result["updates"]) == 0
        assert len(result["deletes"]) == 0


class TestDiffUpdate:
    def test_changed_column_detected_as_update(self, tmp_path):
        old = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        new = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "Hanoi"))  # City changed
        result = _cdc(tmp_path).diff(old, new)
        assert len(result["updates"]) == 1
        assert result["updates"]["CustomerID"].iloc[0] == "A2"
        assert len(result["inserts"]) == 0
        assert len(result["deletes"]) == 0

    def test_update_row_has_new_values(self, tmp_path):
        old = _df(("A1", "Foo", "HCM"))
        new = _df(("A1", "Foo", "Saigon"))
        result = _cdc(tmp_path).diff(old, new)
        assert result["updates"]["City"].iloc[0] == "Saigon"


class TestDiffDelete:
    def test_removed_row_detected_as_delete(self, tmp_path):
        old = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        new = _df(("A1", "Foo", "HCM"))
        result = _cdc(tmp_path).diff(old, new)
        assert len(result["deletes"]) == 1
        assert result["deletes"]["CustomerID"].iloc[0] == "A2"
        assert len(result["inserts"]) == 0
        assert len(result["updates"]) == 0


class TestDiffMixed:
    def test_insert_update_delete_simultaneously(self, tmp_path):
        old = _df(
            ("A1", "Foo", "HCM"),
            ("A2", "Bar", "HN"),
            ("A3", "Baz", "DN"),
        )
        new = _df(
            ("A1", "Foo", "HCM"),      # unchanged
            ("A2", "Bar", "Hanoi"),    # updated City
            ("A4", "New", "VT"),       # inserted
        )                              # A3 deleted
        result = _cdc(tmp_path).diff(old, new)
        assert len(result["inserts"]) == 1
        assert len(result["updates"]) == 1
        assert len(result["deletes"]) == 1


class TestCrcIndex:
    def test_save_and_load_round_trip(self, tmp_path):
        cdc = _cdc(tmp_path)
        df = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        cdc.save_crc_index(df)
        loaded = cdc.load_crc_index()
        assert loaded is not None
        assert "_crc" in loaded.columns
        assert len(loaded) == 2

    def test_load_returns_none_when_no_index(self, tmp_path):
        cdc = _cdc(tmp_path)
        assert cdc.load_crc_index() is None

    def test_save_creates_parquet_file(self, tmp_path):
        cdc = _cdc(tmp_path)
        path = cdc.save_crc_index(_df(("A1", "Foo", "HCM")))
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_second_run_uses_saved_index(self, tmp_path):
        cdc = _cdc(tmp_path)
        first = _df(("A1", "Foo", "HCM"))
        cdc.save_crc_index(first)

        loaded = cdc.load_crc_index()
        old_df = loaded.drop(columns=["_crc"])
        second = _df(("A1", "Foo", "HCM"), ("A2", "Bar", "HN"))
        result = cdc.diff(old_df, second)
        assert len(result["inserts"]) == 1
        assert result["inserts"]["CustomerID"].iloc[0] == "A2"
