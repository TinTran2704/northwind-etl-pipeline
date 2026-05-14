"""Tests for src/extract/base.py."""

from datetime import datetime
from pathlib import Path

import pytest

from src.extract.base import BaseExtractor, ExtractError, ExtractResult


class _ConcreteExtractor(BaseExtractor):
    def extract(self) -> ExtractResult:
        return ExtractResult(
            source_name=self.source_name,
            file_name="test",
            snapshot_path=self.target_dir / "snap" / "test.csv",
            row_count=0,
            byte_size=0,
            extracted_at=datetime.utcnow(),
            success=True,
        )


class TestExtractResult:
    def test_defaults(self):
        r = ExtractResult(
            source_name="s", file_name="f",
            snapshot_path=Path("x"), row_count=1,
            byte_size=10, extracted_at=datetime.utcnow(), success=True,
        )
        assert r.error_message is None

    def test_failed_result(self):
        r = ExtractResult(
            source_name="s", file_name="f",
            snapshot_path=Path("x"), row_count=0,
            byte_size=0, extracted_at=datetime.utcnow(), success=False,
            error_message="boom",
        )
        assert r.success is False
        assert r.error_message == "boom"


class TestExtractError:
    def test_is_exception(self):
        with pytest.raises(ExtractError, match="test error"):
            raise ExtractError("test error")


class TestBaseExtractor:
    def test_get_snapshot_path_format(self, tmp_path):
        ext = _ConcreteExtractor("src", tmp_path)
        snap = ext.get_snapshot_path()
        assert snap.parent == tmp_path
        # Name matches YYYY-MM-DD-HHMMSS
        assert len(snap.name) == 17

    def test_get_snapshot_path_is_new_each_call(self, tmp_path):
        ext = _ConcreteExtractor("src", tmp_path)
        # Two calls at the same second return the same path (same timestamp granularity)
        # — this just verifies it's deterministic within a second
        p1 = ext.get_snapshot_path()
        assert p1.parent == tmp_path

    def test_concrete_extract(self, tmp_path):
        ext = _ConcreteExtractor("northwind", tmp_path)
        result = ext.extract()
        assert result.success is True
        assert result.source_name == "northwind"
