"""Tests for src/extract/http_csv_extractor.py."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
import responses as responses_lib

from src.extract.base import ExtractError
from src.extract.http_csv_extractor import HttpCsvExtractor

_BASE_URL = "https://example.com/data"
_FILE = "customers"
_URL = f"{_BASE_URL}/{_FILE}.csv"
_CSV = "CustomerID,CompanyName\nALFKI,Alfreds\nANATR,Ana\n"


def _make_extractor(tmp_path: Path) -> HttpCsvExtractor:
    return HttpCsvExtractor("northwind", _BASE_URL, _FILE, tmp_path)


class TestExtractSuccess:
    @responses_lib.activate
    def test_returns_result_with_correct_fields(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.success is True
        assert result.source_name == "northwind"
        assert result.file_name == _FILE
        assert result.row_count == 2
        assert result.byte_size > 0
        assert result.snapshot_path.suffix == ".csv"

    @responses_lib.activate
    def test_snapshot_file_is_created(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.snapshot_path.exists()

    @responses_lib.activate
    def test_snapshot_content_matches_response(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.snapshot_path.read_text() == _CSV

    @responses_lib.activate
    def test_no_tmp_file_remains(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        tmp = result.snapshot_path.with_suffix(".tmp")
        assert not tmp.exists()

    @responses_lib.activate
    def test_manifest_is_created(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        manifest_path = result.snapshot_path.parent / "_manifest.json"
        assert manifest_path.exists()

    @responses_lib.activate
    def test_manifest_contains_file_entry(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        manifest = json.loads(
            (result.snapshot_path.parent / "_manifest.json").read_text()
        )
        assert manifest["source"] == "northwind"
        assert len(manifest["files"]) == 1
        entry = manifest["files"][0]
        assert entry["name"] == f"{_FILE}.csv"
        assert entry["rows"] == 2
        assert "sha256" in entry

    @responses_lib.activate
    def test_manifest_accumulates_multiple_files(self, tmp_path):
        """Second extract to same snapshot dir appends to manifest."""
        ext = HttpCsvExtractor("northwind", _BASE_URL, _FILE, tmp_path)
        ext2 = HttpCsvExtractor("northwind", _BASE_URL, "orders", tmp_path)

        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        responses_lib.add(
            responses_lib.GET, f"{_BASE_URL}/orders.csv",
            body="OrderID\n1\n2\n", status=200,
        )
        r1 = ext.extract()
        # Reuse same snapshot dir by patching get_snapshot_path
        snap_dir = r1.snapshot_path.parent
        with patch.object(ext2, "get_snapshot_path", return_value=snap_dir):
            ext2.extract()

        manifest = json.loads((snap_dir / "_manifest.json").read_text())
        assert len(manifest["files"]) == 2


class TestExtractErrors:
    @responses_lib.activate
    def test_http_404_raises_extract_error(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, status=404, body="Not Found")
        with pytest.raises(ExtractError, match="HTTP 404"):
            _make_extractor(tmp_path).extract()

    @responses_lib.activate
    def test_http_404_does_not_create_csv(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, status=404, body="Not Found")
        with pytest.raises(ExtractError):
            _make_extractor(tmp_path).extract()
        csv_files = list(tmp_path.rglob("*.csv"))
        assert csv_files == []

    @responses_lib.activate
    def test_empty_response_raises_extract_error(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, status=200, body=b"")
        with pytest.raises(ExtractError, match="[Ee]mpty"):
            _make_extractor(tmp_path).extract()


class TestAtomicWrite:
    @responses_lib.activate
    def test_no_csv_when_rename_fails(self, tmp_path):
        """If rename raises, no .csv file should exist (atomicity guarantee)."""
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        with patch("pathlib.Path.rename", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                _make_extractor(tmp_path).extract()
        csv_files = list(tmp_path.rglob("*.csv"))
        assert csv_files == []

    @responses_lib.activate
    def test_no_tmp_file_when_rename_fails(self, tmp_path):
        """If rename raises, .tmp must be cleaned up."""
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        with patch("pathlib.Path.rename", side_effect=OSError("disk full")):
            with pytest.raises(OSError):
                _make_extractor(tmp_path).extract()
        tmp_files = list(tmp_path.rglob("*.tmp"))
        assert tmp_files == []


class TestRetry:
    @responses_lib.activate
    @patch("time.sleep")
    def test_succeeds_after_two_network_errors(self, mock_sleep, tmp_path):
        responses_lib.add(
            responses_lib.GET, _URL,
            body=requests.exceptions.ConnectionError("refused"),
        )
        responses_lib.add(
            responses_lib.GET, _URL,
            body=requests.exceptions.ConnectionError("refused"),
        )
        responses_lib.add(responses_lib.GET, _URL, body=_CSV, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.success is True
        assert len(responses_lib.calls) == 3

    @responses_lib.activate
    @patch("time.sleep")
    def test_raises_after_three_consecutive_errors(self, mock_sleep, tmp_path):
        for _ in range(3):
            responses_lib.add(
                responses_lib.GET, _URL,
                body=requests.exceptions.ConnectionError("refused"),
            )
        with pytest.raises(ExtractError, match="[Rr]etries"):
            _make_extractor(tmp_path).extract()
        assert len(responses_lib.calls) == 3

    @responses_lib.activate
    @patch("time.sleep")
    def test_no_retry_on_http_404(self, mock_sleep, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, status=404, body="Not Found")
        with pytest.raises(ExtractError, match="HTTP 404"):
            _make_extractor(tmp_path).extract()
        # Should NOT retry on 4xx
        assert len(responses_lib.calls) == 1
