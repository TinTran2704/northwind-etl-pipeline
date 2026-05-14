"""Tests for src/extract/rest_json_extractor.py."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import requests
import responses as responses_lib

from src.extract.base import ExtractError
from src.extract.rest_json_extractor import RestJsonExtractor

_URL = "https://api.example.com/countries"
_JSON_LIST = [{"cca2": "VN", "name": {"common": "Vietnam"}}, {"cca2": "US", "name": {"common": "USA"}}]
_JSON_OBJ = {"result": "success", "rates": {"EUR": 0.93}}


def _make_extractor(tmp_path: Path, **kwargs) -> RestJsonExtractor:
    return RestJsonExtractor("countries", _URL, "countries", tmp_path, **kwargs)


class TestExtractSuccess:
    @responses_lib.activate
    def test_list_response_row_count(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, json=_JSON_LIST, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.row_count == 2

    @responses_lib.activate
    def test_object_response_row_count_is_one(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, json=_JSON_OBJ, status=200)
        result = RestJsonExtractor("exchange_rate", _URL, "usd_rates", tmp_path).extract()
        assert result.row_count == 1

    @responses_lib.activate
    def test_snapshot_file_created(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, json=_JSON_LIST, status=200)
        result = _make_extractor(tmp_path).extract()
        assert result.snapshot_path.exists()
        assert result.snapshot_path.suffix == ".json"

    @responses_lib.activate
    def test_snapshot_content_is_valid_json(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, json=_JSON_LIST, status=200)
        result = _make_extractor(tmp_path).extract()
        parsed = json.loads(result.snapshot_path.read_bytes())
        assert isinstance(parsed, list)
        assert len(parsed) == 2

    @responses_lib.activate
    def test_no_tmp_file_remains(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, json=_JSON_LIST, status=200)
        result = _make_extractor(tmp_path).extract()
        assert not result.snapshot_path.with_suffix(".tmp").exists()


class TestValidation:
    @responses_lib.activate
    def test_invalid_json_raises_extract_error(self, tmp_path):
        responses_lib.add(
            responses_lib.GET, _URL,
            body=b"NOT JSON {{{{", status=200,
            content_type="application/json",
        )
        with pytest.raises(ExtractError, match="[Ii]nvalid JSON"):
            _make_extractor(tmp_path).extract()

    @responses_lib.activate
    def test_http_404_raises_extract_error(self, tmp_path):
        responses_lib.add(responses_lib.GET, _URL, status=404, body="not found")
        with pytest.raises(ExtractError):
            _make_extractor(tmp_path).extract()


class TestFallback:
    @responses_lib.activate
    @patch("time.sleep")
    def test_uses_seed_file_on_connection_error(self, mock_sleep, tmp_path):
        seed = tmp_path / "seed" / "countries.json"
        seed.parent.mkdir()
        seed.write_text(json.dumps(_JSON_LIST))

        for _ in range(3):
            responses_lib.add(
                responses_lib.GET, _URL,
                body=requests.exceptions.ConnectionError("down"),
            )

        result = _make_extractor(tmp_path / "raw", fallback_seed=seed).extract()
        assert result.success is True
        assert result.row_count == 2

    @responses_lib.activate
    @patch("time.sleep")
    def test_raises_when_no_fallback_and_all_retries_fail(self, mock_sleep, tmp_path):
        for _ in range(3):
            responses_lib.add(
                responses_lib.GET, _URL,
                body=requests.exceptions.ConnectionError("down"),
            )
        with pytest.raises(ExtractError, match="[Rr]etries"):
            _make_extractor(tmp_path).extract()

    @responses_lib.activate
    @patch("time.sleep")
    def test_raises_when_seed_missing_and_all_retries_fail(self, mock_sleep, tmp_path):
        for _ in range(3):
            responses_lib.add(
                responses_lib.GET, _URL,
                body=requests.exceptions.ConnectionError("down"),
            )
        with pytest.raises(ExtractError):
            _make_extractor(tmp_path, fallback_seed=tmp_path / "nonexistent.json").extract()
