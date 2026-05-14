"""Tests for src/common/config.py."""

import os
from pathlib import Path

import pytest
import yaml

from src.common.config import (
    DatabaseConfig,
    get_db_config,
    load_env,
    load_sources_config,
)

_DB_ENV = {
    "ETL_DW_HOST": "testhost",
    "ETL_DW_PORT": "5433",
    "ETL_DW_USER": "testuser",
    "ETL_DW_PASSWORD": "testpass",
    "ETL_DW_DATABASE": "testdb",
}


class TestLoadEnv:
    def test_loads_values_from_file(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("MY_TEST_VAR=hello_world\n")
        load_env(env_file)
        assert os.environ.get("MY_TEST_VAR") == "hello_world"
        del os.environ["MY_TEST_VAR"]

    def test_does_not_override_existing_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_EXISTING_VAR", "original")
        env_file = tmp_path / ".env"
        env_file.write_text("MY_EXISTING_VAR=overridden\n")
        load_env(env_file)
        assert os.environ["MY_EXISTING_VAR"] == "original"

    def test_missing_file_does_not_raise(self, tmp_path):
        load_env(tmp_path / "nonexistent.env")


class TestLoadSourcesConfig:
    def test_loads_valid_yaml(self, tmp_path):
        cfg_file = tmp_path / "sources.yaml"
        cfg_file.write_text("sources:\n  northwind:\n    type: http_csv\n")
        result = load_sources_config(cfg_file)
        assert result["sources"]["northwind"]["type"] == "http_csv"

    def test_empty_yaml_returns_empty_dict(self, tmp_path):
        cfg_file = tmp_path / "sources.yaml"
        cfg_file.write_text("")
        assert load_sources_config(cfg_file) == {}

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="Sources config not found"):
            load_sources_config(tmp_path / "missing.yaml")

    def test_loads_project_sources_yaml(self):
        result = load_sources_config(Path("config/sources.yaml"))
        assert "sources" in result
        assert "northwind" in result["sources"]


class TestDatabaseConfig:
    def test_is_frozen(self):
        cfg = DatabaseConfig(host="h", port=5432, user="u", password="p", database="d")
        with pytest.raises(Exception):
            cfg.host = "other"  # type: ignore[misc]

    def test_fields(self):
        cfg = DatabaseConfig(host="h", port=5432, user="u", password="p", database="d")
        assert cfg.host == "h"
        assert cfg.port == 5432
        assert cfg.user == "u"
        assert cfg.password == "p"
        assert cfg.database == "d"


class TestGetDbConfig:
    def test_returns_database_config(self, monkeypatch):
        for k, v in _DB_ENV.items():
            monkeypatch.setenv(k, v)
        cfg = get_db_config()
        assert isinstance(cfg, DatabaseConfig)
        assert cfg.host == "testhost"
        assert cfg.port == 5433
        assert cfg.user == "testuser"
        assert cfg.password == "testpass"
        assert cfg.database == "testdb"

    def test_port_falls_back_to_postgres_port(self, monkeypatch):
        for k, v in _DB_ENV.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("ETL_DW_PORT", raising=False)
        monkeypatch.setenv("POSTGRES_PORT", "9999")
        cfg = get_db_config()
        assert cfg.port == 9999

    def test_port_defaults_to_5432_when_no_env(self, monkeypatch):
        for k, v in _DB_ENV.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("ETL_DW_PORT", raising=False)
        monkeypatch.delenv("POSTGRES_PORT", raising=False)
        cfg = get_db_config()
        assert cfg.port == 5432

    def test_host_defaults_to_localhost(self, monkeypatch):
        for k, v in _DB_ENV.items():
            monkeypatch.setenv(k, v)
        monkeypatch.delenv("ETL_DW_HOST", raising=False)
        cfg = get_db_config()
        assert cfg.host == "localhost"

    def test_raises_on_missing_required_vars(self, monkeypatch):
        # Patch load_env to no-op so the .env file doesn't repopulate the vars
        monkeypatch.setattr("src.common.config.load_env", lambda *_: None)
        monkeypatch.delenv("ETL_DW_USER", raising=False)
        monkeypatch.delenv("ETL_DW_PASSWORD", raising=False)
        monkeypatch.delenv("ETL_DW_DATABASE", raising=False)
        with pytest.raises(EnvironmentError, match="Missing required"):
            get_db_config()
