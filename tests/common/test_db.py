"""Tests for src/common/db.py."""

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import Engine
from sqlalchemy.pool import QueuePool

from src.common.config import DatabaseConfig
from src.common.db import _MAX_OVERFLOW, _POOL_SIZE, get_engine
from src.common.db import test_connection as db_test_connection

_CFG = DatabaseConfig(
    host="dbhost",
    port=5433,
    user="etl_user",
    password="secret",
    database="northwind_dw",
)


class TestGetEngine:
    def test_returns_engine(self):
        engine = get_engine(_CFG)
        assert isinstance(engine, Engine)

    def test_engine_url_host(self):
        engine = get_engine(_CFG)
        assert engine.url.host == "dbhost"

    def test_engine_url_port(self):
        engine = get_engine(_CFG)
        assert engine.url.port == 5433

    def test_engine_url_username(self):
        engine = get_engine(_CFG)
        assert engine.url.username == "etl_user"

    def test_engine_url_database(self):
        engine = get_engine(_CFG)
        assert engine.url.database == "northwind_dw"

    def test_engine_uses_psycopg2_driver(self):
        engine = get_engine(_CFG)
        assert engine.url.drivername == "postgresql+psycopg2"

    def test_pool_size(self):
        engine = get_engine(_CFG)
        assert engine.pool.size() == _POOL_SIZE

    def test_max_overflow(self):
        engine = get_engine(_CFG)
        assert engine.pool._max_overflow == _MAX_OVERFLOW

    def test_calls_get_db_config_when_no_config(self, monkeypatch):
        mock_cfg = MagicMock(return_value=_CFG)
        monkeypatch.setattr("src.common.db.get_db_config", mock_cfg)
        get_engine()
        mock_cfg.assert_called_once()


class TestTestConnection:
    def _make_mock_engine(self, select_result: int = 1) -> MagicMock:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.scalar.return_value = select_result
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_engine.url.host = "dbhost"
        mock_engine.url.port = 5432
        mock_engine.url.database = "northwind_dw"
        return mock_engine

    @patch("src.common.db.get_engine")
    def test_returns_true_on_success(self, mock_get_engine):
        mock_get_engine.return_value = self._make_mock_engine(select_result=1)
        assert db_test_connection(_CFG) is True

    @patch("src.common.db.get_engine")
    def test_raises_on_unexpected_result(self, mock_get_engine):
        mock_get_engine.return_value = self._make_mock_engine(select_result=99)
        with pytest.raises(RuntimeError, match="Unexpected result"):
            db_test_connection(_CFG)

    @patch("src.common.db.get_engine")
    def test_propagates_operational_error(self, mock_get_engine):
        from sqlalchemy.exc import OperationalError

        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError("conn", None, None)
        mock_get_engine.return_value = mock_engine
        with pytest.raises(OperationalError):
            db_test_connection(_CFG)
