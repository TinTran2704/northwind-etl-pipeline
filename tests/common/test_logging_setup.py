"""Tests for src/common/logging_setup.py."""

import logging
from pathlib import Path

import pytest

import src.common.logging_setup as ls
from src.common.logging_setup import BatchLoggerAdapter, _BatchIdFilter, get_logger, setup_logging


@pytest.fixture(autouse=True)
def restore_root_handlers():
    """Reset root logger handlers after every test to avoid cross-test pollution."""
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    yield
    root.handlers = original_handlers
    root.level = original_level


class TestSetupLogging:
    def test_creates_two_handlers(self, tmp_path):
        setup_logging(log_file=tmp_path / "etl.log")
        assert len(logging.getLogger().handlers) == 2

    def test_log_file_is_created(self, tmp_path):
        log_file = tmp_path / "sub" / "etl.log"
        setup_logging(log_file=log_file)
        logging.getLogger().info("ping")
        assert log_file.exists()

    def test_log_level_is_applied(self, tmp_path):
        setup_logging(log_level="WARNING", log_file=tmp_path / "etl.log")
        assert logging.getLogger().level == logging.WARNING

    def test_repeated_calls_do_not_stack_handlers(self, tmp_path):
        log_file = tmp_path / "etl.log"
        setup_logging(log_file=log_file)
        setup_logging(log_file=log_file)
        assert len(logging.getLogger().handlers) == 2

    def test_format_contains_batch_id_slot(self, tmp_path):
        setup_logging(log_file=tmp_path / "etl.log")
        handler = logging.getLogger().handlers[0]
        assert "batch_id" in handler.formatter._fmt


class TestBatchIdFilter:
    def test_adds_batch_id_if_missing(self):
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        assert not hasattr(record, "batch_id")
        _BatchIdFilter().filter(record)
        assert record.batch_id == "—"

    def test_does_not_overwrite_existing_batch_id(self):
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello", args=(), exc_info=None,
        )
        record.batch_id = "etl-2024-01-01"
        _BatchIdFilter().filter(record)
        assert record.batch_id == "etl-2024-01-01"


class TestBatchLoggerAdapter:
    def test_process_injects_batch_id(self):
        adapter = BatchLoggerAdapter(logging.getLogger("test"), {"batch_id": "run-42"})
        _, kwargs = adapter.process("msg", {})
        assert kwargs["extra"]["batch_id"] == "run-42"

    def test_process_preserves_existing_extra(self):
        adapter = BatchLoggerAdapter(logging.getLogger("test"), {"batch_id": "run-42"})
        _, kwargs = adapter.process("msg", {"extra": {"foo": "bar"}})
        assert kwargs["extra"]["batch_id"] == "run-42"
        assert kwargs["extra"]["foo"] == "bar"

    def test_caller_extra_overrides_adapter_extra(self):
        adapter = BatchLoggerAdapter(logging.getLogger("test"), {"batch_id": "run-A"})
        _, kwargs = adapter.process("msg", {"extra": {"batch_id": "run-B"}})
        assert kwargs["extra"]["batch_id"] == "run-B"


class TestGetLogger:
    def test_returns_batch_logger_adapter(self):
        adapter = get_logger("mymodule", "etl-2024-01-01")
        assert isinstance(adapter, BatchLoggerAdapter)

    def test_adapter_carries_batch_id(self):
        adapter = get_logger("mymodule", "etl-xyz")
        assert adapter.extra["batch_id"] == "etl-xyz"

    def test_underlying_logger_name(self):
        adapter = get_logger("src.extract", "etl-xyz")
        assert adapter.logger.name == "src.extract"
