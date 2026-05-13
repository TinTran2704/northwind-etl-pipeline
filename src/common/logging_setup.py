"""
Logging setup for the Northwind ETL pipeline.

Provides setup_logging() and BatchLoggerAdapter so every log line carries
[batch=<id>] as required by docs/10-metadata-strategy.md §10.7.
"""

import logging
import sys
from pathlib import Path
from typing import Any, MutableMapping, Tuple

LOG_FORMAT = "[%(asctime)s] [batch=%(batch_id)s] %(levelname)s %(name)s — %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE = Path("logs/etl.log")


class _BatchIdFilter(logging.Filter):
    """Inject a fallback batch_id into records that don't carry one.

    Protects the formatter when plain logging.getLogger().info() is used
    without going through BatchLoggerAdapter.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "batch_id"):
            record.batch_id = "—"
        return True


class BatchLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that stamps batch_id on every emitted log record.

    Args:
        logger: Underlying logger instance.
        batch_id: ETL run identifier, e.g. ``etl-2024-06-25-103015``.

    Example::

        log = get_logger(__name__, batch_id="etl-2024-06-25")
        log.info("rows extracted: %d", 3150)
        # → [2024-06-25 10:30:15] [batch=etl-2024-06-25] INFO src.extract — rows extracted: 3150
    """

    def process(
        self,
        msg: str,
        kwargs: MutableMapping[str, Any],
    ) -> Tuple[str, MutableMapping[str, Any]]:
        kwargs["extra"] = {**self.extra, **(kwargs.get("extra") or {})}
        return msg, kwargs


def setup_logging(log_level: str = "INFO", log_file: Path = LOG_FILE) -> None:
    """Configure the root logger with console + file handlers.

    Safe to call multiple times (existing handlers are replaced). Installs
    _BatchIdFilter on every handler so %(batch_id)s is always resolvable.

    Args:
        log_level: One of DEBUG / INFO / WARNING / ERROR / CRITICAL.
        log_file: Destination file for persistent log output.
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    batch_filter = _BatchIdFilter()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(batch_filter)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(batch_filter)

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)


def get_logger(name: str, batch_id: str) -> BatchLoggerAdapter:
    """Return a BatchLoggerAdapter that stamps batch_id on every line.

    Args:
        name: Logger name (pass ``__name__`` from the calling module).
        batch_id: ETL run identifier.

    Returns:
        BatchLoggerAdapter wrapping logging.getLogger(name).
    """
    return BatchLoggerAdapter(logging.getLogger(name), {"batch_id": batch_id})
