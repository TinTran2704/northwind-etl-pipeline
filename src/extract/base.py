"""
Base types for the Extract phase.

Defines ExtractResult, ExtractError, and BaseExtractor (Kimball Subsystem #3).
See docs/05-extract-phase.md §5.3 for the interface spec.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


class ExtractError(Exception):
    """Raised when extraction fails (HTTP error, empty body, I/O failure)."""


@dataclass
class ExtractResult:
    """Outcome of a single extract operation.

    Attributes:
        source_name:   Logical source identifier, e.g. ``"northwind"``.
        file_name:     File stem without extension, e.g. ``"customers"``.
        snapshot_path: Absolute path to the written snapshot file.
        row_count:     Number of data rows (excluding header for CSV).
        byte_size:     File size in bytes after writing.
        extracted_at:  UTC timestamp when extraction started.
        success:       True if extraction completed without error.
        error_message: Populated only when success is False.
    """

    source_name: str
    file_name: str
    snapshot_path: Path
    row_count: int
    byte_size: int
    extracted_at: datetime
    success: bool
    error_message: Optional[str] = None


class BaseExtractor(ABC):
    """Abstract base for all extractors. Implements Kimball Subsystem #3.

    Args:
        source_name: Logical source identifier.
        target_dir:  Root directory where snapshot subdirs are created.
    """

    def __init__(self, source_name: str, target_dir: Path) -> None:
        self.source_name = source_name
        self.target_dir = target_dir

    @abstractmethod
    def extract(self) -> ExtractResult:
        """Run extraction and return an ExtractResult."""

    def get_snapshot_path(self) -> Path:
        """Return a timestamped snapshot directory path (not yet created).

        Returns:
            Path of the form ``target_dir/YYYY-MM-DD-HHMMSS``.
        """
        ts = datetime.utcnow().strftime("%Y-%m-%d-%H%M%S")
        return self.target_dir / ts

    def write_audit_record(self, result: ExtractResult) -> None:
        """Stub — full implementation in src/clean/audit_dimension_builder.py."""
