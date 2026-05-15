"""
Base types for all data quality screens.

Provides Severity enum, ScreenResult dataclass, and BaseScreen ABC.
Implements part of Kimball Subsystem #4.
See docs/06-clean-phase.md §6.4.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, List, Optional

import pandas as pd


class Severity(str, Enum):
    """Violation severity that drives pipeline behaviour.

    INFO  — log only, no downstream effect.
    WARN  — log, persist event, pass row with has_anomalies=True.
    ERROR — log, persist event, quarantine row.
    FATAL — log, persist event, stop entire batch immediately.
    """

    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


@dataclass
class ScreenResult:
    """A single data-quality violation reported by a screen.

    Attributes:
        screen_name:  Dot-separated id, e.g. ``column_property.not_null``.
        severity:     How serious the violation is.
        record_id:    PK of the offending row (stringified for composite keys).
        column_name:  Column involved; None for dataset-level checks.
        expected:     Human-readable description of what was expected.
        actual:       The value that caused the violation.
        message:      Free-text description.
    """

    screen_name: str
    severity: Severity
    record_id: Any
    column_name: Optional[str]
    expected: Optional[str]
    actual: Optional[str]
    message: str


class BaseScreen(ABC):
    """Abstract base for all data quality screens.

    Contract:
    - ``check()`` MUST NOT raise exceptions for data violations — return them.
    - ``check()`` MUST NOT modify the input DataFrame.
    - Every concrete subclass defines a ``name`` class attribute.
    """

    name: str

    @abstractmethod
    def check(self, df: pd.DataFrame) -> List[ScreenResult]:
        """Run this screen against *df* and return all violations found."""
