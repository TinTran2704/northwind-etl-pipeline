"""
Tests for src/extract/ — Subsystems 1-3.

Covers: HttpCsvExtractor, RestJsonExtractor, CRC-based CDC, data profiling.
Network calls must be mocked via requests_mock; no real HTTP in unit tests.
"""
