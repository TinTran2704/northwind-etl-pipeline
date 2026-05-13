"""
Clean phase modules. Implements Kimball Subsystems 4-6.

Subsystem 4 — Data Cleansing System  : src/clean/screens/*.py
Subsystem 5 — Error Event Tracking   : src/clean/error_event_logger.py
Subsystem 6 — Audit Dimension Builder: src/clean/audit_dimension_builder.py

Reads from data/raw/, writes cleaned rows to data/staging/cleaned/
and rejected rows to data/error/error_events.parquet.
See docs/06-clean-phase.md for spec.
"""
