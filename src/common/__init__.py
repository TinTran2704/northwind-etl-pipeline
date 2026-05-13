"""
Common cross-cutting utilities shared by all ETL phases.

Provides: logging helpers, config loader, metadata context, type definitions,
checkpoint/recovery (Subsystem 24), workflow monitor (Subsystem 27),
lineage graph (Subsystem 29), alerting (Subsystem 30), metadata repository
(Subsystem 34).

Must be implemented before any other phase.
See docs/10-metadata-strategy.md.
"""
