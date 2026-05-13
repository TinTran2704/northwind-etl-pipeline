"""
Conform phase modules. Implements Kimball Subsystems 7-8, 17, 21.

Subsystem 7  — Deduplication        : src/conform/deduplicator.py
Subsystem 8  — Survivorship         : src/conform/survivor_selector.py
Subsystem 17 — Dimension Manager    : src/conform/dimension_manager.py
Subsystem 21 — Data Integration Mgr : src/conform/integration_manager.py

Reads from data/staging/cleaned/, writes golden records to
data/staging/conformed/.
See docs/07-conform-phase.md for spec.
"""
