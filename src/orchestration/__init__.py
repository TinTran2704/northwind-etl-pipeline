"""
Orchestration layer. Implements Kimball Subsystems 22-31.

Subsystem 22 — Job Scheduler    : src/orchestration/scheduler.py
Subsystem 24 — Recovery/Restart : src/common/checkpoint.py  (in common/)
Subsystem 26 — Version Migration: src/orchestration/migrations/
Subsystem 27 — Workflow Monitor : src/common/monitor.py     (in common/)
Subsystem 29 — Lineage          : src/common/lineage.py     (in common/)
Subsystem 30 — Problem Escalation: src/common/alerting.py   (in common/)
Subsystem 31 — Parallelism      : concurrent.futures in runner.py

See docs/09-subsystems.md for full subsystem mapping.
"""
