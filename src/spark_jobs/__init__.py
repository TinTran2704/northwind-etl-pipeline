"""
Spark Jobs — PySpark replacements for the Pandas-based transform phases.

Implements Kimball Subsystems #4-6 (Clean) and #9-14 (Deliver) at scale.
See docs/12-phase2-kafka-spark.md §12.8 for spec.

Modules:
    common      — SparkSession factory and JDBC helpers
    clean_job   — Distributed quality screens (replaces src/clean/pipeline.py)
    deliver_job — Distributed SK pipeline and fact load (replaces src/deliver/pipeline.py)
"""
