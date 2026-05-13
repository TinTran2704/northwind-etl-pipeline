"""
Deliver phase modules. Implements Kimball Subsystems 9-16, 18-20.

Subsystem 9  — SCD Manager           : src/deliver/scd_manager.py
Subsystem 10 — Surrogate Key Gen     : src/deliver/surrogate_key_generator.py
Subsystem 11 — Hierarchy Manager     : src/deliver/hierarchy_manager.py
Subsystem 12 — Special Dimensions    : src/deliver/special_dimensions.py
Subsystem 13 — Fact Table Builders   : src/deliver/fact_table_builder.py
Subsystem 14 — Surrogate Key Pipeline: src/deliver/surrogate_key_pipeline.py
Subsystem 15 — Bridge Table Builder  : src/deliver/bridge_table_builder.py
Subsystem 16 — Late Arriving Handler : src/deliver/late_arriving_handler.py
Subsystem 18 — Fact Provider         : src/deliver/fact_provider.py
Subsystem 19 — Aggregate Builder     : src/deliver/aggregate_builder.py
Subsystem 20 — OLAP Cube Builder     : src/deliver/olap_cube_builder.py

Writes final star schema tables to data/warehouse/.
See docs/08-deliver-phase.md for spec.
"""
