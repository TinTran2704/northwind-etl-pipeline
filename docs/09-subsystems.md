# 09 - Mapping 34 Kimball ETL Subsystems

> Đối chiếu mỗi subsystem với module trong dự án này. Hữu ích để Claude Code tra cứu khi generate code.

## Group 1: Extracting (Subsystems 1-3)

| # | Subsystem | Module | Status (sau khi build) |
|---|---|---|---|
| 1 | Data Profiling | `src/extract/profiling.py` (script) | Manual run trước extract |
| 2 | Change Data Capture | `src/extract/cdc/crc_cdc.py` | CRC-based, full snapshot fallback |
| 3 | Extract System | `src/extract/{http_csv,rest_json}_extractor.py` | HTTP-based |

## Group 2: Cleaning & Conforming (Subsystems 4-8)

| # | Subsystem | Module |
|---|---|---|
| 4 | Data Cleansing System | `src/clean/screens/*.py` |
| 5 | Error Event Tracking | `src/clean/error_event_logger.py` |
| 6 | Audit Dimension Creation | `src/clean/audit_dimension_builder.py` |
| 7 | Deduplication | `src/conform/deduplicator.py` |
| 8 | Survivorship | `src/conform/survivor_selector.py` |

## Group 3: Delivering Dimensions (Subsystems 9-12, 17)

| # | Subsystem | Module |
|---|---|---|
| 9 | Slowly Changing Dimension Manager | `src/deliver/scd_manager.py` |
| 10 | Surrogate Key Generator | `src/deliver/surrogate_key_generator.py` |
| 11 | Hierarchy Manager | `src/deliver/hierarchy_manager.py` |
| 12 | Special Dimensions Manager | `src/deliver/special_dimensions.py` |
| 17 | Dimension Manager (Conformed Dim hub) | `src/conform/dimension_manager.py` |

## Group 4: Delivering Facts (Subsystems 13-16, 18-21)

| # | Subsystem | Module |
|---|---|---|
| 13 | Fact Table Builders | `src/deliver/fact_table_builder.py` |
| 14 | Surrogate Key Pipeline | `src/deliver/surrogate_key_pipeline.py` |
| 15 | Multi-Valued Bridge Table Builder | `src/deliver/bridge_table_builder.py` |
| 16 | Late Arriving Data Handler | `src/deliver/late_arriving_handler.py` |
| 18 | Fact Provider | `src/deliver/fact_provider.py` |
| 19 | Aggregate Builder | `src/deliver/aggregate_builder.py` |
| 20 | OLAP Cube Builder | `src/deliver/olap_cube_builder.py` |
| 21 | Data Integration Manager | `src/conform/integration_manager.py` |

## Group 5: Managing the ETL Environment (Subsystems 22-34)

| # | Subsystem | Module / Strategy |
|---|---|---|
| 22 | Job Scheduler | `src/orchestration/scheduler.py` (cron-style) |
| 23 | Backup System | `scripts/backup.sh` (rsync data/ → backups/) |
| 24 | Recovery and Restart | `src/common/checkpoint.py` |
| 25 | Version Control | Git (external) — không code |
| 26 | Version Migration | `src/orchestration/migrations/` |
| 27 | Workflow Monitor | `src/common/monitor.py` (logs metrics) |
| 28 | Sorting | Pandas/DuckDB native |
| 29 | Lineage and Dependency | `src/common/lineage.py` (graph builder) |
| 30 | Problem Escalation | `src/common/alerting.py` (log + email stub) |
| 31 | Paralleling/Pipelining | `concurrent.futures` trong `runner.py` |
| 32 | Security | `.env` cho credentials, gitignore |
| 33 | Compliance Manager | `src/common/compliance.py` (data retention rules) |
| 34 | Metadata Repository | DuckDB `metadata` schema + JSON files |

## Subsystem priority cho dự án này

**Phase 1 (MVP) — bắt buộc:**
- 1, 2, 3 (Extract toàn bộ)
- 4, 5, 6 (Clean cơ bản)
- 9, 10 (SCD Type 2 + SK)
- 13, 14 (Fact + SK Pipeline)
- 34 (Metadata)

**Phase 2 — quan trọng:**
- 7, 8 (Dedup + Survivor)
- 17, 18 (Conformed dim + fact provider)
- 16 (Late arriving)
- 19 (Aggregate)
- 22, 24 (Scheduler + Recovery)

**Phase 3 — advanced:**
- 11, 12, 15 (Hierarchy, special dim, bridge)
- 20 (OLAP cube)
- 26, 27, 29, 30 (Migration, monitor, lineage, escalation)

**Out of scope cho dự án học:**
- 23, 25, 32, 33 (operational concerns)
