# Northwind DW — dbt Project (Phase 3)

This dbt project builds the semantic layer on top of the Northwind data warehouse loaded by the Kimball ETL pipeline (Phases 1–2). It transforms raw warehouse tables in the `warehouse` schema (dims + facts) into versioned, documented, and tested marts ready for BI tools, exposing three model layers: **staging** (thin views over source tables), **intermediate** (business-logic joins), and **marts** (final dimensional tables and aggregations published for consumption).
