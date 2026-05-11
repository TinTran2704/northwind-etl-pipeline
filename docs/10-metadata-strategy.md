# 10 - Metadata Strategy

## 10.1 Three pillars of metadata

| Loại | Mục đích | Người dùng | Lưu ở đâu |
|---|---|---|---|
| **Business Metadata** | Định nghĩa nghiệp vụ, glossary | Analyst, BI users | `data/warehouse/_meta/glossary.yaml` |
| **Technical Metadata** | Schema, mapping, lineage | Engineer | DuckDB `metadata` schema + auto-generated JSON |
| **Process Metadata** | Run history, performance | Ops | `data/warehouse/_meta/runs/{batch_id}.json` |

## 10.2 Business Metadata example

```yaml
# data/warehouse/_meta/glossary.yaml
fact_sales:
  description: "Granular line-item sales fact, one row per order line"
  grain: "Order line item"
  business_owner: "Sales VP"
  
  measures:
    net_amount:
      description: "Revenue after discount, before tax & freight"
      formula: "extended_price - discount_amount"
      unit: "USD"
      additivity: fully_additive
    
    freight_allocated:
      description: "Order-level freight allocated proportionally to lines"
      formula: "orders.Freight × (line_extended / order_total_extended)"
      additivity: fully_additive
      
dim_customer:
  description: "Customers buying products. SCD Type 2 on address attributes."
  business_owner: "CRM team"
  scd_type: 2
  source_systems: [northwind]
  
  attributes:
    company_name:
      description: "Legal/trading name of customer"
      scd: 2
    contact_name:
      description: "Primary contact person at customer"
      scd: 1
```

## 10.3 Technical Metadata schema (DuckDB)

```sql
CREATE SCHEMA metadata;

CREATE TABLE metadata.tables (
  table_name VARCHAR PRIMARY KEY,
  table_type VARCHAR,           -- dim, fact, agg, bridge
  grain VARCHAR,
  scd_type INT,                 -- NULL for fact
  row_count BIGINT,
  last_updated TIMESTAMP,
  source_systems VARCHAR[]
);

CREATE TABLE metadata.columns (
  table_name VARCHAR,
  column_name VARCHAR,
  data_type VARCHAR,
  is_nullable BOOLEAN,
  is_pk BOOLEAN,
  is_fk BOOLEAN,
  fk_target VARCHAR,
  description VARCHAR,
  PRIMARY KEY (table_name, column_name)
);

CREATE TABLE metadata.lineage (
  target_table VARCHAR,
  target_column VARCHAR,
  source_system VARCHAR,
  source_table VARCHAR,
  source_column VARCHAR,
  transformation VARCHAR
);

CREATE TABLE metadata.runs (
  batch_id VARCHAR PRIMARY KEY,
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  status VARCHAR,                -- RUNNING, SUCCESS, FAILED
  rows_extracted BIGINT,
  rows_rejected BIGINT,
  rows_loaded BIGINT,
  error_summary VARCHAR
);
```

## 10.4 Process Metadata (per run)

```json
// data/warehouse/_meta/runs/2024-06-25-103015.json
{
  "batch_id": "etl-2024-06-25-103015",
  "started_at": "2024-06-25T10:30:15Z",
  "ended_at":   "2024-06-25T10:33:42Z",
  "status": "SUCCESS",
  "phases": {
    "extract": {
      "duration_sec": 12.4,
      "sources": {
        "northwind":     { "files": 8, "rows": 3150, "bytes": 234567 },
        "countries":     { "files": 1, "rows": 250,  "bytes": 89234 },
        "exchange_rate": { "files": 1, "rows": 1,    "bytes": 4321 }
      }
    },
    "clean": {
      "duration_sec": 5.8,
      "errors": { "FATAL": 0, "ERROR": 3, "WARN": 12 },
      "rows_quarantined": 3
    },
    "conform": {
      "duration_sec": 3.2,
      "duplicates_found": 2,
      "clusters": 89
    },
    "deliver": {
      "duration_sec": 8.5,
      "tables_loaded": {
        "dim_customer": { "inserts": 91, "updates": 0 },
        "dim_product":  { "inserts": 80, "updates": 0 },
        "fact_sales":   { "inserts": 2148 }
      }
    }
  },
  "anomalies": [
    { "phase": "clean", "table": "customers", "screen": "postal_country_consistency", "count": 2 }
  ]
}
```

## 10.5 Lineage tracking

Cho mỗi cột trong warehouse, có thể trace ngược:

```python
def get_lineage(table: str, column: str) -> list[dict]:
    """
    Returns: [
      { "step": "extract", "source": "northwind/customers.csv", "column": "Country" },
      { "step": "standardize", "rule": "country_aliases.USA → US" },
      { "step": "lookup", "source": "countries.cca2" },
      { "step": "deliver", "table": "dim_customer", "column": "country_code" }
    ]
    """
```

## 10.6 Auto-generated documentation

Sau mỗi run thành công, tự động sinh:

```
data/warehouse/_meta/
├── glossary.yaml             (business)
├── schema.json               (technical, auto)
├── lineage.json              (technical, auto)
├── runs/
│   └── {batch_id}.json       (process)
└── data_quality_report.md    (auto, latest run)
```

## 10.7 Quy tắc cho mọi module

Mỗi module ETL khi chạy phải:

1. **Đọc** `batch_id` từ context.
2. **Ghi log** với prefix `[batch=...]`.
3. **Update** `metadata.runs` table với progress.
4. **Emit lineage events** vào `data/warehouse/_meta/lineage_events.jsonl`.

Module common cung cấp helper:

```python
from src.common.metadata import MetadataContext

ctx = MetadataContext.current()
ctx.log_phase_start("clean")
ctx.record_lineage(target="dim_customer.country_code", source="northwind/customers.Country", rule="country_alias")
ctx.log_phase_end("clean", rows_in=3150, rows_out=3147)
```
