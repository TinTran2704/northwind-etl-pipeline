# 06 - Clean Phase (Data Quality Screens)

## 6.1 Triết lý

> "Hệ thống ETL phải ưu tiên **báo cáo và cảnh báo** lỗi, thay vì tự ý điều chỉnh dữ liệu."

Mỗi violation → ghi vào `error_event_fact` table. Không bao giờ silently drop.

## 6.2 Module structure (`src/clean/`)

```
src/clean/
├── __init__.py
├── screens/
│   ├── base_screen.py
│   ├── column_property_screen.py     # Subsystem #4
│   ├── structure_screen.py           # Subsystem #4
│   ├── data_rule_screen.py           # Subsystem #4
│   └── reasonability_screen.py       # Subsystem #4
├── error_event_logger.py             # Subsystem #5
├── audit_dimension_builder.py        # Subsystem #6
└── pipeline.py                       # Orchestrator
```

## 6.3 Screen hierarchy

| Cấp | Screen Type | Phạm vi | Ví dụ |
|---|---|---|---|
| 1 | Column Property | Single column | NULL check, length check, value in domain |
| 2 | Structure | Cross-table | Referential integrity (FK exists) |
| 3 | Data & Value Rule | Cross-column | postal_code ↔ city consistency |
| 4 | Reasonability | Statistical | Row count vs baseline, distribution drift |

## 6.4 BaseScreen interface

```python
@dataclass
class ScreenResult:
    screen_name: str
    severity: Literal["INFO", "WARN", "ERROR", "FATAL"]
    record_id: Any           # PK của row vi phạm
    column_name: Optional[str]
    expected: Optional[str]
    actual: Optional[str]
    message: str

class BaseScreen(ABC):
    name: str
    severity: str            # default severity
    
    @abstractmethod
    def check(self, df: pd.DataFrame) -> List[ScreenResult]: ...
```

## 6.5 Quy tắc cấu hình (`config/quality_rules.yaml`)

```yaml
screens:
  customers:
    column_property:
      - { column: CustomerID, rule: not_null, severity: FATAL }
      - { column: CustomerID, rule: max_length, value: 5, severity: ERROR }
      - { column: CompanyName, rule: not_null, severity: ERROR }
      - { column: Country, rule: in_list, source: dim_geography.country_name, severity: WARN }
      - { column: PostalCode, rule: regex, pattern: '^[A-Za-z0-9 \-]+$', severity: WARN }
    
    data_rule:
      - name: postal_country_consistency
        condition: "postal_code matches pattern of country"
        severity: WARN
    
    reasonability:
      - { metric: row_count, baseline: 91, tolerance_pct: 20, severity: WARN }

  orders:
    column_property:
      - { column: OrderID, rule: not_null, severity: FATAL }
      - { column: OrderID, rule: unique, severity: FATAL }
      - { column: OrderDate, rule: date_range, min: '1990-01-01', max: 'today', severity: ERROR }
      - { column: Freight, rule: numeric_range, min: 0, max: 99999, severity: WARN }
    
    structure:
      - { column: CustomerID, references: customers.CustomerID, severity: ERROR }
      - { column: EmployeeID, references: employees.EmployeeID, severity: ERROR }

  order-details:
    column_property:
      - { column: Quantity, rule: numeric_range, min: 1, max: 1000, severity: ERROR }
      - { column: UnitPrice, rule: numeric_range, min: 0, max: 99999, severity: ERROR }
      - { column: Discount, rule: numeric_range, min: 0.0, max: 1.0, severity: ERROR }
```

## 6.6 Severity policy

| Severity | Hành vi |
|---|---|
| `INFO` | Log, không block |
| `WARN` | Log, ghi error_event, vẫn pass row downstream với `audit_dim.has_anomalies = True` |
| `ERROR` | Log, ghi error_event, **quarantine** row vào `data/error/quarantine/` |
| `FATAL` | Log, ghi error_event, **dừng cả batch** |

## 6.7 error_event_fact table schema

```
error_event_fact (lưu tại data/error/error_events.parquet)
+----------------------+-----------+
| Column               | Type      |
+----------------------+-----------+
| error_event_id       | BIGINT PK |
| etl_batch_id         | VARCHAR   |
| event_timestamp      | TIMESTAMP |
| source_system        | VARCHAR   |
| source_table         | VARCHAR   |
| source_record_pk     | VARCHAR   |  (stringified PK of offending row)
| screen_name          | VARCHAR   |
| screen_severity      | VARCHAR   |
| column_name          | VARCHAR   |  NULLable (structure screens không có 1 column)
| expected_value       | VARCHAR   |
| actual_value         | VARCHAR   |
| message              | VARCHAR   |
+----------------------+-----------+
```

## 6.8 Audit Dimension construction

Sau khi chạy hết tất cả screens cho 1 batch:

```python
def build_audit_record(batch_id, source, screens_results):
    total_rows = ...
    rejected = sum(1 for r in screens_results if r.severity in ("ERROR", "FATAL"))
    warned   = sum(1 for r in screens_results if r.severity == "WARN")
    
    quality_score = 1 - (rejected + warned * 0.3) / max(total_rows, 1)
    
    return AuditRecord(
        etl_batch_id=batch_id,
        source_system=source,
        extract_row_count=total_rows,
        reject_row_count=rejected,
        quality_score=round(quality_score, 2),
        has_anomalies=(rejected + warned) > 0,
        ...
    )
```

Mọi fact row trong batch này → trỏ về `audit_sk` của bản ghi vừa tạo.

## 6.9 Pipeline orchestration

```python
def clean_phase(batch_id: str, raw_dir: Path) -> CleanResult:
    audit_records = []
    
    for entity in ["customers", "orders", "order-details", "products", ...]:
        df = read_csv(raw_dir / f"{entity}.csv")
        all_violations = []
        
        # Run screens in order
        for screen_class in [ColumnPropertyScreen, StructureScreen, DataRuleScreen, ReasonabilityScreen]:
            screen = screen_class.from_config(entity)
            violations = screen.check(df)
            all_violations.extend(violations)
        
        # Write error events
        ErrorEventLogger.persist(all_violations, batch_id)
        
        # Quarantine ERROR-severity rows
        quarantine_rows(df, all_violations)
        
        # Build audit record
        audit = AuditDimensionBuilder.build(batch_id, entity, df, all_violations)
        audit_records.append(audit)
        
        # Persist cleaned df (only rows that passed)
        clean_df = filter_quarantined(df, all_violations)
        clean_df.to_parquet(staging_dir / f"cleaned_{entity}.parquet")
    
    return CleanResult(audit_records=audit_records)
```

## 6.10 Test cases

```python
def test_column_nullity_catches_null_pk(): ...
def test_referential_integrity_catches_orphan_order(): ...
def test_postal_code_format_screen(): ...
def test_reasonability_row_count_drop(): ...
def test_fatal_severity_stops_batch(): ...
def test_warn_severity_passes_row_with_anomaly_flag(): ...
def test_audit_quality_score_calculation(): ...
def test_quarantine_isolates_only_error_rows(): ...
```
