# 08 - Deliver Phase

## 8.1 Mục tiêu

- Build các Dimension tables (SCD Type 1, 2, 3)
- Build Fact tables qua **Surrogate Key Pipeline**
- Xử lý **Late-arriving data**
- Build **Aggregate tables** + **OLAP cubes**

## 8.2 Module structure (`src/deliver/`)

```
src/deliver/
├── __init__.py
├── surrogate_key_generator.py    # Subsystem #10
├── scd_manager.py                # Subsystem #9
├── hierarchy_manager.py          # Subsystem #11
├── special_dimensions.py         # Subsystem #12 (junk, mini, role-playing)
├── fact_table_builder.py         # Subsystem #13
├── surrogate_key_pipeline.py     # Subsystem #14
├── bridge_table_builder.py       # Subsystem #15
├── late_arriving_handler.py      # Subsystem #16
├── aggregate_builder.py          # Subsystem #19
├── olap_cube_builder.py          # Subsystem #20 (optional)
└── pipeline.py
```

## 8.3 Surrogate Key Generator (Subsystem #10)

```python
class SurrogateKeyGenerator:
    """
    Một sequence per dim. Lưu state trong data/warehouse/_meta/sk_sequences.json
    """
    
    def next_sk(self, dim_name: str) -> int:
        seq = self.load_sequences()
        seq[dim_name] = seq.get(dim_name, 0) + 1
        self.save_sequences(seq)
        return seq[dim_name]
    
    def reserve_unknown(self, dim_name: str) -> int:
        """Pre-allocate sk = -1 cho 'Unknown' member."""
        return -1
```

**Quy tắc:**
- SK bắt đầu từ 1 (không dùng 0).
- SK = -1 reserved cho "Unknown" member (mỗi dim có 1 row Unknown).
- Không bao giờ reuse SK đã expired.

## 8.4 SCD Manager (Subsystem #9)

### SCD Type 2 algorithm

```python
def apply_scd_type_2(
    new_rows: pd.DataFrame,        # Conformed golden records
    existing_dim: pd.DataFrame,    # Current dim_customer
    type2_columns: list[str],      # ['address', 'city', 'country_code', ...]
    type1_columns: list[str],      # ['contact_name', 'phone']
    effective_date: date
) -> pd.DataFrame:
    """
    Returns updated dim with:
      - Unchanged rows: as-is
      - Changed Type-1 attrs: in-place UPDATE both current and historical
      - Changed Type-2 attrs: 
          * UPDATE old current: expiration_date = effective_date - 1, is_current = False
          * INSERT new current with effective_date = effective_date
      - New NKs: INSERT with new SK
      - Missing NKs (deleted at source): mark is_current = False, expiration = today
                                          (or keep with deleted_flag = True)
    """
```

### Logic chi tiết

```
For each row R in new_rows (matched by NK to current row C in existing_dim):
  
  if no current C exists for R.nk:
    → INSERT new row with new SK, effective_date = today, expiration = NULL
  
  else:
    diff_type2 = any(R[col] != C[col] for col in type2_columns)
    diff_type1 = any(R[col] != C[col] for col in type1_columns)
    
    if diff_type2:
      → UPDATE C: expiration_date = today - 1, is_current = False
      → INSERT new row R' with new SK, effective_date = today
      → If diff_type1 also: apply type-1 update on R' before insert
      → Apply type-1 also on all historical rows for same NK
    
    elif diff_type1:
      → UPDATE all rows with same NK: set type-1 cols to new values
      
    else:
      → No change
```

## 8.5 Surrogate Key Pipeline (Subsystem #14) — Quan trọng nhất!

Khi build fact_sales, mỗi natural key trong source phải được resolve về **đúng SK ứng với thời điểm event**:

```python
def resolve_customer_sk(customer_nk: str, event_date: date, dim_customer: pd.DataFrame) -> int:
    """
    Point-in-time lookup.
    """
    candidates = dim_customer[
        (dim_customer.customer_nk == customer_nk) &
        (dim_customer.effective_date <= event_date) &
        (
            (dim_customer.expiration_date.isna()) |
            (dim_customer.expiration_date >= event_date)
        )
    ]
    
    if len(candidates) == 0:
        return -1   # Unknown member
    if len(candidates) > 1:
        raise SKPipelineError(f"Overlapping SCD2 windows for {customer_nk}")
    
    return candidates.iloc[0].customer_sk
```

### Pipeline flow

```
Order line raw → 
  apply: order_date_sk    = lookup_dim_date(order_date)
  apply: customer_sk      = resolve_customer_sk(CustomerID, order_date)
  apply: employee_sk      = resolve_employee_sk(EmployeeID, order_date)
  apply: product_sk       = resolve_product_sk(ProductID, order_date)
  apply: shipper_sk       = resolve_shipper_sk(ShipVia, order_date)   # Type 1, no date logic
  apply: ship_geo_sk      = lookup_geography(ShipCountry)
  apply: audit_sk         = current_batch_audit_sk
  
  compute: extended_price = quantity * unit_price
  compute: discount_amount= extended_price * discount
  compute: net_amount     = extended_price - discount_amount
  compute: freight_alloc  = orders.Freight * (line_extended / order_total_extended)
  
  → fact_sales row
```

## 8.6 Late-arriving Data Handler (Subsystem #16)

### Late-arriving Fact

```python
def handle_late_arriving_fact(fact_row, dim_customer):
    """
    Fact đến sau, nhưng dim đã có đủ history → 
    Surrogate Key Pipeline tự nhiên tìm đúng SK theo order_date của fact.
    KHÔNG cần xử lý đặc biệt nếu SCD2 đã đúng.
    """
    return resolve_customer_sk(fact_row.customer_nk, fact_row.order_date, dim_customer)
```

### Late-arriving Dimension

Khó hơn nhiều. Phát hiện thay đổi của dim **lùi về quá khứ**.

```python
def handle_late_arriving_dimension(
    nk: str,
    new_attributes: dict,
    actual_effective_date: date,   # Quá khứ
    dim_customer: pd.DataFrame,
    fact_sales: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    1. Find existing row that covers actual_effective_date
       → split it into 2 parts:
       Part A: keep with expiration = actual_effective_date - 1
       Part B: new row with new attributes, effective = actual_effective_date,
               expiration = old expiration of original row
    2. UPDATE fact_sales rows where:
        customer_nk = nk AND
        order_date >= actual_effective_date AND order_date <= old_expiration
       → set customer_sk to new_sk (destructive update!)
    """
```

⚠️ Đây là **destructive update** của fact table — có thể vi phạm immutability nếu ràng buộc nghiêm ngặt. Cần log mỗi destructive update vào `data/error/destructive_updates.log`.

## 8.7 Bridge Table Builder (Subsystem #15)

Northwind không có many-to-many phức tạp, nhưng để demo:

**Use case**: Một order có thể có nhiều "tags" (express, gift, fragile…).

```
bridge_order_tags
+---------+----------+-------------+
| order_id| tag_sk   | weight      |
+---------+----------+-------------+
| 10248   | 1 (express)| 1.0       |
| 10248   | 2 (gift) | 1.0         |
+---------+----------+-------------+
```

`weight` dùng cho ragged hierarchies / multi-valued dimensions để tránh double-counting.

## 8.8 Aggregate Builder (Subsystem #19)

```python
def build_agg_sales_monthly(fact_sales, dim_date, dim_product, dim_customer) -> pd.DataFrame:
    """
    Pre-compute monthly totals.
    """
    df = fact_sales.merge(dim_date, on='order_date_sk') \
                   .merge(dim_product[['product_sk', 'category_name']], on='product_sk') \
                   .merge(dim_customer[['customer_sk', 'country_code']], on='customer_sk')
    
    agg = df.groupby(['year', 'month', 'product_sk', 'country_code']).agg(
        total_quantity=('quantity', 'sum'),
        total_net_amount=('net_amount', 'sum'),
        order_count=('order_id', 'nunique')
    ).reset_index()
    
    agg['year_month'] = agg['year'] * 100 + agg['month']
    return agg
```

### Aggregate Navigator (concept)

```python
def route_query(query):
    if query.grain == 'month' and 'product_sk' in query.dims:
        return query_against(agg_sales_monthly)   # ~1500 rows
    else:
        return query_against(fact_sales)          # ~2150 rows
```

## 8.9 OLAP Cube (Subsystem #20)

Optional: dùng `duckdb` để export cube file:

```sql
CREATE TABLE cube_sales AS
SELECT
  d.year, d.quarter, d.month,
  p.category_name,
  c.country_code,
  SUM(f.net_amount) AS revenue,
  SUM(f.quantity)   AS units,
  COUNT(DISTINCT f.order_id) AS orders
FROM fact_sales f
JOIN dim_date d     ON f.order_date_sk = d.date_sk
JOIN dim_product p  ON f.product_sk    = p.product_sk
JOIN dim_customer c ON f.customer_sk   = c.customer_sk
GROUP BY GROUPING SETS (
  (d.year),
  (d.year, d.quarter),
  (d.year, d.quarter, d.month),
  (d.year, p.category_name),
  (d.year, c.country_code),
  (d.year, p.category_name, c.country_code)
);
```

## 8.10 Test cases

```python
def test_sk_generator_increments(): ...
def test_sk_unknown_member_is_negative_one(): ...
def test_scd2_type2_change_creates_new_row(): ...
def test_scd2_type1_change_updates_in_place(): ...
def test_scd2_no_overlap_periods(): ...
def test_sk_pipeline_picks_correct_version_by_date(): ...
def test_sk_pipeline_unknown_returns_minus_one(): ...
def test_late_arriving_fact_routes_to_correct_dim_version(): ...
def test_late_arriving_dim_splits_existing_row(): ...
def test_late_arriving_dim_updates_fact_rows(): ...
def test_aggregate_sum_matches_fact(): ...
def test_freight_allocation_sums_to_order_freight(): ...
```
