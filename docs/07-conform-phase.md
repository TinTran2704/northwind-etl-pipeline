# 07 - Conform Phase

## 7.1 Mục tiêu

- Tạo **Conformed Dimensions** để cho phép drill-across nhiều quy trình.
- Chuẩn hóa, khử trùng lặp (deduplication), chọn bản ghi tối ưu (survivorship).
- Output: golden records sẵn sàng cho Deliver.

## 7.2 Module structure (`src/conform/`)

```
src/conform/
├── __init__.py
├── standardizer.py         # Subsystem #21 phần a
├── deduplicator.py         # Subsystem #7
├── survivor_selector.py    # Subsystem #8
├── conformed_dim_builder.py # Subsystem #21 phần b
└── pipeline.py
```

## 7.3 Bước 1: Standardize

Map các giá trị đa dạng về domain chung.

### 7.3.1 Country name → ISO code

Northwind có `Country = "USA"`, `"UK"`, `"Switzerland"` (string tự do).
REST Countries chỉ chấp nhận common name. Cần lookup table:

```yaml
# config/standardization/country_aliases.yaml
country_aliases:
  USA: United States
  UK: United Kingdom
  Brasil: Brazil
  Holland: Netherlands
  Schweiz: Switzerland
```

Sau standardize: mọi `Country` → `country_code (ISO 3166-1 alpha-2)`.

### 7.3.2 Phone format

```
"(503) 555-7555"  → +1-503-555-7555
"030-0074321"     → +49-30-0074321  (lookup từ country)
```

### 7.3.3 Title case cho tên/địa chỉ

```
"london"  → "London"
"BERLIN"  → "Berlin"
```

### 7.3.4 Currency

Chuẩn hóa currency của fact_sales về USD (hoặc giữ nguyên + thêm cột conformed_amount_usd).

## 7.4 Bước 2: Deduplicate

### Use case: Cùng customer có thể có 2 record nếu nhập 2 lần.

Vì Northwind là 1 OLTP nên ít trùng. Để demo, ta inject duplicates bằng `dirty_data_generator.py`.

### Thuật toán matching

```python
class CustomerMatcher:
    """
    Probabilistic matching dựa trên fuzzy score.
    """
    
    def match_score(self, a: dict, b: dict) -> float:
        """
        Score 0.0 - 1.0
        """
        score = 0.0
        weights = {
            "company_name": 0.40,
            "phone":        0.20,
            "address":      0.20,
            "city":         0.10,
            "country":      0.10,
        }
        
        for field, weight in weights.items():
            sim = string_similarity(a.get(field), b.get(field))  # Jaro-Winkler
            score += weight * sim
        
        return score
    
    def is_match(self, a, b, threshold=0.85) -> bool:
        return self.match_score(a, b) >= threshold
```

### Output:

```
data/staging/conform/customer_clusters.parquet
+----------------+--------------+
| cluster_id     | customer_nk  |
+----------------+--------------+
| cluster_001    | ALFKI        |
| cluster_001    | ALFKI_DUP    |   ← duplicate detected
| cluster_002    | ANATR        |
+----------------+--------------+
```

## 7.5 Bước 3: Survivorship

Khi có nhiều bản ghi trong cùng cluster, chọn giá trị nào "sống sót":

```yaml
# config/survivorship_rules.yaml
customer:
  company_name:
    rule: longest_non_null
  contact_name:
    rule: most_recent           # by extract date
  phone:
    rule: prefer_source
    priority: [northwind, crm, support]
  address:
    rule: prefer_source
    priority: [accounting, sales]
  email:
    rule: most_complete         # has @, has domain
```

### Output: Golden Record

```
data/staging/conform/customer_golden.parquet
+--------------+--------------+--------------+----------+--------------------+-----------+
| cluster_id   | customer_nk  | company_name | city     | country_code (conf)| phone     |
+--------------+--------------+--------------+----------+--------------------+-----------+
| cluster_001  | ALFKI        | Alfreds...   | Berlin   | DE                 | +49-30-...|
+--------------+--------------+--------------+----------+--------------------+-----------+
```

`customer_nk` được giữ là natural key chính (chọn theo rule = `min(customer_nk)` trong cluster).

## 7.6 Conformed Dimension publishing

### Dimension Manager (Subsystem #17)

Là module trung tâm publish các conformed dim ra cho mọi fact provider:

```python
class DimensionManager:
    def publish_dimension(self, dim_name: str, df: pd.DataFrame, version: str):
        """
        1. Validate (PK unique, no nulls in required cols)
        2. Versioning: tag với version + timestamp
        3. Lưu vào /data/staging/conform/published/{dim_name}/v{version}/
        4. Notify subscribers (fact providers)
        """
```

### Fact Provider (Subsystem #18)

Subscriber phía fact, dùng conformed dim để lookup surrogate keys:

```python
class FactProvider:
    def consume_dimension(self, dim_name: str) -> pd.DataFrame:
        latest = self.dimension_manager.get_latest(dim_name)
        return latest
```

## 7.7 Quy tắc conformance kiểm tra

Sau khi build xong các conformed dim, chạy QA suite:

| Check | Mô tả |
|---|---|
| PK uniqueness | `customer_sk` unique trong toàn dim |
| NK consistency | 1 `customer_nk` chỉ thuộc 1 `cluster_id` |
| FK closure | Mọi `country_code` trong dim_customer có trong dim_geography |
| Type 2 invariant | Mỗi NK có đúng 1 row với `is_current = True` |
| No date overlap | Cùng NK không có 2 row có khoảng `[effective, expiration]` overlap |

## 7.8 Test cases

```python
def test_country_alias_resolves_USA_to_US(): ...
def test_phone_normalization_adds_country_code(): ...
def test_matcher_links_minor_typo_companies(): ...
def test_matcher_does_not_link_different_companies(): ...
def test_survivorship_picks_longest_company_name(): ...
def test_survivorship_priority_source_wins(): ...
def test_dimension_manager_versioning(): ...
def test_no_overlap_on_scd2_periods(): ...
```
