# 02 - Nguồn dữ liệu (Data Sources)

> Tất cả nguồn đều **public, không cần API key, không cần authentication**, có thể lấy bằng `requests.get()`.
> Các URL trong file này đã được **verify tồn tại tại thời điểm khởi tạo dự án** (2026-05).

## 2.1 Source 1: Northwind CSV (Primary OLTP) — Neo4j-contrib mirror

**Vì sao chọn nguồn này**: Repo `neo4j-contrib/northwind-neo4j` ổn định lâu dài, dùng dấu gạch ngang nhất quán (`order-details.csv`), được Neo4j community maintain.

**Base URL:**
```
https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data
```

**Các file đã verify tồn tại:**

| File | Endpoint | Vai trò trong DW |
|---|---|---|
| `customers.csv` | `/customers.csv` | → `dim_customer` (SCD Type 2) |
| `orders.csv` | `/orders.csv` | → `fact_sales` (header level) |
| `order-details.csv` | `/order-details.csv` | → `fact_sales` (line-item grain) |
| `products.csv` | `/products.csv` | → `dim_product` (SCD Type 2) |
| `categories.csv` | `/categories.csv` | → `dim_product` (denormalized) |
| `suppliers.csv` | `/suppliers.csv` | → `dim_product` (denormalized) |
| `employees.csv` | `/employees.csv` | → `dim_employee` (SCD Type 2) |
| `territories.csv` | `/territories.csv` | → `dim_territory` (optional) |
| `employee-territories.csv` | `/employee-territories.csv` | → `bridge_employee_territory` (many-to-many) |

⚠️ **Lưu ý**: Repo này **KHÔNG có** `shippers.csv` và `regions.csv`. Có 2 lựa chọn:

**Option A (đề xuất)**: Tạo seed file local cho shippers và regions (hard-code 3 shippers + 4 regions vào `data/seed/`). Phù hợp cho MVP.

**Option B**: Lấy từ nguồn dự phòng `graphql-compose-examples`:
```
https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/shippers.csv
https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/regions.csv
```
⚠️ Nguồn này hoạt động nhưng có thể lưu tên file dạng `order_details.csv` (gạch dưới) — **chỉ dùng cho shippers/regions**.

## 2.2 Source 2: REST Countries (Geography Reference)

**URL:**
```
https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,currencies,capital,languages
```

**Mục đích:**
- Bổ sung `dim_geography` — mapping ISO 3166-1 alpha-2 → region/subregion.
- Tạo Conformed Dimension cho cả Northwind customers và data sources khác.

**Đặc điểm:**
- Format: JSON array of objects
- Không pagination — lấy 1 lần được toàn bộ.
- API public miễn phí, không cần key, nhưng thỉnh thoảng có downtime → **bắt buộc** snapshot fallback file (`data/seed/countries.json`).

**Pseudo-schema:**
```json
{
  "name": { "common": "Vietnam", "official": "Socialist Republic of Vietnam" },
  "cca2": "VN",
  "cca3": "VNM",
  "region": "Asia",
  "subregion": "South-Eastern Asia",
  "currencies": { "VND": { "name": "Vietnamese đồng", "symbol": "₫" } }
}
```

## 2.3 Source 3: Exchange Rate API (Currency Conformance)

**URL:**
```
https://open.er-api.com/v6/latest/USD
```

**Mục đích:**
- Mô phỏng việc chuẩn hóa độ đo tiền tệ (Conformed Fact).
- Northwind chứa giá USD; nếu mở rộng nhiều quốc gia → convert.
- Demo việc gọi API thường xuyên → cơ hội test CDC theo timestamp.

**Response sample:**
```json
{
  "result": "success",
  "time_last_update_unix": 1719360000,
  "base_code": "USD",
  "rates": { "EUR": 0.93, "VND": 25400, "JPY": 158.2 }
}
```

API này không cần key, có giới hạn ~1500 req/month/IP — đủ cho dự án học.

## 2.4 Source 4: Synthetic Dirty Data (cho test Data Quality)

Module `src/extract/dirty_data_generator.py` sẽ inject các lỗi vào staging để test screens:

| Loại lỗi | Cách inject | Screen kỳ vọng bắt được |
|---|---|---|
| NULL trong cột NOT NULL | Random 5% rows có `customer_id = NULL` | Column Nullity Screen |
| Duplicate primary key | Clone 2% rows với cùng PK | Structure Screen |
| Format sai (postal code) | Postal code chứa chữ cái | Column Property Screen |
| Mâu thuẫn city/postal | London + 90210 | Data Rule Screen (cross-column) |
| Outlier số học | UnitPrice = -50 hoặc 99999999 | Reasonability Check |
| Late-arriving | Order với customer_id chưa tồn tại | Late-arriving handler |

## 2.5 Cấu hình nguồn (`config/sources.yaml`)

```yaml
sources:
  northwind:
    type: http_csv
    base_url: https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data
    files:
      - { name: customers,            entity: customer    }
      - { name: orders,               entity: order       }
      - { name: order-details,        entity: order_line  }
      - { name: products,             entity: product     }
      - { name: categories,           entity: category    }
      - { name: suppliers,            entity: supplier    }
      - { name: employees,            entity: employee    }
      - { name: territories,          entity: territory   }
      - { name: employee-territories, entity: emp_terr    }
    cdc_strategy: full_load
    schedule: daily

  northwind_extras:
    type: http_csv
    base_url: https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv
    files:
      - { name: shippers,  entity: shipper, optional: true }
      - { name: regions,   entity: region,  optional: true }
    cdc_strategy: full_load
    schedule: daily
    on_404: skip

  countries:
    type: rest_json
    url: https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,currencies
    cdc_strategy: full_load
    schedule: weekly
    fallback_file: data/seed/countries.json

  exchange_rate:
    type: rest_json
    url: https://open.er-api.com/v6/latest/USD
    cdc_strategy: timestamp
    schedule: hourly
```

## 2.6 Verification script

Trước khi bắt đầu code, chạy `scripts/verify_sources.py`:

```python
import requests

URLS = [
    # Primary Northwind
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/customers.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/orders.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/order-details.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/products.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/categories.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/suppliers.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/employees.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/territories.csv",
    "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/employee-territories.csv",
    # JSON APIs
    "https://restcountries.com/v3.1/all?fields=name,cca2",
    "https://open.er-api.com/v6/latest/USD",
]

for url in URLS:
    try:
        r = requests.head(url, allow_redirects=True, timeout=10)
        status = "OK" if r.status_code == 200 else f"FAIL ({r.status_code})"
    except Exception as e:
        status = f"ERROR ({e})"
    print(f"{status:15s} {url}")
```

Checklist:
- [ ] Mỗi URL trả 200
- [ ] CSV có header đầy đủ
- [ ] JSON parse được, có schema kỳ vọng
- [ ] Snapshot vào `data/seed/` thành công (xem 2.8)
- [ ] Nếu API fail → dùng fallback file

## 2.7 Lý do KHÔNG chọn các nguồn khác

| Nguồn | Lý do loại |
|---|---|
| Kaggle Northwind | Cần authentication / cookie |
| MSSQL sample DB | Cần `.bak` file, không phải HTTP đơn giản |
| Microsoft SQL Server samples GitHub | Dùng PostgreSQL `.sql` script — phức tạp hơn CSV |
| harryho/db-samples | Tên cột bị đổi (singular: `Customer` thay vì `Customers`) — sai schema gốc |
| public APIs có rate limit thấp | Dễ fail trong test |

## 2.8 Snapshot dự phòng (BẮT BUỘC làm trước khi code)

Để tránh phụ thuộc network/source biến mất, **tải sẵn** snapshot vào `data/seed/`:

```bash
mkdir -p data/seed/northwind data/seed/countries

# Northwind core
cd data/seed/northwind
for f in customers orders order-details products categories suppliers employees territories employee-territories; do
  curl -sSLo "${f}.csv" "https://raw.githubusercontent.com/neo4j-contrib/northwind-neo4j/master/data/${f}.csv"
  echo "Downloaded ${f}.csv ($(wc -l < ${f}.csv) lines)"
done

# Optional: shippers + regions từ nguồn 2 (có thể fail, không sao)
curl -sSLo "shippers.csv" "https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/shippers.csv" || echo "shippers.csv unavailable"

# Countries
cd ../countries
curl -sSLo "countries.json" "https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,currencies"
```

**Add `data/seed/` vào Git** nếu kích thước cho phép (Northwind tổng < 200KB, countries < 50KB) — đây là **golden snapshot** đảm bảo dự án reproducible.

`.gitignore`:
```gitignore
# Ignore live data
data/raw/
data/staging/
data/warehouse/
data/error/

# KEEP seed (golden snapshot)
!data/seed/
```
