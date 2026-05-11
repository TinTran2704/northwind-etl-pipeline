# 03 - Logical Data Map (Source-to-Target Mapping)

> Tài liệu này xác định ánh xạ chính xác **từ cột nguồn → cột đích**, kèm quy tắc biến đổi. Đây là "bản hợp đồng" giữa Extract và Deliver.

## 3.1 Format mapping table

Mỗi mapping có các trường:

| Field | Mô tả |
|---|---|
| Target Table | Tên bảng đích (dim_*, fact_*) |
| Target Column | Tên cột đích |
| Target Type | Kiểu dữ liệu đích |
| Source System | northwind / countries / exchange_rate |
| Source Table | Tên file/endpoint |
| Source Column | Tên cột nguồn |
| Transformation | Quy tắc biến đổi |
| SCD Type | 1 / 2 / 3 / N/A |

## 3.2 dim_customer (SCD Type 2)

| Target Column | Type | Source | Source Col | Transform | SCD |
|---|---|---|---|---|---|
| customer_sk | BIGINT PK | — | — | Auto-generated surrogate | — |
| customer_nk | VARCHAR(10) | northwind.customers | CustomerID | TRIM, UPPER | — |
| company_name | VARCHAR(80) | northwind.customers | CompanyName | TRIM | 2 |
| contact_name | VARCHAR(50) | northwind.customers | ContactName | TRIM, NULL if empty | 1 |
| contact_title | VARCHAR(50) | northwind.customers | ContactTitle | TRIM | 1 |
| address | VARCHAR(120) | northwind.customers | Address | TRIM | 2 |
| city | VARCHAR(40) | northwind.customers | City | TRIM, Title Case | 2 |
| postal_code | VARCHAR(15) | northwind.customers | PostalCode | TRIM | 2 |
| country_code | CHAR(2) | derived | Country | Lookup countries.cca2 by country name | 2 |
| region_name | VARCHAR(40) | countries | region | Lookup by country_code | 2 |
| phone | VARCHAR(30) | northwind.customers | Phone | Strip non-digits, format | 1 |
| effective_date | DATE | system | — | First seen date / change date | — |
| expiration_date | DATE | system | — | NULL = current; else end of validity | — |
| is_current | BOOLEAN | system | — | True iff expiration_date IS NULL | — |
| audit_sk | BIGINT FK | dim_audit | — | FK → dim_audit | — |

**Notes:**
- `customer_nk` là natural key giữ nguyên; `customer_sk` là surrogate key dùng cho join.
- Khi `address`, `city`, `postal_code`, `country_code`, `region_name`, hoặc `company_name` thay đổi → tạo bản ghi mới (SCD Type 2).
- Khi `contact_name`, `contact_title`, `phone` thay đổi → overwrite (SCD Type 1).

## 3.3 dim_product (SCD Type 2)

| Target Column | Type | Source | Source Col | Transform | SCD |
|---|---|---|---|---|---|
| product_sk | BIGINT PK | — | — | Surrogate | — |
| product_nk | INT | products | ProductID | as is | — |
| product_name | VARCHAR(80) | products | ProductName | TRIM | 2 |
| category_name | VARCHAR(40) | categories | CategoryName | Lookup by CategoryID | 2 |
| supplier_name | VARCHAR(80) | suppliers | CompanyName | Lookup by SupplierID | 2 |
| supplier_country | CHAR(2) | suppliers + countries | Country | Lookup chain | 2 |
| quantity_per_unit | VARCHAR(40) | products | QuantityPerUnit | TRIM | 1 |
| unit_price | DECIMAL(10,2) | products | UnitPrice | as is, validate ≥ 0 | 2 |
| units_in_stock | INT | products | UnitsInStock | as is | 1 |
| discontinued | BOOLEAN | products | Discontinued | 1 → True, 0 → False | 2 |
| effective_date | DATE | system | — | — | — |
| expiration_date | DATE | system | — | — | — |
| is_current | BOOLEAN | system | — | — | — |
| audit_sk | BIGINT FK | — | — | — | — |

## 3.4 dim_employee (SCD Type 2)

| Target Column | Type | Source | Transform | SCD |
|---|---|---|---|---|
| employee_sk | BIGINT PK | — | Surrogate | — |
| employee_nk | INT | employees.EmployeeID | as is | — |
| full_name | VARCHAR(80) | employees | CONCAT(TitleOfCourtesy, FirstName, LastName) | 2 |
| title | VARCHAR(40) | employees.Title | TRIM | 2 |
| reports_to_nk | INT | employees.ReportsTo | as is (resolve to SK at delivery) | 2 |
| hire_date | DATE | employees.HireDate | parse | 1 |
| city | VARCHAR(40) | employees.City | TRIM | 2 |
| country_code | CHAR(2) | derived | Lookup | 2 |
| effective_date / expiration_date / is_current / audit_sk | — | — | — | — |

## 3.5 dim_geography (Conformed Dimension)

| Target Column | Type | Source | Transform |
|---|---|---|---|
| geography_sk | BIGINT PK | — | Surrogate |
| country_code | CHAR(2) | countries.cca2 | as is |
| country_name | VARCHAR(80) | countries.name.common | as is |
| region | VARCHAR(40) | countries.region | as is |
| subregion | VARCHAR(40) | countries.subregion | as is |
| primary_currency | CHAR(3) | countries.currencies | First key of currencies dict |

**Lưu ý**: Đây là **Conformed Dimension** — mọi `dim_customer`, `dim_employee`, `dim_product` đều dùng `country_code` để join về `dim_geography`.

## 3.6 dim_date (Static / Pre-built)

Generate trước cho dải `1996-01-01` → `2030-12-31`:

| Column | Type | Mô tả |
|---|---|---|
| date_sk | INT PK | YYYYMMDD format (e.g., 20240615) |
| full_date | DATE | — |
| day_of_week | INT | 0=Mon … 6=Sun |
| day_name | VARCHAR(10) | Monday, Tuesday… |
| day_of_month | INT | 1-31 |
| day_of_year | INT | 1-366 |
| week_of_year | INT | 1-53 |
| month | INT | 1-12 |
| month_name | VARCHAR(10) | January… |
| quarter | INT | 1-4 |
| year | INT | — |
| is_weekend | BOOLEAN | — |
| fiscal_year | INT | (Tùy chính sách công ty) |
| fiscal_quarter | INT | — |

## 3.7 dim_audit (Audit Dimension)

| Column | Type | Mô tả |
|---|---|---|
| audit_sk | BIGINT PK | Surrogate |
| etl_batch_id | VARCHAR(40) | UUID của lần chạy ETL |
| etl_run_timestamp | TIMESTAMP | — |
| source_system | VARCHAR(40) | northwind / countries / ... |
| source_file | VARCHAR(120) | Tên file nguồn |
| extract_row_count | INT | — |
| reject_row_count | INT | — |
| quality_score | DECIMAL(3,2) | 0.00 - 1.00 (1.0 = perfect) |
| has_anomalies | BOOLEAN | — |
| created_at | TIMESTAMP | — |

**Mọi fact table phải có FK tới `dim_audit`** — biến metadata thành data có thể lọc trong BI.

## 3.8 fact_sales (Transaction Grain)

**Grain**: Một dòng = một line-item của một order.

| Target Column | Type | Source | Transform |
|---|---|---|---|
| order_id | INT | order-details.OrderID | as is (degenerate dim) |
| line_number | INT | derived | Row number within order |
| order_date_sk | INT FK | orders.OrderDate | Lookup dim_date |
| required_date_sk | INT FK | orders.RequiredDate | Lookup dim_date |
| shipped_date_sk | INT FK | orders.ShippedDate | Lookup dim_date (NULL → 19000101 unknown) |
| customer_sk | BIGINT FK | orders.CustomerID | Surrogate Key Pipeline (point-in-time) |
| employee_sk | BIGINT FK | orders.EmployeeID | Surrogate Key Pipeline |
| product_sk | BIGINT FK | order-details.ProductID | Surrogate Key Pipeline |
| shipper_sk | BIGINT FK | orders.ShipVia | Surrogate Key Pipeline |
| ship_geography_sk | BIGINT FK | orders.ShipCountry | Lookup country_code → dim_geography |
| audit_sk | BIGINT FK | dim_audit | — |
| quantity | INT | order-details.Quantity | as is, validate > 0 |
| unit_price | DECIMAL(10,2) | order-details.UnitPrice | as is |
| discount | DECIMAL(4,3) | order-details.Discount | as is, validate 0 ≤ d ≤ 1 |
| extended_price | DECIMAL(12,2) | derived | quantity × unit_price |
| discount_amount | DECIMAL(12,2) | derived | extended_price × discount |
| net_amount | DECIMAL(12,2) | derived | extended_price - discount_amount |
| freight_allocated | DECIMAL(10,2) | derived | (orders.Freight × line_share) |

**Surrogate Key Pipeline**: `customer_sk` phải lookup theo `customer_nk` **AND** `effective_date ≤ order_date < expiration_date` (point-in-time).

## 3.9 agg_sales_monthly (Aggregate Table)

Pre-computed cho hiệu suất:

| Column | Source |
|---|---|
| year_month | YYYYMM từ order_date |
| product_sk | as is |
| customer_country_code | from dim_customer |
| total_quantity | SUM(quantity) |
| total_net_amount | SUM(net_amount) |
| order_count | COUNT(DISTINCT order_id) |
| audit_sk | latest of contributing rows |

**Aggregate Navigator** (sau này) sẽ tự route truy vấn về bảng này khi user query theo (month, product, country).
