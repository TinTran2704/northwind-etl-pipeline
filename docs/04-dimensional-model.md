# 04 - Dimensional Model (Star Schema)

## 4.1 Star Schema Diagram

```
                    ┌──────────────┐
                    │   dim_date   │
                    │ (date_sk PK) │
                    └──────┬───────┘
                           │
                           │ order_date_sk
                           │ required_date_sk
                           │ shipped_date_sk
                           │
   ┌────────────┐          ▼          ┌─────────────────┐
   │dim_customer│   ┌──────────────┐  │  dim_geography  │
   │            ├──▶│              │◀─┤ (geography_sk PK)│
   │(customer_sk│   │  fact_sales  │  └─────────────────┘
   │    PK)     │   │              │           ▲
   └────────────┘   │              │           │
                    │              │  ┌────────┴───────┐
   ┌────────────┐   │              │  │ ship_geography │
   │dim_product ├──▶│              │◀─┘    (FK)        │
   │            │   │              │
   │(product_sk │   │              │
   │    PK)     │   │              │
   └────────────┘   │              │
                    │              │
   ┌────────────┐   │              │   ┌────────────┐
   │dim_employee├──▶│              │◀──┤dim_shipper │
   │            │   │              │   │            │
   └────────────┘   │              │   └────────────┘
                    │              │
                    │              │   ┌────────────┐
                    │              │◀──┤ dim_audit  │
                    │              │   │            │
                    └──────────────┘   └────────────┘
```

## 4.2 Bảng tóm tắt các Dimension

| Dimension | SCD Type | Grain | Số hàng ước tính | Conformed? |
|---|---|---|---|---|
| dim_date | N/A | 1 ngày | ~12,800 | Yes |
| dim_customer | Type 2 | 1 phiên bản KH | ~100 base × ~3 versions | Yes |
| dim_product | Type 2 | 1 phiên bản SP | ~80 × ~2 versions | Yes |
| dim_employee | Type 2 | 1 phiên bản NV | ~10 × ~2 versions | Yes |
| dim_shipper | Type 1 | 1 shipper | ~3 | Yes |
| dim_geography | Type 1 | 1 quốc gia | ~250 | **Yes (conformed)** |
| dim_audit | Insert-only | 1 ETL batch | growing | Yes |

## 4.3 Fact Tables

| Fact | Grain | Type | Số hàng ước tính |
|---|---|---|---|
| fact_sales | Order line item | Transaction | ~2,150 |
| agg_sales_monthly | Month × Product × Country | Periodic Snapshot | ~1,500 |

## 4.4 Quy tắc đặt tên (Naming Conventions)

| Element | Pattern | Example |
|---|---|---|
| Dimension table | `dim_<noun>` | `dim_customer` |
| Fact table | `fact_<process>` | `fact_sales` |
| Aggregate | `agg_<fact>_<grain>` | `agg_sales_monthly` |
| Surrogate key | `<entity>_sk` | `customer_sk` |
| Natural key | `<entity>_nk` | `customer_nk` |
| Bridge table | `bridge_<a>_<b>` | `bridge_customer_segment` |

## 4.5 Các quy tắc thiết kế bắt buộc

1. **Mọi dim đều có surrogate key** (BIGINT auto-increment).
2. **Mọi fact đều có FK tới dim_audit** — không ngoại lệ.
3. **Dim không bao giờ snowflake** — flatten mọi hierarchy (category, supplier merge thẳng vào `dim_product`).
4. **Date được join qua `date_sk` (INT YYYYMMDD)** — không dùng kiểu DATE để join.
5. **Khóa "Unknown" placeholder**: 
   - `customer_sk = -1` cho khách hàng không xác định
   - `date_sk = 19000101` cho ngày chưa biết (e.g., chưa ship)
   - `geography_sk = -1` cho quốc gia chưa rõ

## 4.6 Slowly Changing Dimension - chi tiết logic

### dim_customer flow ví dụ

```
Initial load (2024-01-01):
+-----------+-------------+---------+----------+----------------+----------------+------------+
| customer_sk | customer_nk | city    | country  | effective_date | expiration_date| is_current |
+-----------+-------------+---------+----------+----------------+----------------+------------+
| 1         | ALFKI       | Berlin  | DE       | 2024-01-01     | NULL           | True       |
+-----------+-------------+---------+----------+----------------+----------------+------------+

ALFKI moves to Munich on 2024-06-15 → SCD Type 2 trigger:

+-----------+-------------+---------+----------+----------------+----------------+------------+
| 1         | ALFKI       | Berlin  | DE       | 2024-01-01     | 2024-06-14     | False      |
| 2         | ALFKI       | Munich  | DE       | 2024-06-15     | NULL           | True       |
+-----------+-------------+---------+----------+----------------+----------------+------------+

Order placed on 2024-03-10 → join với customer_sk=1 (Berlin)
Order placed on 2024-08-20 → join với customer_sk=2 (Munich)
```

### Phân biệt Type 1 vs Type 2 attributes

| Attribute thay đổi | Hành động |
|---|---|
| `contact_name`, `contact_title`, `phone` | **Type 1**: UPDATE in-place trên cả 2 bản ghi (current + historical) |
| `address`, `city`, `postal_code`, `country_code`, `region_name`, `company_name` | **Type 2**: INSERT new row, expire old row |

## 4.7 Late-arriving data

**Late-arriving Fact**: Order với `order_date = 2024-03-10` đến hệ thống vào 2024-09-01.
- Lookup customer_sk theo `effective_date ≤ 2024-03-10 < expiration_date`
- → tìm được `customer_sk = 1` (Berlin) — đúng lịch sử.

**Late-arriving Dimension**: Phát hiện ALFKI thực ra đã ở Hamburg từ 2024-04-01 đến 2024-06-14 (chưa được ghi nhận):
- INSERT bản ghi mới với `effective_date = 2024-04-01`, `expiration_date = 2024-06-14`.
- UPDATE bản ghi cũ: `expiration_date = 2024-03-31`.
- UPDATE các fact rows trong khoảng đó để trỏ tới `customer_sk` mới (destructive update).

## 4.8 Degenerate Dimensions

`order_id` được giữ trong `fact_sales` mà không tạo dim riêng — đây là **degenerate dimension**, dùng để group line items thành order.

## 4.9 Junk / Mini Dimensions (future)

Khi có nhiều low-cardinality flags (e.g., is_express, is_gift, payment_method), có thể gom thành `dim_order_indicators` để giảm số column trong fact.
