# 14 — Role-Based Access Control (RBAC)

> PostgreSQL RBAC design for the Northwind Data Warehouse.
> Implements Principle of Least Privilege across 4 access tiers.

## 14.1 Role vs User — Tại sao phải tách?

| Khái niệm | PostgreSQL object | Mục đích |
|---|---|---|
| **Role** (nhóm) | `ROLE … NOLOGIN` | Đóng gói tập quyền. Không đăng nhập được. |
| **User** (người dùng) | `ROLE … LOGIN` | Tài khoản đăng nhập, kế thừa 1 role. |

**Lý do tách:**
- Thêm người mới → chỉ cần `GRANT role_analyst TO new_user` — không cần cấp quyền lại từng bảng.
- Thu hồi quyền của cả nhóm → chỉ sửa 1 role, tất cả user trong nhóm bị ảnh hưởng ngay.
- Audit rõ ràng: log PostgreSQL ghi `analyst_alice` (ai) + role cho biết (được gì).

## 14.2 Sơ đồ 4 tầng quyền

```
┌─────────────────────────────────────────────────────────────────────┐
│                     northwind_dw Database                           │
│                                                                     │
│  ┌─────────────┐  ┌────────────────┐  ┌──────────────────────────┐ │
│  │  warehouse  │  │    staging     │  │  analytics_staging/marts │ │
│  │  (dims+fact)│  │  (error_events)│  │       (dbt layer)        │ │
│  └─────────────┘  └────────────────┘  └──────────────────────────┘ │
│         │                │                         │               │
│  role_readonly ─ SELECT ─┘                         │               │
│         │                                          │               │
│  role_analyst ── SELECT ──────────────────── SELECT(staging)        │
│         │                                          │               │
│  role_engineer ─ ALL ────── ALL ────────── ALL(all schemas)        │
│         │                                          │               │
│  role_marts_only ─────────────────── SELECT(analytics_marts only)  │
└─────────────────────────────────────────────────────────────────────┘
```

## 14.3 Bảng quyền chi tiết

| Role | warehouse | staging | metadata | analytics_staging | analytics_marts |
|---|---|---|---|---|---|
| `role_readonly` | SELECT | ✗ | ✗ | ✗ | ✗ |
| `role_analyst` | SELECT | SELECT | ✗ | SELECT | ✗ |
| `role_engineer` | ALL | ALL | ALL | ALL | ALL |
| `role_marts_only` | `v_customer_masked` only | ✗ | ✗ | ✗ | SELECT |

## 14.4 User → Role mapping

| Login User | Inherits Role | Persona |
|---|---|---|
| `bi_metabase` | `role_marts_only` | Metabase service account |
| `analyst_alice` | `role_analyst` | Data analyst (Germany region) |
| `engineer_bob` | `role_engineer` | Data engineer |
| `viewer_charlie` | `role_readonly` | Business stakeholder |

## 14.5 Principle of Least Privilege

> *"Mỗi entity chỉ có đúng quyền tối thiểu cần thiết để làm việc."*

Áp dụng trong project:
- `bi_metabase` **không thể** nhìn thấy `warehouse.dim_customer` gốc (có PII). Chỉ thấy `v_customer_masked`.
- `viewer_charlie` có SELECT nhưng **không thể** INSERT/UPDATE/DELETE — ngay cả khi cố tình.
- `analyst_alice` thấy staging để debug nhưng **không thể** sửa data.
- Default: schema không được GRANT → **access denied** tự động (PostgreSQL deny-by-default).

## 14.6 Column-Level Security (CLS)

**Vấn đề:** `dim_customer` chứa `phone` và `address` — PII data.

**Giải pháp:** Tạo masked VIEW thay vì grant table gốc:

```sql
CREATE VIEW warehouse.v_customer_masked AS
SELECT
    customer_sk, company_name, city, country_code,
    '***-****' AS phone_masked   -- real phone hidden
FROM warehouse.dim_customer;

GRANT SELECT ON warehouse.v_customer_masked TO role_marts_only;
-- role_marts_only KHÔNG được grant trên dim_customer gốc
```

Kết quả: `bi_metabase` connect vào Metabase → thấy công ty, thành phố, nhưng phone hiển thị `***-****`.

## 14.7 Row-Level Security (RLS)

**Use case:** Mỗi analyst phụ trách một vùng địa lý. `analyst_alice` chỉ được thấy orders của Germany.

**Cơ chế:**

```sql
-- Bảng mapping: user → country
warehouse.analyst_country_assignment:
  analyst_alice → 'DE'

-- Policy trên fact_sales
CREATE POLICY pol_analyst_country ON warehouse.fact_sales
    FOR SELECT TO role_analyst
    USING (
        -- Không có assignment → thấy tất cả (fallback)
        NOT EXISTS (SELECT 1 FROM analyst_country_assignment WHERE username = current_user)
        OR
        -- Có assignment → chỉ thấy orders của customer thuộc country đó
        customer_sk IN (
            SELECT c.customer_sk FROM dim_customer c
            JOIN analyst_country_assignment a
              ON trim(c.country_code) = a.country_code
             AND a.username = current_user
        )
    );
```

**Lưu ý quan trọng:**
- `etl_user` (table owner) **bypass RLS** tự động → ETL pipeline vẫn load đầy đủ data.
- Superuser cũng bypass RLS.
- Thêm assignment mới → `INSERT INTO analyst_country_assignment` là đủ, không cần đổi policy.

## 14.8 Cách test từng user

```bash
# 1. viewer_charlie — có thể SELECT fact_sales
docker exec etl_postgres psql -U viewer_charlie -d northwind_dw \
  -c "SELECT COUNT(*) FROM warehouse.fact_sales;"
# Kết quả mong đợi: count = 1716 (số rows hiện tại)

# 2. bi_metabase — bị DENIED khi truy cập dim_customer gốc
docker exec etl_postgres psql -U bi_metabase -d northwind_dw \
  -c "SELECT * FROM warehouse.dim_customer LIMIT 1;"
# Kết quả mong đợi: ERROR: permission denied for table dim_customer

# 3. bi_metabase — được phép dùng masked view
docker exec etl_postgres psql -U bi_metabase -d northwind_dw \
  -c "SELECT customer_sk, company_name, phone_masked FROM warehouse.v_customer_masked LIMIT 3;"
# Kết quả mong đợi: phone_masked = '***-****'

# 4. viewer_charlie — bị DENIED khi INSERT
docker exec etl_postgres psql -U viewer_charlie -d northwind_dw \
  -c "INSERT INTO warehouse.dim_customer DEFAULT VALUES;"
# Kết quả mong đợi: ERROR: permission denied for table dim_customer

# 5. analyst_alice — RLS: chỉ thấy orders của Germany (DE)
docker exec etl_postgres psql -U analyst_alice -d northwind_dw \
  -c "SELECT COUNT(*) FROM warehouse.fact_sales;"
# Kết quả: chỉ các orders của customer DE

# 6. engineer_bob — thấy tất cả
docker exec etl_postgres psql -U engineer_bob -d northwind_dw \
  -c "SELECT COUNT(*) FROM warehouse.fact_sales;"
# Kết quả: full count = 1716
```

## 14.9 Mở rộng trong tương lai

| Cần | Cách làm |
|---|---|
| Thêm analyst mới | `CREATE ROLE analyst_dave LOGIN PASSWORD '...'; GRANT role_analyst TO analyst_dave;` |
| Assign country cho user | `INSERT INTO warehouse.analyst_country_assignment VALUES ('analyst_dave', 'FR');` |
| Revoke quyền group | `REVOKE role_analyst FROM analyst_alice;` — ngay lập tức |
| Thêm masked column | Thêm column vào `v_customer_masked` view |
| Column encryption | Dùng `pgcrypto` extension cho sensitive fields |
