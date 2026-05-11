# 01 - Kiến trúc tổng thể hệ thống ETL

## 1.1 Triết lý thiết kế

Hệ thống tuân thủ phương pháp luận **Kimball/Caserta** với 4 giai đoạn:

```
┌─────────┐     ┌────────┐     ┌─────────┐     ┌──────────┐
│ EXTRACT │ ──▶ │ CLEAN  │ ──▶ │ CONFORM │ ──▶ │ DELIVER  │
└─────────┘     └────────┘     └─────────┘     └──────────┘
   (E)            (C)             (C)             (D)
```

**Nguyên tắc cốt lõi:**
- Giá trị ETL không nằm ở việc dịch chuyển dữ liệu, mà ở việc khắc phục sai sót, giải quyết xung đột định danh, và tạo nguồn sự thật duy nhất.
- ETL **báo cáo lỗi** thay vì **âm thầm sửa lỗi**.
- Mọi giai đoạn đều **stage to disk** để tạo recovery point.

## 1.2 Back Room vs Front Room

| Khu vực | Vai trò | Người truy cập |
|---|---|---|
| **Back Room** (`data/staging/`, `data/raw/`, `data/error/`) | Trích xuất, làm sạch, chuẩn hóa | ETL team only |
| **Front Room** (`data/warehouse/`) | Mô hình chiều, sẵn sàng cho BI | End-users, BI tools |

⚠️ **Tuyệt đối** không cho user truy cập Back Room → phá vỡ kiểm soát chất lượng.

## 1.3 Sơ đồ luồng dữ liệu

```
[Source Systems]
    ├── Northwind CSV  ──┐
    ├── REST Countries  ─┤
    └── Exchange Rate   ─┤
                         ▼
                   [EXTRACT]  ──▶  data/raw/{source}/{date}/
                         │           (immutable snapshots)
                         ▼
                    [CLEAN]   ──▶  data/staging/cleaned/
                         │     ──▶  data/error/error_events.parquet
                         │     ──▶  audit_dim records
                         ▼
                   [CONFORM]  ──▶  data/staging/conformed/
                         │           (deduped, golden records)
                         ▼
                   [DELIVER]  ──▶  data/warehouse/
                         │           ├── dim_customer
                         │           ├── dim_product
                         │           ├── dim_date
                         │           ├── dim_geography
                         │           ├── dim_audit
                         │           ├── fact_sales
                         │           └── agg_sales_monthly
                         ▼
                    [BI Tools / Queries]
```

## 1.4 Quyết định kiến trúc

| Quyết định | Lựa chọn | Lý do |
|---|---|---|
| ETL Tool vs Hand-coding | **Hand-coding (Python)** | Dễ học, kiểm soát đầy đủ, dùng pytest cho QA |
| Batch vs Streaming | **Batch (microbatch ready)** | Đơn giản hóa version đầu, có thể nâng cấp sau |
| Staging strategy | **File + DuckDB** | Files cho audit; DuckDB cho query nhanh |
| Surrogate keys | **Auto-increment integers** | Tuân thủ Kimball, không phụ thuộc nguồn |
| Recovery model | **Idempotent + re-entrant** | Mỗi job có thể chạy lại không gây trùng |

## 1.5 Yêu cầu phi chức năng (NFRs)

- **Auditability**: Mọi hàng dữ liệu phải truy ngược được nguồn gốc (lineage).
- **Recoverability**: Khôi phục từ bất kỳ điểm staging nào.
- **Reproducibility**: Chạy lại với cùng input cho cùng output (deterministic).
- **Observability**: Log đầy đủ rows in / rows out / rows rejected cho từng bước.
- **Security**: Không có credentials hard-code; dùng `.env`.

## 1.6 Module dependency graph

```
extract  ──▶  clean  ──▶  conform  ──▶  deliver
   │            │            │            │
   └────────────┴────────────┴────────────┘
                       ▼
                   common/
              (logging, metadata,
               quality screens,
               surrogate keys)
```

`common/` được mọi module dùng chung — phải implement TRƯỚC khi build các phase khác.
