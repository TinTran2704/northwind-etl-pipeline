# ETL Data Warehouse Project - Kimball Methodology

> Dự án xây dựng hệ thống ETL hoàn chỉnh theo phương pháp luận Kimball/Caserta với mô hình 4 giai đoạn **E-C-C-D** (Extract → Clean → Conform → Deliver) và áp dụng các phân hệ kiến trúc chuẩn của Kimball Group.

## 🎯 Mục tiêu dự án

Xây dựng một hệ thống ETL **production-grade** mô phỏng kiến trúc kho dữ liệu doanh nghiệp, bao gồm:

- ✅ Trích xuất dữ liệu từ nhiều nguồn (CSV, REST API, JSON)
- ✅ Làm sạch dữ liệu với hệ thống Data Quality Screens phân cấp
- ✅ Định dạng chuẩn với Conformed Dimensions
- ✅ Phân phối theo mô hình chiều (Star Schema) với SCD Type 1, 2, 3
- ✅ Surrogate Key Pipeline & Late-arriving Data handling
- ✅ Audit Dimension & Error Event Tracking
- ✅ Aggregate Tables & OLAP-ready output

## 📊 Nguồn dữ liệu

Dự án sử dụng các nguồn dữ liệu **public, không cần API key**, dễ truy cập bằng HTTP request:

### 1. Primary Source: Northwind Database (CSV)
- **URL**: `https://raw.githubusercontent.com/graphql-compose/graphql-compose-examples/master/examples/northwind/data/csv/`
- **Files**: `customers.csv`, `orders.csv`, `order-details.csv`, `products.csv`, `categories.csv`, `suppliers.csv`, `employees.csv`, `shippers.csv`
- **Lý do chọn**: Dataset kinh điển cho data warehousing, có cấu trúc OLTP rõ ràng, dễ chuyển sang Star Schema.

### 2. Secondary Source: REST Countries API (JSON)
- **URL**: `https://restcountries.com/v3.1/all?fields=name,cca2,region,subregion,currencies`
- **Mục đích**: Bổ sung Geography Dimension, tạo Conformed Dimension.

### 3. Exchange Rate API (JSON) - cho data integration
- **URL**: `https://open.er-api.com/v6/latest/USD`
- **Mục đích**: Mô phỏng việc tích hợp dữ liệu real-time, xử lý FX conversion trong fact table.

### 4. Synthetic "Dirty Data" Generator
- Tự sinh dữ liệu lỗi (NULL, duplicate, format sai, mã bưu điện không khớp city) để test các Data Quality Screens.

## 🗂️ Cấu trúc thư mục

```
etl-project/
├── README.md                          # File này
├── docs/                              # Tài liệu thiết kế chi tiết
│   ├── 01-architecture.md             # Kiến trúc tổng thể, Back Room vs Front Room
│   ├── 02-data-sources.md             # Chi tiết nguồn dữ liệu & cách extract
│   ├── 03-logical-data-map.md         # Bản đồ source-to-target mapping
│   ├── 04-dimensional-model.md        # Star schema design (Fact + Dimension)
│   ├── 05-extract-phase.md            # Spec giai đoạn Extract & CDC
│   ├── 06-clean-phase.md              # Data Quality Screens & Error Event Table
│   ├── 07-conform-phase.md            # Conformed Dimensions, Survivorship
│   ├── 08-deliver-phase.md            # SCD, Surrogate Key Pipeline, Aggregates
│   ├── 09-subsystems.md               # 34 Kimball Subsystems mapping
│   ├── 10-metadata-strategy.md        # Business/Technical/Process metadata
│   └── 11-prompting-guide.md          # Hướng dẫn prompt Claude Code hiệu quả
├── src/                               # Source code (Claude Code sẽ generate)
│   ├── extract/
│   ├── clean/
│   ├── conform/
│   ├── deliver/
│   ├── common/                        # Utilities, logging, metadata
│   └── orchestration/                 # Job scheduler, DAG
├── data/
│   ├── raw/                           # Dữ liệu thô từ nguồn
│   ├── staging/                       # Staging area (Back Room)
│   ├── error/                         # Error event records
│   └── warehouse/                     # Final dimensional model (Front Room)
├── tests/                             # Unit & integration tests
├── config/
│   ├── sources.yaml
│   └── quality_rules.yaml
└── requirements.txt
```

## 🛠️ Tech Stack đề xuất

| Lớp | Công nghệ | Lý do |
|---|---|---|
| Language | Python 3.11+ | Hệ sinh thái data engineering mạnh |
| ETL Framework | Pandas + SQLAlchemy | Đủ cho học tập, dễ mở rộng sang PySpark |
| Storage (Staging) | DuckDB / SQLite | Embedded, không cần setup server |
| Storage (Warehouse) | DuckDB / PostgreSQL | DuckDB cho local; Postgres cho production-like |
| Data Quality | Great Expectations (optional) | Hoặc tự code các screens theo Kimball |
| Orchestration | Prefect / Airflow (optional) | Bắt đầu với Python script đơn giản |
| Testing | pytest | Unit test cho từng subsystem |

## 🚀 Cách sử dụng dự án này

1. **Đọc các file trong `docs/` theo thứ tự 01 → 11** để hiểu thiết kế.
2. **Mở Claude Code** trong thư mục dự án.
3. **Sử dụng các prompt trong `docs/11-prompting-guide.md`** để Claude Code generate code từng phân hệ một.
4. **Kiểm tra output** bằng các test cases được mô tả trong từng spec.

## 📚 Tài liệu tham khảo

- *The Data Warehouse ETL Toolkit* - Ralph Kimball & Joe Caserta
- *The Data Warehouse Toolkit* (3rd ed.) - Ralph Kimball & Margy Ross
- Kimball Group - 34 Subsystems of ETL
