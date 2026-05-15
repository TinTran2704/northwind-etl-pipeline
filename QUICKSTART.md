# ETL Stack — Hướng dẫn khởi động nhanh (Windows)

## Yêu cầu

| Phần mềm | Version tối thiểu | Link |
|---|---|---|
| Docker Desktop | 4.x | https://docker.com/products/docker-desktop |
| Python | 3.11+ | https://python.org/downloads/ |
| Git (optional) | any | https://git-scm.com |

## Bước 1 — Lần đầu khởi động (5-10 phút)

```
Double-click: start.bat
→ Chọn 6  (Setup Python venv)
→ Chọn 8  (Tải seed data)
→ Chọn 1  (Start Docker stack)
```

Docker sẽ pull các images (~2GB lần đầu). Uống cà phê ☕

## Bước 2 — Truy cập UI

| Service | URL | Login |
|---|---|---|
| **pgAdmin** (quản lý database) | http://localhost:5050 | ${AIRFLOW_ADMIN_EMAIL} / admin |
| **Airflow** (orchestrator) | http://localhost:8080 | admin / admin |

## Bước 3 — Chạy ETL pipeline

```
start.bat → Chọn 9  (Full pipeline)
```

Hoặc từ terminal:
```cmd
.venv\Scripts\activate
set PYTHONPATH=%CD%
python -m src.orchestration.pipeline run
```

## Cấu trúc containers

```
etl_postgres          ← PostgreSQL 16 (port 5432)
etl_pgadmin           ← pgAdmin 4    (port 5050)
etl_airflow_init      ← 1-time init, thoát sau khi xong
etl_airflow_web       ← Airflow Webserver (port 8080)
etl_airflow_scheduler ← Airflow Scheduler
```

## Kết nối database từ IDE / tool bên ngoài

```
Host:     localhost
Port:     5432
Database: northwind_dw
User:     etl_user
Password: etl_password
```

## Dừng stack (giữ data)

```
start.bat → Chọn 2
```

## Xóa toàn bộ + làm lại từ đầu

```
start.bat → Chọn 13 → Gõ YES
```

## Troubleshooting

**Docker không khởi động?**
→ Mở Docker Desktop, đợi "Engine running"

**Port 5432/8080/5050 đã bị dùng?**
→ Sửa ports trong `docker-compose.yml`
→ Ví dụ: `"5433:5432"` để dùng port 5433 cho Postgres bên ngoài

**Airflow mắc kẹt ở "airflow-init"?**
→ `docker logs etl_airflow_init`

**Xem log của service nào đó:**
→ `start.bat → Chọn 5` → gõ tên service
→ Hoặc: `docker logs etl_postgres -f`