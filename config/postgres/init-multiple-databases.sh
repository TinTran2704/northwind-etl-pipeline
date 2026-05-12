#!/bin/bash
# ============================================================
# Tự động chạy khi PostgreSQL khởi động LẦN ĐẦU
# (Postgres scan /docker-entrypoint-initdb.d/ và chạy file *.sh, *.sql)
#
# Làm 5 việc trong 1 lần:
#   1. Tạo airflow database
#   2. Tạo $ETL_DW_DATABASE
#   3. Tạo airflow user
#   4. Tạo $ETL_DW_USER
#   5. Tạo schemas warehouse/staging/metadata + toàn bộ tables
# ============================================================
set -e

echo "[init] Starting database initialization..."

# ── Bước 1+2: Tạo databases ──────────────────────────────────
for db in airflow "$ETL_DW_DATABASE"; do
    echo "[init] Creating database: $db"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        SELECT 'CREATE DATABASE $db'
        WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '$db')\gexec
EOSQL
done

# ── Bước 3: Tạo airflow user ─────────────────────────────────
echo "[init] Creating user: airflow"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow') THEN
            CREATE ROLE airflow LOGIN PASSWORD '$AIRFLOW_DB_PASSWORD';
        END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname airflow <<-EOSQL
    GRANT ALL ON SCHEMA public TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow;
EOSQL

# ── Bước 4: Tạo ETL user ─────────────────────────────────────
echo "[init] Creating user: $ETL_DW_USER"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '$ETL_DW_USER') THEN
            CREATE ROLE $ETL_DW_USER LOGIN PASSWORD '$ETL_DW_PASSWORD';
        END IF;
    END
    \$\$;
    GRANT ALL PRIVILEGES ON DATABASE $ETL_DW_DATABASE TO $ETL_DW_USER;
EOSQL

# ── Bước 5: Build schema warehouse trong ETL DW ──────────────
echo "[init] Building warehouse schema in $ETL_DW_DATABASE..."
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$ETL_DW_DATABASE" <<-'EOSQL'

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS metadata;

-- ── METADATA ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metadata.etl_runs (
    batch_id        VARCHAR(64) PRIMARY KEY,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          VARCHAR(16) NOT NULL DEFAULT 'RUNNING'
                    CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    rows_extracted  BIGINT DEFAULT 0,
    rows_rejected   BIGINT DEFAULT 0,
    rows_loaded     BIGINT DEFAULT 0,
    error_summary   TEXT
);

CREATE TABLE IF NOT EXISTS metadata.lineage (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        VARCHAR(64) REFERENCES metadata.etl_runs(batch_id),
    target_table    VARCHAR(80) NOT NULL,
    target_column   VARCHAR(80),
    source_system   VARCHAR(40),
    source_table    VARCHAR(80),
    source_column   VARCHAR(80),
    transformation  TEXT,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── DIM_DATE ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_sk         INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN
);

-- ── DIM_GEOGRAPHY (Conformed, Type 1) ────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_geography (
    geography_sk        BIGSERIAL PRIMARY KEY,
    country_code        CHAR(2) NOT NULL UNIQUE,
    country_name        VARCHAR(80),
    region              VARCHAR(40),
    subregion           VARCHAR(40),
    primary_currency    CHAR(3)
);
INSERT INTO warehouse.dim_geography (geography_sk, country_code, country_name, region, subregion)
VALUES (-1, 'ZZ', 'Unknown', 'Unknown', 'Unknown')
ON CONFLICT DO NOTHING;

-- ── DIM_AUDIT ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_audit (
    audit_sk            BIGSERIAL PRIMARY KEY,
    etl_batch_id        VARCHAR(64) REFERENCES metadata.etl_runs(batch_id),
    etl_run_timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(40),
    source_file         VARCHAR(120),
    extract_row_count   INT DEFAULT 0,
    reject_row_count    INT DEFAULT 0,
    quality_score       NUMERIC(4,3) CHECK (quality_score BETWEEN 0 AND 1),
    has_anomalies       BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO warehouse.dim_audit (audit_sk, source_system, quality_score)
VALUES (-1, 'SYSTEM', 1.0)
ON CONFLICT DO NOTHING;

-- ── DIM_CUSTOMER (SCD Type 2) ────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk         BIGSERIAL PRIMARY KEY,
    customer_nk         VARCHAR(10) NOT NULL,
    company_name        VARCHAR(80),
    contact_name        VARCHAR(50),
    contact_title       VARCHAR(50),
    address             VARCHAR(120),
    city                VARCHAR(40),
    postal_code         VARCHAR(15),
    country_code        CHAR(2) REFERENCES warehouse.dim_geography(country_code),
    region_name         VARCHAR(40),
    phone               VARCHAR(30),
    effective_date      DATE NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_customer_scd UNIQUE (customer_nk, effective_date)
);
INSERT INTO warehouse.dim_customer (customer_sk, customer_nk, company_name, effective_date, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Customer', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_customer_nk ON warehouse.dim_customer(customer_nk);
CREATE INDEX IF NOT EXISTS idx_customer_current ON warehouse.dim_customer(customer_nk) WHERE is_current = TRUE;

-- ── DIM_PRODUCT (SCD Type 2) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_sk          BIGSERIAL PRIMARY KEY,
    product_nk          INT NOT NULL,
    product_name        VARCHAR(80),
    category_name       VARCHAR(40),
    supplier_name       VARCHAR(80),
    supplier_country    CHAR(2),
    quantity_per_unit   VARCHAR(40),
    unit_price          NUMERIC(10,2),
    units_in_stock      INT,
    discontinued        BOOLEAN,
    effective_date      DATE NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_product_scd UNIQUE (product_nk, effective_date)
);
INSERT INTO warehouse.dim_product (product_sk, product_nk, product_name, effective_date, is_current)
VALUES (-1, -1, 'Unknown Product', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_product_nk ON warehouse.dim_product(product_nk);

-- ── DIM_EMPLOYEE (SCD Type 2) ────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_employee (
    employee_sk         BIGSERIAL PRIMARY KEY,
    employee_nk         INT NOT NULL,
    full_name           VARCHAR(80),
    title               VARCHAR(40),
    reports_to_nk       INT,
    hire_date            DATE,
    city                VARCHAR(40),
    country_code        CHAR(2),
    effective_date      DATE NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_employee_scd UNIQUE (employee_nk, effective_date)
);
INSERT INTO warehouse.dim_employee (employee_sk, employee_nk, full_name, effective_date, is_current)
VALUES (-1, -1, 'Unknown Employee', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;

-- ── DIM_SHIPPER (SCD Type 1) ─────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.dim_shipper (
    shipper_sk          BIGSERIAL PRIMARY KEY,
    shipper_nk          INT NOT NULL UNIQUE,
    company_name        VARCHAR(80),
    phone               VARCHAR(30),
    audit_sk            BIGINT REFERENCES warehouse.dim_audit(audit_sk)
);
INSERT INTO warehouse.dim_shipper (shipper_sk, shipper_nk, company_name)
VALUES (-1, -1, 'Unknown Shipper')
ON CONFLICT DO NOTHING;

-- ── FACT_SALES ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    order_id            INT NOT NULL,
    line_number         SMALLINT NOT NULL,
    order_date_sk       INT REFERENCES warehouse.dim_date(date_sk),
    required_date_sk    INT REFERENCES warehouse.dim_date(date_sk),
    shipped_date_sk     INT REFERENCES warehouse.dim_date(date_sk),
    customer_sk         BIGINT NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    employee_sk         BIGINT NOT NULL REFERENCES warehouse.dim_employee(employee_sk),
    product_sk          BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    shipper_sk          BIGINT NOT NULL REFERENCES warehouse.dim_shipper(shipper_sk),
    ship_geography_sk   BIGINT REFERENCES warehouse.dim_geography(geography_sk),
    audit_sk            BIGINT NOT NULL REFERENCES warehouse.dim_audit(audit_sk),
    quantity            INT NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(10,2) NOT NULL,
    discount            NUMERIC(4,3) CHECK (discount BETWEEN 0 AND 1),
    extended_price      NUMERIC(12,2),
    discount_amount     NUMERIC(12,2),
    net_amount          NUMERIC(12,2),
    freight_allocated   NUMERIC(10,2),
    PRIMARY KEY (order_id, line_number)
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON warehouse.fact_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON warehouse.fact_sales(order_date_sk);

-- ── AGG_SALES_MONTHLY ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouse.agg_sales_monthly (
    year_month          INT NOT NULL,
    product_sk          BIGINT NOT NULL REFERENCES warehouse.dim_product(product_sk),
    customer_country    CHAR(2),
    total_quantity      BIGINT,
    total_net_amount    NUMERIC(14,2),
    order_count         INT,
    audit_sk            BIGINT REFERENCES warehouse.dim_audit(audit_sk),
    PRIMARY KEY (year_month, product_sk, customer_country)
);

-- ── ERROR_EVENTS (staging) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS staging.error_events (
    error_event_id      BIGSERIAL PRIMARY KEY,
    etl_batch_id        VARCHAR(64),
    event_timestamp     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(40),
    source_table        VARCHAR(80),
    source_record_pk    VARCHAR(120),
    screen_name         VARCHAR(80),
    screen_severity     VARCHAR(10) CHECK (screen_severity IN ('INFO','WARN','ERROR','FATAL')),
    column_name         VARCHAR(80),
    expected_value      TEXT,
    actual_value        TEXT,
    message             TEXT
);
CREATE INDEX IF NOT EXISTS idx_error_batch ON staging.error_events(etl_batch_id);
CREATE INDEX IF NOT EXISTS idx_error_severity ON staging.error_events(screen_severity);

EOSQL

# ── Bước 6: Grant quyền cho ETL user trên các schema ─────────
echo "[init] Granting permissions to $ETL_DW_USER..."
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$ETL_DW_DATABASE" <<-EOSQL
    GRANT USAGE ON SCHEMA warehouse, staging, metadata TO $ETL_DW_USER;
    GRANT ALL ON ALL TABLES IN SCHEMA warehouse TO $ETL_DW_USER;
    GRANT ALL ON ALL TABLES IN SCHEMA staging TO $ETL_DW_USER;
    GRANT ALL ON ALL TABLES IN SCHEMA metadata TO $ETL_DW_USER;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA warehouse TO $ETL_DW_USER;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA staging TO $ETL_DW_USER;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA metadata TO $ETL_DW_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT ALL ON TABLES TO $ETL_DW_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA staging   GRANT ALL ON TABLES TO $ETL_DW_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA metadata  GRANT ALL ON TABLES TO $ETL_DW_USER;
EOSQL

echo "[init] ============================================"
echo "[init]  Database setup COMPLETE"
echo "[init]  - airflow DB ready"
echo "[init]  - $ETL_DW_DATABASE ready"
echo "[init]    schemas: warehouse, staging, metadata"
echo "[init]    user: $ETL_DW_USER"
echo "[init] ============================================"