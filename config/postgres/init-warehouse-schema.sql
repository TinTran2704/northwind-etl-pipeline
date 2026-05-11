-- ============================================================
-- Khởi tạo schema cho Data Warehouse (northwind_dw)
-- Chạy tự động khi PostgreSQL init lần đầu
-- ============================================================

\c northwind_dw;

-- Tạo schemas phân tách Back Room và Front Room
CREATE SCHEMA IF NOT EXISTS staging;      -- Back Room: raw + cleaned
CREATE SCHEMA IF NOT EXISTS warehouse;    -- Front Room: dimensional model
CREATE SCHEMA IF NOT EXISTS metadata;     -- ETL metadata

-- ============================================================
-- METADATA SCHEMA
-- ============================================================

CREATE TABLE IF NOT EXISTS metadata.etl_runs (
    batch_id        VARCHAR(64)  PRIMARY KEY,
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          VARCHAR(16)  NOT NULL DEFAULT 'RUNNING'
                                 CHECK (status IN ('RUNNING','SUCCESS','FAILED','PARTIAL')),
    rows_extracted  BIGINT       DEFAULT 0,
    rows_rejected   BIGINT       DEFAULT 0,
    rows_loaded     BIGINT       DEFAULT 0,
    error_summary   TEXT
);

CREATE TABLE IF NOT EXISTS metadata.lineage (
    id              BIGSERIAL    PRIMARY KEY,
    batch_id        VARCHAR(64)  REFERENCES metadata.etl_runs(batch_id),
    target_table    VARCHAR(80)  NOT NULL,
    target_column   VARCHAR(80),
    source_system   VARCHAR(40),
    source_table    VARCHAR(80),
    source_column   VARCHAR(80),
    transformation  TEXT,
    recorded_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ============================================================
-- WAREHOUSE SCHEMA — Dimension Tables
-- ============================================================

-- dim_date: Pre-built, không có SCD
CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_sk         INT          PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL,
    day_of_week     SMALLINT,
    day_name        VARCHAR(10),
    day_of_month    SMALLINT,
    day_of_year     SMALLINT,
    week_of_year    SMALLINT,
    month           SMALLINT,
    month_name      VARCHAR(10),
    quarter         SMALLINT,
    year            SMALLINT,
    is_weekend      BOOLEAN,
    fiscal_year     SMALLINT,
    fiscal_quarter  SMALLINT
);

-- dim_geography: Conformed, Type 1 (update in place)
CREATE TABLE IF NOT EXISTS warehouse.dim_geography (
    geography_sk        BIGSERIAL    PRIMARY KEY,
    country_code        CHAR(2)      NOT NULL UNIQUE,
    country_name        VARCHAR(80),
    region              VARCHAR(40),
    subregion           VARCHAR(40),
    primary_currency    CHAR(3)
);
INSERT INTO warehouse.dim_geography (geography_sk, country_code, country_name, region, subregion)
VALUES (-1, 'ZZ', 'Unknown', 'Unknown', 'Unknown')
ON CONFLICT DO NOTHING;

-- dim_audit: Insert-only, linked to every fact row
CREATE TABLE IF NOT EXISTS warehouse.dim_audit (
    audit_sk            BIGSERIAL    PRIMARY KEY,
    etl_batch_id        VARCHAR(64)  REFERENCES metadata.etl_runs(batch_id),
    etl_run_timestamp   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(40),
    source_file         VARCHAR(120),
    extract_row_count   INT          DEFAULT 0,
    reject_row_count    INT          DEFAULT 0,
    quality_score       NUMERIC(4,3) CHECK (quality_score BETWEEN 0 AND 1),
    has_anomalies       BOOLEAN      DEFAULT FALSE,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
-- Unknown / system audit record
INSERT INTO warehouse.dim_audit (audit_sk, etl_batch_id, source_system, quality_score)
VALUES (-1, NULL, 'SYSTEM', 1.0)
ON CONFLICT DO NOTHING;

-- dim_customer: SCD Type 2
CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk         BIGSERIAL    PRIMARY KEY,
    customer_nk         VARCHAR(10)  NOT NULL,
    company_name        VARCHAR(80),
    contact_name        VARCHAR(50),
    contact_title       VARCHAR(50),
    address             VARCHAR(120),
    city                VARCHAR(40),
    postal_code         VARCHAR(15),
    country_code        CHAR(2)      REFERENCES warehouse.dim_geography(country_code),
    region_name         VARCHAR(40),
    phone               VARCHAR(30),
    effective_date      DATE         NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT       REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_customer_scd UNIQUE (customer_nk, effective_date)
);
INSERT INTO warehouse.dim_customer (customer_sk, customer_nk, company_name, effective_date, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Customer', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_customer_nk ON warehouse.dim_customer(customer_nk);
CREATE INDEX IF NOT EXISTS idx_customer_current ON warehouse.dim_customer(customer_nk) WHERE is_current = TRUE;

-- dim_product: SCD Type 2
CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_sk          BIGSERIAL    PRIMARY KEY,
    product_nk          INT          NOT NULL,
    product_name        VARCHAR(80),
    category_name       VARCHAR(40),
    supplier_name       VARCHAR(80),
    supplier_country    CHAR(2),
    quantity_per_unit   VARCHAR(40),
    unit_price          NUMERIC(10,2),
    units_in_stock      INT,
    discontinued        BOOLEAN,
    effective_date      DATE         NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT       REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_product_scd UNIQUE (product_nk, effective_date)
);
INSERT INTO warehouse.dim_product (product_sk, product_nk, product_name, effective_date, is_current)
VALUES (-1, -1, 'Unknown Product', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;
CREATE INDEX IF NOT EXISTS idx_product_nk ON warehouse.dim_product(product_nk);

-- dim_employee: SCD Type 2
CREATE TABLE IF NOT EXISTS warehouse.dim_employee (
    employee_sk         BIGSERIAL    PRIMARY KEY,
    employee_nk         INT          NOT NULL,
    full_name           VARCHAR(80),
    title               VARCHAR(40),
    reports_to_nk       INT,
    hire_date           DATE,
    city                VARCHAR(40),
    country_code        CHAR(2),
    effective_date      DATE         NOT NULL,
    expiration_date     DATE,
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    audit_sk            BIGINT       REFERENCES warehouse.dim_audit(audit_sk),
    CONSTRAINT uq_employee_scd UNIQUE (employee_nk, effective_date)
);
INSERT INTO warehouse.dim_employee (employee_sk, employee_nk, full_name, effective_date, is_current)
VALUES (-1, -1, 'Unknown Employee', '1900-01-01', TRUE)
ON CONFLICT DO NOTHING;

-- dim_shipper: SCD Type 1
CREATE TABLE IF NOT EXISTS warehouse.dim_shipper (
    shipper_sk          BIGSERIAL    PRIMARY KEY,
    shipper_nk          INT          NOT NULL UNIQUE,
    company_name        VARCHAR(80),
    phone               VARCHAR(30),
    audit_sk            BIGINT       REFERENCES warehouse.dim_audit(audit_sk)
);
INSERT INTO warehouse.dim_shipper (shipper_sk, shipper_nk, company_name)
VALUES (-1, -1, 'Unknown Shipper')
ON CONFLICT DO NOTHING;

-- ============================================================
-- WAREHOUSE SCHEMA — Fact Tables
-- ============================================================

-- fact_sales: Transaction grain (1 row = 1 order line item)
CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    -- Degenerate dimension
    order_id            INT          NOT NULL,
    line_number         SMALLINT     NOT NULL,
    -- Date FKs
    order_date_sk       INT          REFERENCES warehouse.dim_date(date_sk),
    required_date_sk    INT          REFERENCES warehouse.dim_date(date_sk),
    shipped_date_sk     INT          REFERENCES warehouse.dim_date(date_sk),
    -- Dimension FKs
    customer_sk         BIGINT       NOT NULL REFERENCES warehouse.dim_customer(customer_sk),
    employee_sk         BIGINT       NOT NULL REFERENCES warehouse.dim_employee(employee_sk),
    product_sk          BIGINT       NOT NULL REFERENCES warehouse.dim_product(product_sk),
    shipper_sk          BIGINT       NOT NULL REFERENCES warehouse.dim_shipper(shipper_sk),
    ship_geography_sk   BIGINT       REFERENCES warehouse.dim_geography(geography_sk),
    audit_sk            BIGINT       NOT NULL REFERENCES warehouse.dim_audit(audit_sk),
    -- Measures
    quantity            INT          NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(10,2) NOT NULL,
    discount            NUMERIC(4,3) CHECK (discount BETWEEN 0 AND 1),
    extended_price      NUMERIC(12,2),
    discount_amount     NUMERIC(12,2),
    net_amount          NUMERIC(12,2),
    freight_allocated   NUMERIC(10,2),
    -- Constraints
    PRIMARY KEY (order_id, line_number)
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON warehouse.fact_sales(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product  ON warehouse.fact_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_sales_date     ON warehouse.fact_sales(order_date_sk);

-- agg_sales_monthly: Pre-computed aggregate
CREATE TABLE IF NOT EXISTS warehouse.agg_sales_monthly (
    year_month          INT          NOT NULL,  -- YYYYMM
    product_sk          BIGINT       NOT NULL REFERENCES warehouse.dim_product(product_sk),
    customer_country    CHAR(2),
    total_quantity      BIGINT,
    total_net_amount    NUMERIC(14,2),
    order_count         INT,
    audit_sk            BIGINT       REFERENCES warehouse.dim_audit(audit_sk),
    PRIMARY KEY (year_month, product_sk, customer_country)
);

-- ============================================================
-- STAGING SCHEMA — Error tracking
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.error_events (
    error_event_id      BIGSERIAL    PRIMARY KEY,
    etl_batch_id        VARCHAR(64),
    event_timestamp     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    source_system       VARCHAR(40),
    source_table        VARCHAR(80),
    source_record_pk    VARCHAR(120),
    screen_name         VARCHAR(80),
    screen_severity     VARCHAR(10)  CHECK (screen_severity IN ('INFO','WARN','ERROR','FATAL')),
    column_name         VARCHAR(80),
    expected_value      TEXT,
    actual_value        TEXT,
    message             TEXT
);
CREATE INDEX IF NOT EXISTS idx_error_batch ON staging.error_events(etl_batch_id);
CREATE INDEX IF NOT EXISTS idx_error_severity ON staging.error_events(screen_severity);

-- ============================================================
-- Grant quyền cho etl_user
-- ============================================================
GRANT USAGE ON SCHEMA warehouse, staging, metadata TO etl_user;
GRANT ALL ON ALL TABLES IN SCHEMA warehouse TO etl_user;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT ALL ON ALL TABLES IN SCHEMA metadata TO etl_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA warehouse TO etl_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA staging TO etl_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA metadata TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA warehouse GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT ALL ON TABLES TO etl_user;

\echo '==> Warehouse schema initialized successfully'