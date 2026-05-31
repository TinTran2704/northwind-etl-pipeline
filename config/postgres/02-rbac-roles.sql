-- ============================================================
-- 02-rbac-roles.sql
-- Role-Based Access Control for Northwind Data Warehouse
--
-- Runs automatically after 01-init-databases.sh (alphabetical order).
-- Implements Principle of Least Privilege across 4 access tiers.
-- ============================================================

\connect northwind_dw

-- ============================================================
-- SECTION 1 — GROUP ROLES (no login, represent permissions)
-- ============================================================

DO $$
BEGIN
    -- Tier 1: read-only on warehouse (BI consumers, dashboards)
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_readonly') THEN
        CREATE ROLE role_readonly NOLOGIN;
        COMMENT ON ROLE role_readonly IS 'SELECT on warehouse schema. For analysts and BI read tools.';
    END IF;

    -- Tier 2: analyst — warehouse + staging visibility
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_analyst') THEN
        CREATE ROLE role_analyst NOLOGIN;
        COMMENT ON ROLE role_analyst IS 'SELECT on warehouse + staging. For data analysts who need raw visibility.';
    END IF;

    -- Tier 3: engineer — full access to all schemas
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_engineer') THEN
        CREATE ROLE role_engineer NOLOGIN;
        COMMENT ON ROLE role_engineer IS 'ALL privileges on all schemas. For data engineers running ETL.';
    END IF;

    -- Tier 4: marts-only — only the final BI-ready layer
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'role_marts_only') THEN
        CREATE ROLE role_marts_only NOLOGIN;
        COMMENT ON ROLE role_marts_only IS 'SELECT on analytics_marts only. For Metabase and business users.';
    END IF;
END
$$;

-- ============================================================
-- SECTION 2 — LOGIN USERS (inherit one role each)
-- ============================================================

DO $$
BEGIN
    -- Metabase service account — only sees published marts
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_metabase') THEN
        CREATE ROLE bi_metabase LOGIN PASSWORD 'metabase_pass';
        COMMENT ON ROLE bi_metabase IS 'Metabase service account. role_marts_only tier.';
    END IF;

    -- Data analyst — can inspect warehouse + staging for troubleshooting
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_alice') THEN
        CREATE ROLE analyst_alice LOGIN PASSWORD 'alice_pass';
        COMMENT ON ROLE analyst_alice IS 'Data analyst. role_analyst tier (DE region assigned).';
    END IF;

    -- Data engineer — full access to run ETL, debug, load data
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'engineer_bob') THEN
        CREATE ROLE engineer_bob LOGIN PASSWORD 'bob_pass';
        COMMENT ON ROLE engineer_bob IS 'Data engineer. role_engineer tier.';
    END IF;

    -- Business stakeholder — read-only on finished warehouse
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'viewer_charlie') THEN
        CREATE ROLE viewer_charlie LOGIN PASSWORD 'charlie_pass';
        COMMENT ON ROLE viewer_charlie IS 'Business viewer. role_readonly tier.';
    END IF;
END
$$;

-- Assign roles to users
GRANT role_marts_only TO bi_metabase;
GRANT role_analyst    TO analyst_alice;
GRANT role_engineer   TO engineer_bob;
GRANT role_readonly   TO viewer_charlie;

-- All users need CONNECT on the database
GRANT CONNECT ON DATABASE northwind_dw
    TO role_readonly, role_analyst, role_engineer, role_marts_only;

-- ============================================================
-- SECTION 3 — SCHEMA-LEVEL GRANTS PER ROLE
-- ============================================================

-- ── role_readonly ────────────────────────────────────────────
-- Sees: warehouse schema (dims + facts)
-- Cannot see: staging, metadata, analytics_staging
GRANT USAGE ON SCHEMA warehouse TO role_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO role_readonly;
-- Future tables created by etl_user in warehouse → auto-grant SELECT
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA warehouse
    GRANT SELECT ON TABLES TO role_readonly;

-- ── role_analyst ─────────────────────────────────────────────
-- Sees: warehouse + staging + analytics_staging (dbt views)
GRANT USAGE ON SCHEMA warehouse, staging TO role_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO role_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA staging  TO role_analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA warehouse
    GRANT SELECT ON TABLES TO role_analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA staging
    GRANT SELECT ON TABLES TO role_analyst;

-- analytics_staging (dbt layer) — etl_user creates views there
CREATE SCHEMA IF NOT EXISTS analytics_staging;
GRANT USAGE ON SCHEMA analytics_staging TO role_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_staging TO role_analyst;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA analytics_staging
    GRANT SELECT ON TABLES TO role_analyst;

-- ── role_engineer ────────────────────────────────────────────
-- Full access to every schema including metadata lineage
GRANT USAGE ON SCHEMA warehouse, staging, metadata TO role_engineer;
GRANT ALL ON ALL TABLES    IN SCHEMA warehouse TO role_engineer;
GRANT ALL ON ALL TABLES    IN SCHEMA staging   TO role_engineer;
GRANT ALL ON ALL TABLES    IN SCHEMA metadata  TO role_engineer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA warehouse TO role_engineer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA staging   TO role_engineer;
GRANT ALL ON ALL SEQUENCES IN SCHEMA metadata  TO role_engineer;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA warehouse
    GRANT ALL ON TABLES TO role_engineer;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA staging
    GRANT ALL ON TABLES TO role_engineer;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA metadata
    GRANT ALL ON TABLES TO role_engineer;

-- ── role_marts_only ──────────────────────────────────────────
-- Sees ONLY analytics_marts (Metabase tier)
-- Must NOT see raw warehouse tables
CREATE SCHEMA IF NOT EXISTS analytics_marts;
GRANT USAGE ON SCHEMA analytics_marts TO role_marts_only;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_marts TO role_marts_only;
ALTER DEFAULT PRIVILEGES FOR ROLE etl_user IN SCHEMA analytics_marts
    GRANT SELECT ON TABLES TO role_marts_only;

-- role_marts_only needs USAGE on warehouse schema to access the masked view
-- (USAGE = navigate schema; does NOT grant SELECT on any table)
GRANT USAGE ON SCHEMA warehouse TO role_marts_only;

-- ============================================================
-- SECTION 4 — COLUMN-LEVEL SECURITY (PII masking demo)
-- ============================================================
-- Problem: dim_customer.phone and .address are PII.
-- Solution: expose a masked VIEW; role_marts_only uses the view,
--           never the underlying table.

CREATE OR REPLACE VIEW warehouse.v_customer_masked AS
SELECT
    customer_sk,
    customer_nk,
    company_name,
    contact_name,
    contact_title,
    city,
    postal_code,
    country_code,
    region_name,
    '***-****'::VARCHAR AS phone_masked,  -- PII column hidden
    effective_date,
    expiration_date,
    is_current
FROM warehouse.dim_customer;

-- role_readonly and role_analyst see the real table (trusted users)
-- role_marts_only sees only the masked view
GRANT SELECT ON warehouse.v_customer_masked TO role_marts_only;

-- ============================================================
-- SECTION 5 — ROW-LEVEL SECURITY (country assignment demo)
-- ============================================================
-- Use case: each analyst is responsible for specific countries.
-- analyst_alice → Germany (DE). Other analysts see all.

-- Mapping table: username → allowed country
CREATE TABLE IF NOT EXISTS warehouse.analyst_country_assignment (
    username     VARCHAR(50) PRIMARY KEY,
    country_code CHAR(2)     NOT NULL
);

INSERT INTO warehouse.analyst_country_assignment (username, country_code)
VALUES ('analyst_alice', 'DE')
ON CONFLICT DO NOTHING;

-- Enable RLS on fact_sales
ALTER TABLE warehouse.fact_sales ENABLE ROW LEVEL SECURITY;
-- etl_user (table owner) still bypasses RLS — no FORCE needed for ETL writes

-- Policy for role_readonly: sees ALL rows
DROP POLICY IF EXISTS pol_readonly_all ON warehouse.fact_sales;
CREATE POLICY pol_readonly_all ON warehouse.fact_sales
    FOR SELECT TO role_readonly
    USING (TRUE);

-- Policy for role_engineer: sees ALL rows
DROP POLICY IF EXISTS pol_engineer_all ON warehouse.fact_sales;
CREATE POLICY pol_engineer_all ON warehouse.fact_sales
    FOR SELECT TO role_engineer
    USING (TRUE);

-- Policy for role_analyst: sees orders filtered by assigned country.
-- If no assignment exists for current_user → sees all rows.
DROP POLICY IF EXISTS pol_analyst_country ON warehouse.fact_sales;
CREATE POLICY pol_analyst_country ON warehouse.fact_sales
    FOR SELECT TO role_analyst
    USING (
        -- No assignment row → unrestricted view
        NOT EXISTS (
            SELECT 1 FROM warehouse.analyst_country_assignment
            WHERE username = current_user
        )
        OR
        -- Assignment exists → filter by country via dim_customer join
        customer_sk IN (
            SELECT c.customer_sk
            FROM warehouse.dim_customer c
            JOIN warehouse.analyst_country_assignment a
              ON trim(c.country_code) = a.country_code
             AND a.username = current_user
        )
    );

-- Grant access to the mapping table to role_analyst
GRANT SELECT ON warehouse.analyst_country_assignment TO role_analyst;

-- ============================================================
-- END OF RBAC SETUP
-- ============================================================
