#!/bin/bash
# ============================================================
# Tạo nhiều database và user trong PostgreSQL khi init lần đầu.
# Mount vào: /docker-entrypoint-initdb.d/init-multiple-databases.sh
# ============================================================
set -e

function create_database() {
    local database=$1
    echo "Creating database: $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
EOSQL
}

function create_user_and_grant() {
    local user=$1
    local password=$2
    local database=$3
    echo "Creating user $user and granting access to $database"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        DO \$\$
        BEGIN
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$user') THEN
                CREATE ROLE $user LOGIN PASSWORD '$password';
            END IF;
        END
        \$\$;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
        \c $database
        GRANT ALL ON SCHEMA public TO $user;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $user;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $user;
EOSQL
}

# Tạo databases từ biến POSTGRES_MULTIPLE_DATABASES
# Ví dụ: POSTGRES_MULTIPLE_DATABASES=airflow,northwind_dw
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Creating databases: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
fi

# Tạo ETL user cho warehouse
create_user_and_grant "etl_user" "etl_password" "northwind_dw"
create_user_and_grant "airflow" "airflow" "airflow"

echo "==> Database initialization COMPLETE"