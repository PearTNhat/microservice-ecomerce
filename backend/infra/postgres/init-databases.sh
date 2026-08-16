#!/bin/bash
set -e

# Tự động tạo các database độc lập cho từng Microservice
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE ecom_user_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecom_user_db')\gexec
    SELECT 'CREATE DATABASE ecom_product_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecom_product_db')\gexec
    SELECT 'CREATE DATABASE ecom_order_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'ecom_order_db')\gexec
    GRANT ALL PRIVILEGES ON DATABASE ecom_user_db TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE ecom_product_db TO $POSTGRES_USER;
    GRANT ALL PRIVILEGES ON DATABASE ecom_order_db TO $POSTGRES_USER;
EOSQL

echo "✅ Đã khởi tạo thành công 3 databases: ecom_user_db, ecom_product_db, ecom_order_db!"
