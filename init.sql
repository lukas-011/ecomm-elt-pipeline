CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE raw.orders (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    total_price NUMERIC,
    currency VARCHAR(3),
    financial_status VARCHAR(50),
    line_items JSONB,
    order_number INT
);

CREATE TABLE raw.products (
    id BIGINT PRIMARY KEY,
    title VARCHAR(255),
    vendor VARCHAR(255),
    product_type VARCHAR(100),
    created_at TIMESTAMP,
    handle VARCHAR(255),
    variants JSONB,
    options JSONB
);