-- init-scripts/02_create_staging.sql
SET search_path TO staging;

-- Staging tables mirror source system structure
CREATE TABLE stg_customers (
    customer_id VARCHAR(50),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    gender VARCHAR(10),
    birth_date VARCHAR(20), -- Raw date as string
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    segment VARCHAR(50),
    registration_date VARCHAR(20),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_products (
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(200),
    brand VARCHAR(100),
    price VARCHAR(20), -- Raw price as string
    cost VARCHAR(20), -- Raw cost as string
    description TEXT,
    launch_date VARCHAR(20),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_stores (
    store_id VARCHAR(50),
    store_name VARCHAR(255),
    store_type VARCHAR(50),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    size_sqft VARCHAR(20),
    manager VARCHAR(200),
    opening_date VARCHAR(20),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg_sales (
    transaction_id VARCHAR(100),
    transaction_date VARCHAR(20),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    store_id VARCHAR(50),
    quantity VARCHAR(20),
    unit_price VARCHAR(20),
    discount VARCHAR(20),
    payment_method VARCHAR(50),
    promotion_code VARCHAR(50),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);