-- init-scripts/01_create_schema.sql
-- Create schemas for different layers
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS mart;

-- Set search path
SET search_path TO warehouse;

-- ===========================
-- DIMENSION TABLES
-- ===========================

-- Date Dimension (most important dimension)
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INTEGER NOT NULL,
    quarter_name VARCHAR(2) NOT NULL,
    year_number INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(200),
    email VARCHAR(255),
    phone VARCHAR(20),
    gender VARCHAR(10),
    age_group VARCHAR(20),
    birth_date DATE,
    
    -- Geographic attributes
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    region VARCHAR(100),
    
    -- Behavioral attributes
    customer_segment VARCHAR(50),
    loyalty_tier VARCHAR(20),
    registration_date DATE,
    first_purchase_date DATE,
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    
    -- Product hierarchy
    category_l1 VARCHAR(100), -- Electronics
    category_l2 VARCHAR(100), -- Mobile Phones
    category_l3 VARCHAR(100), -- Smartphones
    brand VARCHAR(100),
    manufacturer VARCHAR(100),
    
    -- Product attributes
    color VARCHAR(50),
    size VARCHAR(50),
    weight DECIMAL(10,2),
    unit_of_measure VARCHAR(20),
    
    -- Pricing information
    list_price DECIMAL(10,2),
    standard_cost DECIMAL(10,2),
    
    -- Product lifecycle
    launch_date DATE,
    discontinue_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store Dimension
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(50) NOT NULL UNIQUE,
    store_name VARCHAR(255) NOT NULL,
    store_type VARCHAR(50), -- Mall, Street, Online
    
    -- Geographic information
    address_line1 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    region VARCHAR(100),
    district VARCHAR(100),
    
    -- Store characteristics
    store_size_sqft INTEGER,
    parking_spaces INTEGER,
    number_of_employees INTEGER,
    
    -- Management
    store_manager VARCHAR(200),
    district_manager VARCHAR(200),
    
    -- Operational dates
    opening_date DATE,
    closing_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ===========================
-- FACT TABLES
-- ===========================

-- Sales Fact Table (Transaction Grain)
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key),
    
    -- Transaction identifiers
    transaction_id VARCHAR(100) NOT NULL,
    line_item_number INTEGER NOT NULL,
    
    -- Measures (Facts)
    quantity_sold INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    gross_amount DECIMAL(12,2) NOT NULL,
    tax_amount DECIMAL(10,2) DEFAULT 0,
    net_amount DECIMAL(12,2) NOT NULL,
    
    -- Cost measures
    unit_cost DECIMAL(10,2),
    total_cost DECIMAL(12,2),
    gross_profit DECIMAL(12,2),
    
    -- Additional attributes (degenerate dimensions)
    payment_method VARCHAR(50),
    promotion_code VARCHAR(50),
    sales_channel VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure grain uniqueness
    UNIQUE(transaction_id, line_item_number)
);

-- Daily Sales Summary Fact (Aggregate Fact)
CREATE TABLE fact_daily_sales_summary (
    summary_key SERIAL PRIMARY KEY,
    
    -- Dimension Keys
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key),
    
    -- Aggregated Measures
    total_transactions INTEGER NOT NULL,
    total_line_items INTEGER NOT NULL,
    total_customers INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    total_gross_amount DECIMAL(15,2) NOT NULL,
    total_discount_amount DECIMAL(15,2) NOT NULL,
    total_net_amount DECIMAL(15,2) NOT NULL,
    total_tax_amount DECIMAL(15,2) NOT NULL,
    total_cost DECIMAL(15,2) NOT NULL,
    total_profit DECIMAL(15,2) NOT NULL,
    
    -- Derived measures
    average_transaction_value DECIMAL(10,2),
    average_items_per_transaction DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure grain uniqueness
    UNIQUE(date_key, store_key)
);

-- Create indexes for performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_fact_sales_transaction ON fact_sales(transaction_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_sales_date_store ON fact_sales(date_key, store_key);
CREATE INDEX idx_fact_sales_date_product ON fact_sales(date_key, product_key);