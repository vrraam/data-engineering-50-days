-- 1. CUSTOMER DIMENSION TABLE

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    segment VARCHAR(50) NOT NULL,
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '2999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);




CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON dim_customer(is_current);


-- 2. PRODUCT DIMENSION TABLE  
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(50) NOT NULL,
    sub_category VARCHAR(50) NOT NULL,
    
    -- Business classifications
    profitability_tier VARCHAR(20),
    price_tier VARCHAR(20),
    
    -- SCD Type 2 fields
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '2999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_category ON dim_product(category);
CREATE INDEX IF NOT EXISTS idx_dim_product_current ON dim_product(is_current);

-- 3. GEOGRAPHY DIMENSION TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key SERIAL PRIMARY KEY,
    country VARCHAR(50) NOT NULL,
    region VARCHAR(50) NOT NULL,
    state VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    postal_code VARCHAR(20),
    
    -- Business classifications
    market_size VARCHAR(20),
    region_code VARCHAR(10),
    
    -- SCD Type 2 fields
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '2999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_geography_region ON dim_geography(region, state);
CREATE INDEX IF NOT EXISTS idx_dim_geography_current ON dim_geography(is_current);


-- 4. SHIP MODE DIMENSION TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_ship_mode (
    ship_mode_key SERIAL PRIMARY KEY,
    ship_mode VARCHAR(50) UNIQUE NOT NULL,
    ship_category VARCHAR(30) NOT NULL,
    priority_level INTEGER,
    delivery_days_estimate INTEGER,
    cost_tier VARCHAR(20),
    
    -- SCD Type 2 fields
    effective_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE DEFAULT '2999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_ship_mode_name ON dim_ship_mode(ship_mode);
CREATE INDEX IF NOT EXISTS idx_dim_ship_mode_current ON dim_ship_mode(is_current);


-- 5. DATE DIMENSION TABLE (MOST IMPORTANT!)
-- =====================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    
    -- Day attributes
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    
    -- Week attributes
    week_of_year INTEGER NOT NULL,
    week_start_date DATE,
    week_end_date DATE,
    
    -- Month attributes
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    month_start_date DATE,
    month_end_date DATE,
    
    -- Quarter attributes
    quarter_number INTEGER NOT NULL,
    quarter_name VARCHAR(10) NOT NULL,
    quarter_start_date DATE,
    quarter_end_date DATE,
    
    -- Year attributes
    year_number INTEGER NOT NULL,
    
    -- Business attributes
    is_weekday BOOLEAN NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(50),
    
    -- Fiscal calendar (assuming July-June fiscal year)
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL,
    fiscal_month INTEGER NOT NULL,
    
    -- Relative date attributes
    is_current_day BOOLEAN DEFAULT FALSE,
    is_current_month BOOLEAN DEFAULT FALSE,
    is_current_quarter BOOLEAN DEFAULT FALSE,
    is_current_year BOOLEAN DEFAULT FALSE
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON dim_date(year_number, month_number);
CREATE INDEX IF NOT EXISTS idx_dim_date_fiscal ON dim_date(fiscal_year, fiscal_quarter);



-- 6. CENTRAL FACT TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_key SERIAL PRIMARY KEY,
    
    -- Foreign Keys to Dimensions (STAR SCHEMA CONNECTIONS)
    customer_key INTEGER REFERENCES dim_customer(customer_key),
    product_key INTEGER REFERENCES dim_product(product_key),
    geography_key INTEGER REFERENCES dim_geography(geography_key),
    ship_mode_key INTEGER REFERENCES dim_ship_mode(ship_mode_key),
    order_date_key INTEGER REFERENCES dim_date(date_key),
    ship_date_key INTEGER REFERENCES dim_date(date_key),
    
    -- Degenerate Dimensions (stored in fact table)
    order_id VARCHAR(50) NOT NULL,
    row_id INTEGER NOT NULL,
    
    -- ADDITIVE MEASURES (can be summed across all dimensions)
    sales_amount DECIMAL(12,2) NOT NULL,
    quantity INTEGER NOT NULL,
    discount_amount DECIMAL(12,2) DEFAULT 0,
    profit_amount DECIMAL(12,2) NOT NULL,
    
    -- SEMI-ADDITIVE MEASURES (can be averaged)
    unit_price DECIMAL(10,2),
    profit_margin DECIMAL(8,4),
    discount_percentage DECIMAL(5,4),
    
    -- NON-ADDITIVE MEASURES (calculated fields)
    cost_amount DECIMAL(12,2),
    
    -- PROCESS MEASURES (business process metrics)
    ship_days INTEGER,
    processing_days INTEGER,
    
    -- DATA QUALITY FLAGS
    data_quality_score INTEGER DEFAULT 100,
    has_anomaly BOOLEAN DEFAULT FALSE,
    
    -- AUDIT FIELDS
    source_system VARCHAR(50) DEFAULT 'Superstore',
    batch_id VARCHAR(50),
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add indexes for fact table performance (CRITICAL for query speed)
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_geography ON fact_sales(geography_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_ship_mode ON fact_sales(ship_mode_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_order_date ON fact_sales(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_ship_date ON fact_sales(ship_date_key);

-- Composite indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_date ON fact_sales(customer_key, order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_date ON fact_sales(product_key, order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_geography_date ON fact_sales(geography_key, order_date_key);

-- To check what tables created

SELECT table_name, table_type 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name LIKE 'dim_%' OR table_name LIKE 'fact_%'
ORDER BY table_name;


-- Check how many rows in each table
SELECT 
    'dim_customer' as table_name, COUNT(*) as row_count FROM dim_customer
UNION ALL SELECT 
    'dim_product' as table_name, COUNT(*) as row_count FROM dim_product  
UNION ALL SELECT
    'dim_geography' as table_name, COUNT(*) as row_count FROM dim_geography
UNION ALL SELECT
    'dim_ship_mode' as table_name, COUNT(*) as row_count FROM dim_ship_mode
UNION ALL SELECT
    'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL SELECT
    'fact_sales' as table_name, COUNT(*) as row_count FROM fact_sales;


-- See foreign key relationships
SELECT 
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu 
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu 
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
ORDER BY tc.table_name;


select count(*) from dim_date;


