-- advanced_analytics.sql
-- Advanced Business Intelligence Queries

-- 1. Customer RFM Analysis (Recency, Frequency, Monetary)
WITH customer_rfm AS (
    SELECT 
        c.customer_key,
        c.full_name,
        c.customer_segment,
        -- Recency (days since last purchase)
        CURRENT_DATE - MAX(d.date_actual) as days_since_last_purchase,
        
        -- Frequency (number of distinct purchase dates)
        COUNT(DISTINCT d.date_actual) as purchase_frequency,
        
        -- Monetary (total amount spent)
        SUM(f.net_amount) as total_spent,
        
        -- Additional metrics
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        AVG(f.net_amount) as avg_transaction_value
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.full_name, c.customer_segment
),
rfm_scores AS (
    SELECT *,
        -- RFM Scoring (1-5 scale)
        CASE 
            WHEN days_since_last_purchase <= 7 THEN 5
            WHEN days_since_last_purchase <= 14 THEN 4
            WHEN days_since_last_purchase <= 30 THEN 3
            WHEN days_since_last_purchase <= 60 THEN 2
            ELSE 1
        END as recency_score,
        
        CASE 
            WHEN purchase_frequency >= 5 THEN 5
            WHEN purchase_frequency >= 4 THEN 4
            WHEN purchase_frequency >= 3 THEN 3
            WHEN purchase_frequency >= 2 THEN 2
            ELSE 1
        END as frequency_score,
        
        CASE 
            WHEN total_spent >= 5000 THEN 5
            WHEN total_spent >= 2000 THEN 4
            WHEN total_spent >= 1000 THEN 3
            WHEN total_spent >= 500 THEN 2
            ELSE 1
        END as monetary_score
    FROM customer_rfm
)
SELECT 
    full_name,
    customer_segment,
    days_since_last_purchase,
    purchase_frequency,
    total_spent,
    recency_score,
    frequency_score,
    monetary_score,
    -- Customer Classification
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'New Customers'
        WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN 'Lost Customers'
        ELSE 'Standard'
    END as customer_classification
FROM rfm_scores
ORDER BY total_spent DESC;

-- 2. Product Performance with Growth Analysis
WITH monthly_product_sales AS (
    SELECT 
        p.product_name,
        p.category_l1,
        p.brand,
        d.year_number,
        d.month_number,
        SUM(f.net_amount) as monthly_revenue,
        SUM(f.quantity_sold) as monthly_quantity
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_product p ON f.product_key = p.product_key
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY p.product_name, p.category_l1, p.brand, d.year_number, d.month_number
),
product_growth AS (
    SELECT *,
        LAG(monthly_revenue) OVER (
            PARTITION BY product_name 
            ORDER BY year_number, month_number
        ) as previous_month_revenue,
        LAG(monthly_quantity) OVER (
            PARTITION BY product_name 
            ORDER BY year_number, month_number
        ) as previous_month_quantity
    FROM monthly_product_sales
)
SELECT 
    product_name,
    category_l1,
    brand,
    year_number,
    month_number,
    monthly_revenue,
    monthly_quantity,
    previous_month_revenue,
    CASE 
        WHEN previous_month_revenue > 0 THEN 
            ROUND(((monthly_revenue - previous_month_revenue) / previous_month_revenue * 100), 2)
        ELSE NULL
    END as revenue_growth_percent,
    CASE 
        WHEN previous_month_quantity > 0 THEN 
            ROUND(((monthly_quantity - previous_month_quantity) / previous_month_quantity * 100), 2)
        ELSE NULL
    END as quantity_growth_percent
FROM product_growth
WHERE previous_month_revenue IS NOT NULL
ORDER BY product_name, year_number, month_number;

-- 3. Store Efficiency Analysis with Rankings
WITH store_metrics AS (
    SELECT 
        s.store_key,
        s.store_name,
        s.city,
        s.state,
        s.store_type,
        s.store_size_sqft,
        -- Sales Metrics
        SUM(f.net_amount) as total_revenue,
        COUNT(DISTINCT f.transaction_id) as total_transactions,
        COUNT(DISTINCT f.customer_key) as unique_customers,
        SUM(f.quantity_sold) as total_items_sold,
        AVG(f.net_amount) as avg_transaction_value,
        -- Efficiency Metrics
        SUM(f.net_amount) / s.store_size_sqft as revenue_per_sqft,
        COUNT(DISTINCT f.customer_key) / COUNT(DISTINCT d.date_actual) as customers_per_day,
        SUM(f.net_amount) / COUNT(DISTINCT d.date_actual) as revenue_per_day
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_store s ON f.store_key = s.store_key
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY s.store_key, s.store_name, s.city, s.state, s.store_type, s.store_size_sqft
)
SELECT 
    store_name,
    city,
    state,
    store_type,
    store_size_sqft,
    total_revenue,
    total_transactions,
    unique_customers,
    ROUND(avg_transaction_value, 2) as avg_transaction_value,
    ROUND(revenue_per_sqft, 2) as revenue_per_sqft,
    ROUND(customers_per_day, 1) as customers_per_day,
    ROUND(revenue_per_day, 2) as revenue_per_day,
    -- Rankings
    RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
    RANK() OVER (ORDER BY revenue_per_sqft DESC) as efficiency_rank,
    RANK() OVER (ORDER BY avg_transaction_value DESC) as aov_rank,
    -- Performance Classification
    CASE 
        WHEN RANK() OVER (ORDER BY total_revenue DESC) <= 3 THEN 'Top Performer'
        WHEN RANK() OVER (ORDER BY total_revenue DESC) <= 7 THEN 'Above Average'
        ELSE 'Needs Improvement'
    END as performance_category
FROM store_metrics
ORDER BY total_revenue DESC;

-- 4. Sales Trend Analysis with Seasonality
WITH daily_sales AS (
    SELECT 
        d.date_actual,
        d.day_name,
        d.month_name,
        d.quarter_name,
        d.is_weekend,
        SUM(f.net_amount) as daily_revenue,
        COUNT(DISTINCT f.transaction_id) as daily_transactions,
        COUNT(DISTINCT f.customer_key) as daily_customers
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY d.date_actual, d.day_name, d.month_name, d.quarter_name, d.is_weekend
),
sales_with_moving_avg AS (
    SELECT *,
        AVG(daily_revenue) OVER (
            ORDER BY date_actual 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7day_avg,
        LAG(daily_revenue, 7) OVER (ORDER BY date_actual) as same_day_last_week
    FROM daily_sales
)
SELECT 
    date_actual,
    day_name,
    month_name,
    quarter_name,
    CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    daily_revenue,
    daily_transactions,
    daily_customers,
    ROUND(rolling_7day_avg, 2) as rolling_7day_avg,
    same_day_last_week,
    CASE 
        WHEN same_day_last_week > 0 THEN 
            ROUND(((daily_revenue - same_day_last_week) / same_day_last_week * 100), 2)
        ELSE NULL
    END as week_over_week_growth
FROM sales_with_moving_avg
ORDER BY date_actual;

-- 5. Customer Cohort Retention Analysis
WITH customer_first_purchase AS (
    SELECT 
        c.customer_key,
        MIN(d.date_actual) as first_purchase_date,
        DATE_TRUNC('month', MIN(d.date_actual)) as cohort_month
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    GROUP BY c.customer_key
),
customer_purchases AS (
    SELECT 
        cfp.customer_key,
        cfp.cohort_month,
        DATE_TRUNC('month', d.date_actual) as purchase_month,
        EXTRACT(MONTH FROM AGE(d.date_actual, cfp.first_purchase_date)) as months_since_first_purchase
    FROM warehouse.fact_sales f
    JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
    JOIN warehouse.dim_date d ON f.date_key = d.date_key
    JOIN customer_first_purchase cfp ON c.customer_key = cfp.customer_key
),
cohort_table AS (
    SELECT 
        cohort_month,
        months_since_first_purchase,
        COUNT(DISTINCT customer_key) as customers_active
    FROM customer_purchases
    GROUP BY cohort_month, months_since_first_purchase
),
cohort_sizes AS (
    SELECT 
        cohort_month,
        COUNT(DISTINCT customer_key) as cohort_size
    FROM customer_first_purchase
    GROUP BY cohort_month
)
SELECT 
    ct.cohort_month,
    cs.cohort_size,
    ct.months_since_first_purchase,
    ct.customers_active,
    ROUND((ct.customers_active::FLOAT / cs.cohort_size * 100), 2) as retention_rate
FROM cohort_table ct
JOIN cohort_sizes cs ON ct.cohort_month = cs.cohort_month
WHERE ct.months_since_first_purchase <= 12  -- Show first year retention
ORDER BY ct.cohort_month, ct.months_since_first_purchase;

-- 6. Promotion Effectiveness Analysis
WITH promotion_analysis AS (
    SELECT 
        f.promotion_code,
        COUNT(DISTINCT f.transaction_id) as transactions_with_promo,
        SUM(f.discount_amount) as total_discount_given,
        SUM(f.gross_amount) as gross_revenue,
        SUM(f.net_amount) as net_revenue,
        AVG(f.discount_amount) as avg_discount_per_transaction,
        AVG(f.net_amount) as avg_transaction_value,
        COUNT(DISTINCT f.customer_key) as unique_customers_using_promo
    FROM warehouse.fact_sales f
    WHERE f.promotion_code IS NOT NULL 
      AND f.promotion_code != ''
    GROUP BY f.promotion_code
),
no_promotion_baseline AS (
    SELECT 
        AVG(f.net_amount) as baseline_avg_transaction_value,
        COUNT(DISTINCT f.transaction_id) as baseline_transactions
    FROM warehouse.fact_sales f
    WHERE f.promotion_code IS NULL 
       OR f.promotion_code = ''
)
SELECT 
    pa.promotion_code,
    pa.transactions_with_promo,
    pa.total_discount_given,
    pa.net_revenue,
    ROUND(pa.avg_discount_per_transaction, 2) as avg_discount_per_transaction,
    ROUND(pa.avg_transaction_value, 2) as avg_transaction_value_with_promo,
    ROUND(npb.baseline_avg_transaction_value, 2) as baseline_avg_transaction_value,
    pa.unique_customers_using_promo,
    -- Effectiveness Metrics
    ROUND((pa.total_discount_given / pa.gross_revenue * 100), 2) as discount_rate_percent,
    ROUND(((pa.avg_transaction_value - npb.baseline_avg_transaction_value) / npb.baseline_avg_transaction_value * 100), 2) as transaction_value_lift_percent,
    ROUND((pa.net_revenue / pa.total_discount_given), 2) as revenue_per_dollar_discount
FROM promotion_analysis pa
CROSS JOIN no_promotion_baseline npb
ORDER BY pa.net_revenue DESC;