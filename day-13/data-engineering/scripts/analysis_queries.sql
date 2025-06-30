-- 1. Monthly Sales Trend
SELECT 
    d.year_number,
    d.month_name,
    SUM(f.net_amount) as total_sales,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    AVG(f.net_amount) as avg_transaction_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.year_number, d.month_number, d.month_name
ORDER BY d.year_number, d.month_number;

-- 2. Top Performing Products
SELECT 
    p.product_name,
    p.category_l1,
    p.brand,
    SUM(f.quantity_sold) as total_quantity,
    SUM(f.net_amount) as total_revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name, p.category_l1, p.brand
ORDER BY total_revenue DESC;

-- 3. Store Performance Analysis
SELECT 
    s.store_name,
    s.city,
    s.state,
    SUM(f.net_amount) as total_sales,
    COUNT(DISTINCT f.transaction_id) as total_transactions,
    COUNT(DISTINCT f.customer_key) as unique_customers,
    AVG(f.net_amount) as avg_transaction_value,
    SUM(f.net_amount) / s.store_size_sqft as sales_per_sqft
FROM warehouse.fact_sales f
JOIN warehouse.dim_store s ON f.store_key = s.store_key
GROUP BY s.store_name, s.city, s.state, s.store_size_sqft
ORDER BY total_sales DESC;

-- 4. Customer Segment Analysis
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.net_amount) as total_spent,
    AVG(f.net_amount) as avg_transaction_value,
    SUM(f.quantity_sold) as total_items_purchased
FROM warehouse.fact_sales f
JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment
ORDER BY total_spent DESC;

-- 5. Daily Sales Summary (using aggregate fact table)
SELECT 
    d.date_actual,
    d.day_name,
    SUM(ds.total_net_amount) as daily_sales,
    SUM(ds.total_transactions) as daily_transactions,
    SUM(ds.total_customers) as daily_customers,
    AVG(ds.average_transaction_value) as avg_transaction_value
FROM warehouse.fact_daily_sales_summary ds
JOIN warehouse.dim_date d ON ds.date_key = d.date_key
GROUP BY d.date_actual, d.day_name
ORDER BY d.date_actual;

-- 6. Product Category Performance
SELECT 
    p.category_l1,
    COUNT(DISTINCT p.product_key) as product_count,
    SUM(f.net_amount) as category_revenue,
    SUM(f.quantity_sold) as category_quantity,
    AVG(f.unit_price) as avg_selling_price
FROM warehouse.fact_sales f
JOIN warehouse.dim_product p ON f.product_key = p.product_key
GROUP BY p.category_l1
ORDER BY category_revenue DESC;

-- 7. Customer Geographical Analysis
SELECT 
    c.state,
    c.city,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.net_amount) as total_revenue,
    AVG(f.net_amount) as avg_customer_value
FROM warehouse.fact_sales f
JOIN warehouse.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.state, c.city
ORDER BY total_revenue DESC;

-- 8. Payment Method Analysis
SELECT 
    f.payment_method,
    COUNT(DISTINCT f.transaction_id) as transaction_count,
    SUM(f.net_amount) as total_amount,
    AVG(f.net_amount) as avg_transaction_value,
    SUM(f.discount_amount) as total_discounts
FROM warehouse.fact_sales f
WHERE f.payment_method IS NOT NULL
GROUP BY f.payment_method
ORDER BY total_amount DESC;

-- 9. Weekend vs Weekday Sales
SELECT 
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
    COUNT(DISTINCT f.transaction_id) as transactions,
    SUM(f.net_amount) as total_sales,
    AVG(f.net_amount) as avg_transaction_value,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM warehouse.fact_sales f
JOIN warehouse.dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend
ORDER BY total_sales DESC;

-- 10. Promotion Code Effectiveness
SELECT 
    f.promotion_code,
    COUNT(DISTINCT f.transaction_id) as transactions_with_promo,
    SUM(f.discount_amount) as total_discount_given,
    SUM(f.net_amount) as total_revenue,
    AVG(f.discount_amount) as avg_discount_per_transaction
FROM warehouse.fact_sales f
WHERE f.promotion_code IS NOT NULL AND f.promotion_code != ''
GROUP BY f.promotion_code
ORDER BY total_discount_given DESC;