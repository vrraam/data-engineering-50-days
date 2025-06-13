-- Rank customers by total sales
SELECT 
    customer_name,
    SUM(sales) as total_sales,
    ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) as rank,
    NTILE(10) OVER (ORDER BY SUM(sales)) as decile
FROM superstore 
GROUP BY customer_name;

-- Compare this month's sales to last month
SELECT 
    order_date,
    sales,
    LAG(sales, 1) OVER (ORDER BY order_date) as previous_month_sales
FROM superstore;

-- Calculate 7-day moving average
SELECT 
    order_date,
    sales,
    AVG(sales) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_average
FROM superstore;

-- Clean CTE approach
WITH customer_totals AS (
    SELECT customer_id, SUM(sales) as total_sales
    FROM superstore 
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT * FROM customer_totals 
    WHERE total_sales > 1000
)
SELECT * FROM high_value_customers 
ORDER BY total_sales DESC;

-- Pre-calculate heavy computations
CREATE MATERIALIZED VIEW monthly_sales_summary AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    SUM(sales) as total_sales,
    COUNT(*) as order_count
FROM superstore 
GROUP BY DATE_TRUNC('month', order_date);