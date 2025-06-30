# scripts/etl_pipeline.py
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import logging
from pathlib import Path
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class RetailDataWarehouseETL:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None
    
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def execute_sql(self, sql, params=None):
        """Execute SQL statement"""
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall() if cursor.description else None
    
    def load_staging_data(self, data_dir):
        """Load raw data into staging tables"""
        logger.info("Starting staging data load...")
        
        # Load customers
        customers_df = pd.read_csv(f"{data_dir}/customer_data.csv")
        self._load_dataframe_to_table(customers_df, 'staging.stg_customers', 'customer_data.csv')
        
        # Load products
        products_df = pd.read_csv(f"{data_dir}/product_data.csv")
        self._load_dataframe_to_table(products_df, 'staging.stg_products', 'product_data.csv')
        
        # Load stores
        stores_df = pd.read_csv(f"{data_dir}/store_data.csv")
        self._load_dataframe_to_table(stores_df, 'staging.stg_stores', 'store_data.csv')
        
        # Load sales
        sales_df = pd.read_csv(f"{data_dir}/sales_data.csv")
        self._load_dataframe_to_table(sales_df, 'staging.stg_sales', 'sales_data.csv')
        
        self.conn.commit()
        logger.info("Staging data load completed")
    
    def _load_dataframe_to_table(self, df, table_name, file_name):
        """Load dataframe to database table"""
        # Add metadata columns
        df['file_name'] = file_name
        df['load_timestamp'] = datetime.now()
        
        # Convert to lowercase column names
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Load to database
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Insert data
        columns = list(df.columns)
        placeholders = ','.join(['%s'] * len(columns))
        insert_sql = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES ({placeholders})
        """
        
        for _, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row))
        
        logger.info(f"Loaded {len(df)} rows into {table_name}")
    
    def populate_date_dimension(self, start_date='2020-01-01', end_date='2025-12-31'):
        """Populate date dimension with date range"""
        logger.info("Populating date dimension...")
        
        cursor = self.conn.cursor()
        
        # Clear existing data
        cursor.execute("TRUNCATE TABLE warehouse.dim_date")
        
        # Generate date range
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        current_date = start
        while current_date <= end:
            date_key = int(current_date.strftime('%Y%m%d'))
            
            # Calculate date attributes
            day_of_week = current_date.weekday() + 1  # 1=Monday, 7=Sunday
            day_name = current_date.strftime('%A')
            month_name = current_date.strftime('%B')
            quarter = (current_date.month - 1) // 3 + 1
            is_weekend = current_date.weekday() >= 5
            
            # Fiscal year (assuming April-March)
            if current_date.month >= 4:
                fiscal_year = current_date.year
            else:
                fiscal_year = current_date.year - 1
            fiscal_quarter = ((current_date.month - 4) % 12) // 3 + 1
            
            insert_sql = """
                INSERT INTO warehouse.dim_date (
                    date_key, date_actual, day_of_week, day_name, day_of_month,
                    day_of_year, week_of_year, month_number, month_name,
                    quarter_number, quarter_name, year_number, is_weekend,
                    fiscal_year, fiscal_quarter
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_sql, (
                date_key, current_date.date(), day_of_week, day_name,
                current_date.day, current_date.timetuple().tm_yday,
                current_date.isocalendar()[1], current_date.month, month_name,
                quarter, f'Q{quarter}', current_date.year, is_weekend,
                fiscal_year, fiscal_quarter
            ))
            
            current_date += timedelta(days=1)
        
        self.conn.commit()
        logger.info("Date dimension populated successfully")
    
    def transform_and_load_dimensions(self):
        """Transform staging data and load into dimension tables"""
        logger.info("Starting dimension table loads...")
        
        # Load Customer Dimension
        self._load_customer_dimension()
        
        # Load Product Dimension  
        self._load_product_dimension()
        
        # Load Store Dimension
        self._load_store_dimension()
        
        self.conn.commit()
        logger.info("Dimension tables loaded successfully")
    
    def _load_customer_dimension(self):
        """Load customer dimension with SCD Type 2"""
        logger.info("Loading customer dimension...")
        
        transform_sql = """
            INSERT INTO warehouse.dim_customer (
                customer_id, first_name, last_name, full_name, email, phone,
                gender, birth_date, city, state, postal_code, country,
                customer_segment, registration_date
            )
            SELECT DISTINCT
                customer_id,
                first_name,
                last_name,
                CONCAT(first_name, ' ', last_name) as full_name,
                email,
                phone,
                gender,
                CASE 
                    WHEN birth_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}
                    THEN birth_date::DATE
                    ELSE NULL
                END as birth_date,
                city,
                state,
                postal_code,
                country,
                segment as customer_segment,
                CASE 
                    WHEN registration_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}
                    THEN registration_date::DATE
                    ELSE NULL
                END as registration_date
            FROM staging.stg_customers
            WHERE customer_id IS NOT NULL
            ON CONFLICT (customer_id) DO NOTHING;
        """
        self.execute_sql(transform_sql)
        logger.info("Customer dimension loaded")
    
    def _load_product_dimension(self):
        """Load product dimension with hierarchical categories"""
        logger.info("Loading product dimension...")
        
        transform_sql = """
            INSERT INTO warehouse.dim_product (
                product_id, product_name, category_l1, brand, list_price,
                standard_cost, launch_date, product_description
            )
            SELECT DISTINCT
                product_id,
                product_name,
                SPLIT_PART(category, '>', 1) as category_l1,
                brand,
                CASE 
                    WHEN price ~ '^[0-9]+\.?[0-9]*
                    THEN price::DECIMAL(10,2)
                    ELSE NULL
                END as list_price,
                CASE 
                    WHEN cost ~ '^[0-9]+\.?[0-9]*
                    THEN cost::DECIMAL(10,2)
                    ELSE NULL
                END as standard_cost,
                CASE 
                    WHEN launch_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}
                    THEN launch_date::DATE
                    ELSE NULL
                END as launch_date,
                description as product_description
            FROM staging.stg_products
            WHERE product_id IS NOT NULL;
        """
        self.execute_sql(transform_sql)
        logger.info("Product dimension loaded")
    
    def _load_store_dimension(self):
        """Load store dimension"""
        logger.info("Loading store dimension...")
        
        transform_sql = """
            INSERT INTO warehouse.dim_store (
                store_id, store_name, store_type, city, state, postal_code,
                store_size_sqft, store_manager, opening_date
            )
            SELECT DISTINCT
                store_id,
                store_name,
                store_type,
                city,
                state,
                postal_code,
                CASE 
                    WHEN size_sqft ~ '^[0-9]+
                    THEN size_sqft::INTEGER
                    ELSE NULL
                END as store_size_sqft,
                manager as store_manager,
                CASE 
                    WHEN opening_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}
                    THEN opening_date::DATE
                    ELSE NULL
                END as opening_date
            FROM staging.stg_stores
            WHERE store_id IS NOT NULL;
        """
        self.execute_sql(transform_sql)
        logger.info("Store dimension loaded")
    
    def load_fact_tables(self):
        """Load fact tables with proper dimension key lookups"""
        logger.info("Loading fact tables...")
        
        # Load Sales Facts
        self._load_sales_facts()
        
        # Generate Daily Summary Facts
        self._load_daily_summary_facts()
        
        self.conn.commit()
        logger.info("Fact tables loaded successfully")
    
    def _load_sales_facts(self):
        """Load sales fact table"""
        logger.info("Loading sales fact table...")
        
        transform_sql = """
            INSERT INTO warehouse.fact_sales (
                date_key, customer_key, product_key, store_key,
                transaction_id, line_item_number, quantity_sold,
                unit_price, discount_amount, gross_amount, net_amount,
                payment_method, promotion_code
            )
            SELECT
                -- Date key lookup
                COALESCE(d.date_key, 19000101) as date_key,
                
                -- Dimension key lookups
                COALESCE(c.customer_key, -1) as customer_key,
                COALESCE(p.product_key, -1) as product_key,
                COALESCE(s.store_key, -1) as store_key,
                
                -- Transaction details
                st.transaction_id,
                ROW_NUMBER() OVER (PARTITION BY st.transaction_id ORDER BY st.load_timestamp) as line_item_number,
                
                -- Measures
                CASE 
                    WHEN st.quantity ~ '^[0-9]+
                    THEN st.quantity::INTEGER
                    ELSE 1
                END as quantity_sold,
                
                CASE 
                    WHEN st.unit_price ~ '^[0-9]+\.?[0-9]*
                    THEN st.unit_price::DECIMAL(10,2)
                    ELSE 0
                END as unit_price,
                
                CASE 
                    WHEN st.discount ~ '^[0-9]+\.?[0-9]*
                    THEN st.discount::DECIMAL(10,2)
                    ELSE 0
                END as discount_amount,
                
                -- Calculated measures
                (CASE 
                    WHEN st.quantity ~ '^[0-9]+ AND st.unit_price ~ '^[0-9]+\.?[0-9]*
                    THEN st.quantity::INTEGER * st.unit_price::DECIMAL(10,2)
                    ELSE 0
                END) as gross_amount,
                
                (CASE 
                    WHEN st.quantity ~ '^[0-9]+ AND st.unit_price ~ '^[0-9]+\.?[0-9]* AND st.discount ~ '^[0-9]+\.?[0-9]*
                    THEN (st.quantity::INTEGER * st.unit_price::DECIMAL(10,2)) - st.discount::DECIMAL(10,2)
                    ELSE 0
                END) as net_amount,
                
                st.payment_method,
                st.promotion_code
                
            FROM staging.stg_sales st
            
            -- Date dimension lookup
            LEFT JOIN warehouse.dim_date d ON d.date_actual = 
                CASE 
                    WHEN st.transaction_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}
                    THEN st.transaction_date::DATE
                    ELSE NULL
                END
            
            -- Customer dimension lookup
            LEFT JOIN warehouse.dim_customer c ON c.customer_id = st.customer_id
                AND c.is_current = TRUE
                
            -- Product dimension lookup  
            LEFT JOIN warehouse.dim_product p ON p.product_id = st.product_id
                AND p.is_current = TRUE
                
            -- Store dimension lookup
            LEFT JOIN warehouse.dim_store s ON s.store_id = st.store_id
            
            WHERE st.transaction_id IS NOT NULL;
        """
        self.execute_sql(transform_sql)
        logger.info("Sales fact table loaded")
    
    def _load_daily_summary_facts(self):
        """Generate daily summary facts from transaction facts"""
        logger.info("Loading daily summary fact table...")
        
        transform_sql = """
            INSERT INTO warehouse.fact_daily_sales_summary (
                date_key, store_key, total_transactions, total_line_items,
                total_customers, total_quantity, total_gross_amount,
                total_discount_amount, total_net_amount,
                average_transaction_value, average_items_per_transaction
            )
            SELECT
                fs.date_key,
                fs.store_key,
                COUNT(DISTINCT fs.transaction_id) as total_transactions,
                COUNT(*) as total_line_items,
                COUNT(DISTINCT fs.customer_key) as total_customers,
                SUM(fs.quantity_sold) as total_quantity,
                SUM(fs.gross_amount) as total_gross_amount,
                SUM(fs.discount_amount) as total_discount_amount,
                SUM(fs.net_amount) as total_net_amount,
                AVG(fs.net_amount) as average_transaction_value,
                AVG(fs.quantity_sold) as average_items_per_transaction
            FROM warehouse.fact_sales fs
            GROUP BY fs.date_key, fs.store_key;
        """
        self.execute_sql(transform_sql)
        logger.info("Daily summary fact table loaded")
    
    def run_full_etl(self, data_dir):
        """Run complete ETL process"""
        logger.info("Starting full ETL process...")
        
        try:
            self.connect_db()
            
            # Step 1: Load staging data
            self.load_staging_data(data_dir)
            
            # Step 2: Populate date dimension
            self.populate_date_dimension()
            
            # Step 3: Load dimension tables
            self.transform_and_load_dimensions()
            
            # Step 4: Load fact tables
            self.load_fact_tables()
            
            logger.info("ETL process completed successfully!")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            if self.conn:
                self.conn.close()

# Usage example
if __name__ == "__main__":
    db_config = {
        'host': '127.0.0.1',  # Use IPv4 explicitly
        'port': 5432,
        'database': 'retail_dw',
        'user': 'dw_admin',
        'password': 'warehouse123'
    }
    
    etl = RetailDataWarehouseETL(db_config)
    etl.run_full_etl('data')