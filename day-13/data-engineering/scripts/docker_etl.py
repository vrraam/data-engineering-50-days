# scripts/docker_etl.py
import subprocess
import csv
import logging
from datetime import datetime, timedelta
import tempfile
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DockerETL:
    def __init__(self, container_name='retail-warehouse'):
        self.container_name = container_name
        self.db_user = 'dw_admin'
        self.db_name = 'retail_dw'
    
    def run_sql_command(self, sql_command):
        """Execute SQL command via docker exec"""
        cmd = [
            'docker', 'exec', '-i', self.container_name,
            'psql', '-U', self.db_user, '-d', self.db_name,
            '-c', sql_command
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"SQL command failed: {e.stderr}")
            raise
    
    def run_sql_file(self, sql_file_path):
        """Execute SQL file via docker exec"""
        cmd = [
            'docker', 'exec', '-i', self.container_name,
            'psql', '-U', self.db_user, '-d', self.db_name
        ]
        
        try:
            with open(sql_file_path, 'r') as f:
                result = subprocess.run(cmd, stdin=f, capture_output=True, text=True, check=True)
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"SQL file execution failed: {e.stderr}")
            raise
    
    def load_csv_to_staging(self, csv_file, table_name, columns):
        """Load CSV file to staging table"""
        logger.info(f"Loading {csv_file} to {table_name}")
        
        # First, clear the staging table
        self.run_sql_command(f"TRUNCATE TABLE staging.{table_name};")
        
        # Prepare the COPY command
        copy_sql = f"""
        \\copy staging.{table_name}({','.join(columns)}) FROM STDIN WITH CSV HEADER
        """
        
        # Create a temporary SQL file with the COPY command and data
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as temp_file:
            temp_file.write(copy_sql)
            temp_file.write('\n')
            
            # Add the CSV data
            with open(csv_file, 'r') as csv_f:
                temp_file.write(csv_f.read())
            
            temp_file.write('\n\\.\n')  # End of COPY data
            temp_file_path = temp_file.name
        
        try:
            self.run_sql_file(temp_file_path)
            logger.info(f"Successfully loaded {csv_file} to {table_name}")
        finally:
            os.unlink(temp_file_path)
    
    def populate_date_dimension(self):
        """Populate date dimension"""
        logger.info("Populating date dimension...")
        
        sql = """
        TRUNCATE TABLE warehouse.dim_date;
        
        INSERT INTO warehouse.dim_date (
            date_key, date_actual, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month_number, month_name,
            quarter_number, quarter_name, year_number, is_weekend,
            fiscal_year, fiscal_quarter
        )
        SELECT 
            TO_CHAR(date_actual, 'YYYYMMDD')::INTEGER as date_key,
            date_actual,
            EXTRACT(DOW FROM date_actual) + 1 as day_of_week,
            TO_CHAR(date_actual, 'Day') as day_name,
            EXTRACT(DAY FROM date_actual) as day_of_month,
            EXTRACT(DOY FROM date_actual) as day_of_year,
            EXTRACT(WEEK FROM date_actual) as week_of_year,
            EXTRACT(MONTH FROM date_actual) as month_number,
            TO_CHAR(date_actual, 'Month') as month_name,
            EXTRACT(QUARTER FROM date_actual) as quarter_number,
            'Q' || EXTRACT(QUARTER FROM date_actual) as quarter_name,
            EXTRACT(YEAR FROM date_actual) as year_number,
            CASE WHEN EXTRACT(DOW FROM date_actual) IN (0,6) THEN TRUE ELSE FALSE END as is_weekend,
            CASE 
                WHEN EXTRACT(MONTH FROM date_actual) >= 4 THEN EXTRACT(YEAR FROM date_actual)
                ELSE EXTRACT(YEAR FROM date_actual) - 1
            END as fiscal_year,
            CASE 
                WHEN EXTRACT(MONTH FROM date_actual) >= 4 THEN 
                    ((EXTRACT(MONTH FROM date_actual) - 4) / 3) + 1
                ELSE 
                    ((EXTRACT(MONTH FROM date_actual) + 8) / 3) + 1
            END as fiscal_quarter
        FROM generate_series('2020-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) AS date_actual;
        """
        
        self.run_sql_command(sql)
        logger.info("Date dimension populated successfully")
    
    def load_dimensions(self):
        """Load all dimension tables"""
        logger.info("Loading dimension tables...")
        
        # Customer dimension
        customer_sql = """
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
                WHEN birth_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN birth_date::DATE
                ELSE NULL
            END as birth_date,
            city,
            state,
            postal_code,
            country,
            segment as customer_segment,
            CASE 
                WHEN registration_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN registration_date::DATE
                ELSE NULL
            END as registration_date
        FROM staging.stg_customers
        WHERE customer_id IS NOT NULL
        ON CONFLICT (customer_id) DO NOTHING;
        """
        self.run_sql_command(customer_sql)
        logger.info("Customer dimension loaded")
        
        # Product dimension
        product_sql = """
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
                WHEN price ~ '^[0-9]+\\.?[0-9]*$'
                THEN price::DECIMAL(10,2)
                ELSE NULL
            END as list_price,
            CASE 
                WHEN cost ~ '^[0-9]+\\.?[0-9]*$'
                THEN cost::DECIMAL(10,2)
                ELSE NULL
            END as standard_cost,
            CASE 
                WHEN launch_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN launch_date::DATE
                ELSE NULL
            END as launch_date,
            description as product_description
        FROM staging.stg_products
        WHERE product_id IS NOT NULL;
        """
        self.run_sql_command(product_sql)
        logger.info("Product dimension loaded")
        
        # Store dimension
        store_sql = """
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
                WHEN size_sqft ~ '^[0-9]+$'
                THEN size_sqft::INTEGER
                ELSE NULL
            END as store_size_sqft,
            manager as store_manager,
            CASE 
                WHEN opening_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN opening_date::DATE
                ELSE NULL
            END as opening_date
        FROM staging.stg_stores
        WHERE store_id IS NOT NULL;
        """
        self.run_sql_command(store_sql)
        logger.info("Store dimension loaded")
    
    def load_facts(self):
        """Load fact tables"""
        logger.info("Loading fact tables...")
        
        # Sales fact table
        sales_sql = """
        INSERT INTO warehouse.fact_sales (
            date_key, customer_key, product_key, store_key,
            transaction_id, line_item_number, quantity_sold,
            unit_price, discount_amount, gross_amount, net_amount,
            payment_method, promotion_code
        )
        SELECT
            COALESCE(d.date_key, 19000101) as date_key,
            COALESCE(c.customer_key, -1) as customer_key,
            COALESCE(p.product_key, -1) as product_key,
            COALESCE(s.store_key, -1) as store_key,
            st.transaction_id,
            ROW_NUMBER() OVER (PARTITION BY st.transaction_id ORDER BY st.load_timestamp) as line_item_number,
            CASE 
                WHEN st.quantity ~ '^[0-9]+$'
                THEN st.quantity::INTEGER
                ELSE 1
            END as quantity_sold,
            CASE 
                WHEN st.unit_price ~ '^[0-9]+\\.?[0-9]*$'
                THEN st.unit_price::DECIMAL(10,2)
                ELSE 0
            END as unit_price,
            CASE 
                WHEN st.discount ~ '^[0-9]+\\.?[0-9]*$'
                THEN st.discount::DECIMAL(10,2)
                ELSE 0
            END as discount_amount,
            (CASE 
                WHEN st.quantity ~ '^[0-9]+$' AND st.unit_price ~ '^[0-9]+\\.?[0-9]*$'
                THEN st.quantity::INTEGER * st.unit_price::DECIMAL(10,2)
                ELSE 0
            END) as gross_amount,
            (CASE 
                WHEN st.quantity ~ '^[0-9]+$' AND st.unit_price ~ '^[0-9]+\\.?[0-9]*$' AND st.discount ~ '^[0-9]+\\.?[0-9]*$'
                THEN (st.quantity::INTEGER * st.unit_price::DECIMAL(10,2)) - st.discount::DECIMAL(10,2)
                ELSE 0
            END) as net_amount,
            st.payment_method,
            st.promotion_code
        FROM staging.stg_sales st
        LEFT JOIN warehouse.dim_date d ON d.date_actual = 
            CASE 
                WHEN st.transaction_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN st.transaction_date::DATE
                ELSE NULL
            END
        LEFT JOIN warehouse.dim_customer c ON c.customer_id = st.customer_id
            AND c.is_current = TRUE
        LEFT JOIN warehouse.dim_product p ON p.product_id = st.product_id
            AND p.is_current = TRUE
        LEFT JOIN warehouse.dim_store s ON s.store_id = st.store_id
        WHERE st.transaction_id IS NOT NULL;
        """
        self.run_sql_command(sales_sql)
        logger.info("Sales fact table loaded")
        
        # Daily summary fact
        summary_sql = """
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
        self.run_sql_command(summary_sql)
        logger.info("Daily summary fact table loaded")
    
    def run_full_etl(self):
        """Run the complete ETL process"""
        logger.info("Starting Docker-based ETL process...")
        
        try:
            # Test connection first
            result = self.run_sql_command("SELECT current_user, current_database();")
            logger.info(f"Connected successfully: {result.strip()}")
            
            # Load staging data
            logger.info("Loading staging data...")
            
            # Add file_name and load_timestamp to CSV data
            self.load_csv_to_staging(
                'data/customer_data.csv', 
                'stg_customers',
                ['customer_id', 'first_name', 'last_name', 'email', 'phone', 'gender', 
                 'birth_date', 'address', 'city', 'state', 'postal_code', 'country', 
                 'segment', 'registration_date']
            )
            
            self.load_csv_to_staging(
                'data/product_data.csv',
                'stg_products', 
                ['product_id', 'product_name', 'category', 'brand', 'price', 'cost', 
                 'description', 'launch_date']
            )
            
            self.load_csv_to_staging(
                'data/store_data.csv',
                'stg_stores',
                ['store_id', 'store_name', 'store_type', 'address', 'city', 'state', 
                 'postal_code', 'size_sqft', 'manager', 'opening_date']
            )
            
            self.load_csv_to_staging(
                'data/sales_data.csv',
                'stg_sales',
                ['transaction_id', 'transaction_date', 'customer_id', 'product_id', 
                 'store_id', 'quantity', 'unit_price', 'discount', 'payment_method', 
                 'promotion_code']
            )
            
            # Add metadata to staging tables
            self.run_sql_command("""
                UPDATE staging.stg_customers SET file_name = 'customer_data.csv', load_timestamp = CURRENT_TIMESTAMP WHERE file_name IS NULL;
                UPDATE staging.stg_products SET file_name = 'product_data.csv', load_timestamp = CURRENT_TIMESTAMP WHERE file_name IS NULL;
                UPDATE staging.stg_stores SET file_name = 'store_data.csv', load_timestamp = CURRENT_TIMESTAMP WHERE file_name IS NULL;
                UPDATE staging.stg_sales SET file_name = 'sales_data.csv', load_timestamp = CURRENT_TIMESTAMP WHERE file_name IS NULL;
            """)
            
            # Populate date dimension
            self.populate_date_dimension()
            
            # Load dimensions
            self.load_dimensions()
            
            # Load facts
            self.load_facts()
            
            logger.info("ETL process completed successfully!")
            
            # Show summary
            result = self.run_sql_command("""
                SELECT 'customers' as table_name, COUNT(*) as record_count FROM warehouse.dim_customer
                UNION ALL
                SELECT 'products', COUNT(*) FROM warehouse.dim_product
                UNION ALL  
                SELECT 'stores', COUNT(*) FROM warehouse.dim_store
                UNION ALL
                SELECT 'sales_facts', COUNT(*) FROM warehouse.fact_sales
                UNION ALL
                SELECT 'daily_summaries', COUNT(*) FROM warehouse.fact_daily_sales_summary;
            """)
            
            logger.info("Data warehouse summary:")
            logger.info(result)
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise

if __name__ == "__main__":
    etl = DockerETL()
    etl.run_full_etl()
