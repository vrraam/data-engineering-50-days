import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class SimpleSuperStoreETL:
    def __init__(self):
        # Database connection - UPDATE YOUR USERNAME!
        self.db_connection = "postgresql://raam@localhost:5432/dataengineering"
        self.engine = create_engine(self.db_connection)
        print("🚀 ETL Pipeline initialized!")
    
    def extract_data(self, csv_path):
        """Step 1: Extract data from CSV"""
        print("\n📥 STEP 1: Extracting data from CSV...")
        try:
            df = pd.read_csv(csv_path)
            print(f"✅ Extracted {len(df)} records")
            print(f"📊 Columns: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"❌ Error extracting data: {e}")
            return None
    
    def transform_data(self, df):
        """Step 2: Transform and clean the data"""
        print("\n🔧 STEP 2: Transforming data...")
        
        # Make a copy to avoid modifying original
        df_clean = df.copy()
        
        # Clean column names
        df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_').str.replace('-', '_')
        print("✅ Column names standardized")
        
        # Convert dates
        df_clean['order_date'] = pd.to_datetime(df_clean['order_date'])
        df_clean['ship_date'] = pd.to_datetime(df_clean['ship_date'])
        print("✅ Dates converted")
        
        # Add calculated fields
        df_clean['profit_margin'] = (df_clean['profit'] / df_clean['sales'] * 100).round(2)
        df_clean['days_to_ship'] = (df_clean['ship_date'] - df_clean['order_date']).dt.days
        
        # Add time dimensions
        df_clean['order_year'] = df_clean['order_date'].dt.year
        df_clean['order_month'] = df_clean['order_date'].dt.month
        df_clean['order_quarter'] = df_clean['order_date'].dt.quarter
        df_clean['day_of_week'] = df_clean['order_date'].dt.day_name()
        print("✅ Calculated fields added")
        
        # Handle missing values
        df_clean = df_clean.fillna({
            'postal_code': 'Unknown',
            'profit_margin': 0,
            'days_to_ship': 0
        })
        print("✅ Missing values handled")
        
        print(f"📊 Final dataset shape: {df_clean.shape}")
        return df_clean
    
    def load_data(self, df, table_name='superstore_enhanced'):
        """Step 3: Load data into PostgreSQL"""
        print(f"\n📤 STEP 3: Loading data to {table_name}...")
        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            print(f"✅ Successfully loaded {len(df)} records to {table_name}")
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def create_summary_views(self):
        """Step 4: Create useful views for analysis"""
        print("\n🏗️ STEP 4: Creating summary views...")
        
        views = {
            'monthly_summary': """
                CREATE OR REPLACE VIEW monthly_summary AS
                SELECT 
                    order_year,
                    order_month,
                    COUNT(*) as total_orders,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    SUM(sales) as total_sales,
                    SUM(profit) as total_profit,
                    AVG(profit_margin) as avg_profit_margin
                FROM superstore_enhanced
                GROUP BY order_year, order_month
                ORDER BY order_year, order_month
            """,
            
            'category_performance': """
                CREATE OR REPLACE VIEW category_performance AS
                SELECT 
                    category,
                    COUNT(*) as total_orders,
                    SUM(sales) as total_sales,
                    SUM(profit) as total_profit,
                    AVG(profit_margin) as avg_margin,
                    AVG(days_to_ship) as avg_shipping_days
                FROM superstore_enhanced
                GROUP BY category
                ORDER BY total_sales DESC
            """,
            
            'top_customers': """
                CREATE OR REPLACE VIEW top_customers AS
                SELECT 
                    customer_name,
                    segment,
                    region,
                    COUNT(*) as total_orders,
                    SUM(sales) as total_sales,
                    AVG(sales) as avg_order_value,
                    SUM(profit) as total_profit
                FROM superstore_enhanced
                GROUP BY customer_name, segment, region
                ORDER BY total_sales DESC
            """
        }
        
        try:
            with self.engine.connect() as conn:
                for view_name, query in views.items():
                    conn.execute(text(query))
                    print(f"✅ Created view: {view_name}")
                conn.commit()
            print("🎉 All views created successfully!")
        except Exception as e:
            print(f"❌ Error creating views: {e}")
    
    def generate_report(self):
        """Step 5: Generate a simple business report"""
        print("\n📊 STEP 5: Generating business report...")
        
        queries = {
            "Total Business Metrics": """
                SELECT 
                    COUNT(*) as total_orders,
                    COUNT(DISTINCT customer_id) as unique_customers,
                    ROUND(SUM(sales)::numeric, 2) as total_sales,
                    ROUND(SUM(profit)::numeric, 2) as total_profit,
                    ROUND(AVG(profit_margin)::numeric, 2) as avg_profit_margin
                FROM superstore_enhanced
            """,
            
            "Top 5 Products by Sales": """
                SELECT 
                    product_name,
                    ROUND(SUM(sales)::numeric, 2) as total_sales
                FROM superstore_enhanced
                GROUP BY product_name
                ORDER BY SUM(sales) DESC
                LIMIT 5
            """,
            
            "Sales by Region": """
                SELECT 
                    region,
                    ROUND(SUM(sales)::numeric, 2) as total_sales,
                    COUNT(*) as orders
                FROM superstore_enhanced
                GROUP BY region
                ORDER BY SUM(sales) DESC
            """
        }
        
        try:
            with self.engine.connect() as conn:
                for title, query in queries.items():
                    print(f"\n📈 {title}:")
                    print("-" * 40)
                    result = conn.execute(text(query))
                    for row in result:
                        print(row)
        except Exception as e:
            print(f"❌ Error generating report: {e}")
    
    def run_full_pipeline(self, csv_path):
        """Run the complete ETL pipeline"""
        print("🎯 Starting Complete ETL Pipeline")
        print("=" * 50)
        start_time = datetime.now()
        
        # Extract
        raw_data = self.extract_data(csv_path)
        if raw_data is None:
            return False
        
        # Transform
        clean_data = self.transform_data(raw_data)
        
        # Load
        success = self.load_data(clean_data)
        if not success:
            return False
        
        # Create views
        self.create_summary_views()
        
        # Generate report
        self.generate_report()
        
        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\n🎉 Pipeline completed in {duration.total_seconds():.2f} seconds!")
        return True

# Usage
if __name__ == "__main__":
    # IMPORTANT: Update this path to your CSV file location!
    csv_file_path = "/Users/raam/Downloads/superstore_clean.csv"
    
    # Create and run pipeline
    etl = SimpleSuperStoreETL()
    success = etl.run_full_pipeline(csv_file_path)
    
    if success:
        print("\n✅ ETL Pipeline completed successfully!")
        print("\n🔍 You can now run these queries in PostgreSQL:")
        print("SELECT * FROM monthly_summary;")
        print("SELECT * FROM category_performance;")
        print("SELECT * FROM top_customers LIMIT 10;")
    else:
        print("\n❌ Pipeline failed. Check the error messages above.")