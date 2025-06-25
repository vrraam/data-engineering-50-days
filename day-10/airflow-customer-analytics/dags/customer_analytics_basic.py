"""
Basic Customer Analytics DAG
============================
A simple workflow demonstrating Airflow concepts
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from airflow.operators.bash import BashOperator      # Updated import

# Default arguments for all tasks
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'customer_analytics_basic',
    default_args=default_args,
    description='Basic customer analytics pipeline',
    schedule=timedelta(days=1),  # Updated from schedule_interval
    catchup=False,
    tags=['customer', 'analytics', 'beginner']
)

def extract_customer_data(**context):
    """Extract customer data from CSV file"""
    import pandas as pd
    import os
    
    print("🔄 Starting data extraction...")
    
    # Create sample data
    sample_data = {
        'ID': range(1, 101),
        'Name': [f'Customer_{i}' for i in range(1, 101)],
        'Email': [f'customer{i}@email.com' for i in range(1, 101)],
        'Age': [20 + (i % 50) for i in range(1, 101)],
        'Purchase_Amount': [50 + (i * 5.5) for i in range(1, 101)],
        'Purchase_Date': ['2024-01-01' for _ in range(100)]
    }
    df = pd.DataFrame(sample_data)
    
    # Make sure data directory exists
    os.makedirs('/opt/airflow/data', exist_ok=True)
    df.to_csv('/opt/airflow/data/extracted_data.csv', index=False)
    
    print(f"📊 Extracted {len(df)} customer records")
    print("✅ Data extraction completed!")
    return len(df)

def transform_customer_data(**context):
    """Transform customer data"""
    import pandas as pd
    
    print("⚙️ Starting data transformation...")
    
    df = pd.read_csv('/opt/airflow/data/extracted_data.csv')
    
    # Simple transformations
    df['Customer_Segment'] = df['Purchase_Amount'].apply(
        lambda x: 'High Value' if x > 300 else 'Regular'
    )
    
    df['Age_Group'] = df['Age'].apply(
        lambda x: 'Young' if x < 30 else 'Middle' if x < 50 else 'Senior'
    )
    
    df.to_csv('/opt/airflow/data/transformed_data.csv', index=False)
    
    print("✅ Data transformation completed!")
    print(f"📊 Segments: {df['Customer_Segment'].value_counts().to_dict()}")
    return len(df)

def load_customer_data(**context):
    """Load customer data"""
    import pandas as pd
    import json
    
    print("💾 Starting data loading...")
    
    df = pd.read_csv('/opt/airflow/data/transformed_data.csv')
    
    summary = {
        'total_customers': len(df),
        'total_revenue': df['Purchase_Amount'].sum(),
        'segments': df['Customer_Segment'].value_counts().to_dict()
    }
    
    print(f"📈 Processed {len(df)} customers")
    print(f"💰 Total revenue: ${summary['total_revenue']:,.2f}")
    print("✅ Data loading completed!")
    return True

# Define tasks
extract_task = PythonOperator(
    task_id='extract_customer_data',
    python_callable=extract_customer_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_customer_data',
    python_callable=transform_customer_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_customer_data',
    python_callable=load_customer_data,
    dag=dag
)

health_check = BashOperator(
    task_id='health_check',
    bash_command='echo "✅ Pipeline completed successfully!"',
    dag=dag
)

# Define dependencies
extract_task >> transform_task >> load_task >> health_check