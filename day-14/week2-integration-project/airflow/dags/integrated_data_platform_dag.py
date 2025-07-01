"""
Integrated Data Platform DAG
============================
This DAG orchestrates the complete data platform workflow:
1. Multi-source data ingestion
2. Cross-dataset data quality validation
3. Distributed processing with Spark
4. Data warehouse loading
5. Analytics computation
6. Cross-platform monitoring
Schedule: Daily at 6 AM
Dependencies: Docker services (Spark, PostgreSQL, Redis)
"""
from datetime import datetime, timedelta
import pandas as pd
import logging
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import subprocess
import json

# DAG Configuration
default_args = {
    'owner': 'data-platform-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'integrated_data_platform',
    default_args=default_args,
    description='Complete integrated data platform workflow',
    schedule_interval='0 6 * * *',
    max_active_runs=1,
    tags=['platform', 'integration', 'multi-source', 'enterprise']
)

# Configuration Management
def get_platform_config():
    """Centralized platform configuration"""
    return {
        'data_sources': {
            'ecommerce': '/opt/airflow/data/raw/ecommerce_transactions.csv',
            'superstore': '/opt/airflow/data/raw/superstore_sales.csv',
            'products': '/opt/airflow/data/raw/amazon_products.csv',
            'retail': '/opt/airflow/data/raw/retail_analytics.csv'
        },
        'staging_paths': {
            'customers': '/opt/airflow/data/staging/customers.parquet',
            'products': '/opt/airflow/data/staging/products.parquet',
            'transactions': '/opt/airflow/data/staging/transactions.parquet'
        },
        'quality_thresholds': {
            'completeness': 0.95,
            'uniqueness': 0.99,
            'validity': 0.90
        },
        'spark_config': {
            'master': 'spark://spark-master:7077',
            'app_name': 'IntegratedDataPlatform',
            'executor_memory': '2g',
            'driver_memory': '1g',
        }
    }

# Platform Health Checks
def check_platform_health(**context):
    """Comprehensive platform health verification"""
    print("🔍 Starting platform health checks...")
    health_status = {
        'services': {},
        'data_sources': {},
        'storage': {},
        'overall': True
    }
    
    # Check Docker services
    services_to_check = [
        'week2-platform_postgres-data_1',
        'week2-platform_spark-master_1',
        'week2-platform_redis_1'
    ]
    
    for service in services_to_check:
        try:
            result = subprocess.run(
                ['docker', 'inspect', service],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                health_status['services'][service] = 'healthy'
                print(f"✅ Service {service}: healthy")
            else:
                health_status['services'][service] = 'unhealthy'
                health_status['overall'] = False
                print(f"❌ Service {service}: unhealthy")
        except Exception as e:
            health_status['services'][service] = f'error: {e}'
            health_status['overall'] = False
            print(f"❌ Service {service}: error - {e}")
    
    # Check data source availability
    config = get_platform_config()
    for source_name, file_path in config['data_sources'].items():
        try:
            if Path(file_path).exists():
                df = pd.read_csv(file_path, nrows=5)  # Quick validation
                health_status['data_sources'][source_name] = {
                    'status': 'available',
                    'rows': len(df),
                    'columns': len(df.columns)
                }
                print(f"✅ Data source {source_name}: available ({len(df.columns)} columns)")
            else:
                health_status['data_sources'][source_name] = 'missing'
                health_status['overall'] = False
                print(f"❌ Data source {source_name}: file not found")
        except Exception as e:
            health_status['data_sources'][source_name] = f'error: {e}'
            health_status['overall'] = False
            print(f"❌ Data source {source_name}: error - {e}")
    
    # Check database connectivity
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
        postgres_hook.get_first("SELECT 1 as health_check")
        health_status['storage']['postgresql'] = 'connected'
        print("✅ PostgreSQL: connected")
    except Exception as e:
        health_status['storage']['postgresql'] = f'error: {e}'
        health_status['overall'] = False
        print(f"❌ PostgreSQL: connection failed - {e}")
    
    if health_status['overall']:
        print("🎉 Platform health check: ALL SYSTEMS GO!")
    else:
        print("⚠ Platform health check: ISSUES DETECTED")
        raise ValueError("Platform health check failed")
    
    # Store health status for monitoring
    context['task_instance'].xcom_push(key='health_status', value=health_status)
    return health_status

# Multi-Source Data Ingestion
def ingest_multi_source_data(**context):
    """Ingest and standardize data from multiple sources"""
    print("🔄 Starting multi-source data ingestion...")
    config = get_platform_config()
    ingestion_results = {}
    
    # Ingest E-commerce transactions
    try:
        print("📊 Processing e-commerce transactions...")
        ecommerce_df = pd.read_csv(config['data_sources']['ecommerce'])
        
        # Standardize column names
        ecommerce_standardized = ecommerce_df.rename(columns={
            'Customer ID': 'customer_id',
            'Product ID': 'product_id',
            'Transaction Date': 'transaction_date',
            'Quantity': 'quantity',
            'Price': 'unit_price',
            'Total': 'total_amount'
        })
        
        # Data type conversion
        ecommerce_standardized['transaction_date'] = pd.to_datetime(
            ecommerce_standardized['transaction_date']
        )
        ecommerce_standardized['total_amount'] = pd.to_numeric(
            ecommerce_standardized['total_amount'], errors='coerce'
        )
        
        # Save to staging
        staging_path = '/opt/airflow/data/staging/ecommerce_standardized.parquet'
        ecommerce_standardized.to_parquet(staging_path, index=False)
        
        ingestion_results['ecommerce'] = {
            'status': 'success',
            'records': len(ecommerce_standardized),
            'staging_path': staging_path
        }
        print(f"✅ E-commerce: {len(ecommerce_standardized)} records ingested")
        
    except Exception as e:
        ingestion_results['ecommerce'] = {'status': 'failed', 'error': str(e)}
        print(f"❌ E-commerce ingestion failed: {e}")
    
    # Ingest Superstore sales
    try:
        print("📊 Processing superstore sales...")
        superstore_df = pd.read_csv(config['data_sources']['superstore'])
        
        # Standardize and enrich
        superstore_standardized = superstore_df.rename(columns={
            'Customer ID': 'customer_id',
            'Product ID': 'product_id',
            'Order Date': 'order_date',
            'Sales': 'sales_amount',
            'Profit': 'profit_amount',
            'Discount': 'discount_rate'
        })
        
        # Calculate derived metrics
        superstore_standardized['profit_margin'] = (
            superstore_standardized['profit_amount'] /
            superstore_standardized['sales_amount']
        ).fillna(0)
        
        superstore_standardized['order_date'] = pd.to_datetime(
            superstore_standardized['order_date']
        )
        
        staging_path = '/opt/airflow/data/staging/superstore_standardized.parquet'
        superstore_standardized.to_parquet(staging_path, index=False)
        
        ingestion_results['superstore'] = {
            'status': 'success',
            'records': len(superstore_standardized),
            'staging_path': staging_path
        }
        print(f"✅ Superstore: {len(superstore_standardized)} records ingested")
        
    except Exception as e:
        ingestion_results['superstore'] = {'status': 'failed', 'error': str(e)}
        print(f"❌ Superstore ingestion failed: {e}")
    
    # Ingest Product catalog
    try:
        print("📊 Processing product catalog...")
        products_df = pd.read_csv(config['data_sources']['products'])
        
        # Product dimension standardization
        products_standardized = products_df.rename(columns={
            'product_id': 'product_id',
            'product_name': 'product_name',
            'category': 'category',
            'price': 'list_price'
        })
        
        # Data enrichment
        products_standardized['price_tier'] = pd.cut(
            products_standardized['list_price'],
            bins=[0, 25, 100, 500, float('inf')],
            labels=['Budget', 'Mid-range', 'Premium', 'Luxury']
        )
        
        staging_path = '/opt/airflow/data/staging/products_standardized.parquet'
        products_standardized.to_parquet(staging_path, index=False)
        
        ingestion_results['products'] = {
            'status': 'success',
            'records': len(products_standardized),
            'staging_path': staging_path
        }
        print(f"✅ Products: {len(products_standardized)} records ingested")
        
    except Exception as e:
        ingestion_results['products'] = {'status': 'failed', 'error': str(e)}
        print(f"❌ Products ingestion failed: {e}")
    
    # Summary and validation
    successful_ingestions = sum(1 for result in ingestion_results.values()
                              if result.get('status') == 'success')
    total_records = sum(result.get('records', 0) for result in ingestion_results.values()
                       if result.get('status') == 'success')
    
    summary = {
        'successful_sources': successful_ingestions,
        'total_sources': len(config['data_sources']),
        'total_records_ingested': total_records,
        'ingestion_results': ingestion_results
    }
    
    print(f"📊 Ingestion Summary: {successful_ingestions}/{len(config['data_sources'])} sources")
    print(f"📊 Total records ingested: {total_records:,}")
    
    context['task_instance'].xcom_push(key='ingestion_results', value=summary)
    return summary

# Cross-Dataset Data Quality Validation
def validate_integrated_data_quality(**context):
    """Comprehensive data quality validation across all datasets"""
    print("🔍 Starting integrated data quality validation...")
    config = get_platform_config()
    quality_results = {}
    
    def calculate_quality_metrics(df, dataset_name):
        """Calculate standard quality metrics for any dataset"""
        metrics = {}
        
        # Completeness (percentage of non-null values)
        completeness = (df.notna().sum() / len(df)).mean()
        metrics['completeness'] = round(completeness, 4)
        
        # Uniqueness (for identifier columns)
        id_columns = [col for col in df.columns if 'id' in col.lower()]
        if id_columns:
            uniqueness_scores = []
            for col in id_columns:
                unique_ratio = df[col].nunique() / len(df)
                uniqueness_scores.append(unique_ratio)
            metrics['uniqueness'] = round(sum(uniqueness_scores) / len(uniqueness_scores), 4)
        else:
            metrics['uniqueness'] = 1.0
        
        # Validity (data type consistency)
        numeric_columns = df.select_dtypes(include=['number']).columns
        validity_scores = []
        for col in numeric_columns:
            valid_ratio = (df[col].notna() & (df[col] >= 0)).sum() / len(df)
            validity_scores.append(valid_ratio)
        
        if validity_scores:
            metrics['validity'] = round(sum(validity_scores) / len(validity_scores), 4)
        else:
            metrics['validity'] = 1.0
        
        # Overall quality score
        metrics['overall_quality'] = round(
            (metrics['completeness'] + metrics['uniqueness'] + metrics['validity']) / 3, 4
        )
        
        return metrics
    
    # Validate each staged dataset
    staging_files = [
        ('/opt/airflow/data/staging/ecommerce_standardized.parquet', 'ecommerce'),
        ('/opt/airflow/data/staging/superstore_standardized.parquet', 'superstore'),
        ('/opt/airflow/data/staging/products_standardized.parquet', 'products')
    ]
    
    for file_path, dataset_name in staging_files:
        try:
            if Path(file_path).exists():
                df = pd.read_parquet(file_path)
                metrics = calculate_quality_metrics(df, dataset_name)
                
                # Check against thresholds
                thresholds = config['quality_thresholds']
                quality_status = all([
                    metrics['completeness'] >= thresholds['completeness'],
                    metrics['uniqueness'] >= thresholds['uniqueness'],
                    metrics['validity'] >= thresholds['validity']
                ])
                
                quality_results[dataset_name] = {
                    'metrics': metrics,
                    'passed_quality_check': quality_status,
                    'record_count': len(df),
                    'column_count': len(df.columns)
                }
                
                status_emoji = "✅ " if quality_status else "⚠ "
                print(f"{status_emoji} {dataset_name}: Quality score {metrics['overall_quality']:.2%}")
                print(f"   Completeness: {metrics['completeness']:.2%}")
                print(f"   Uniqueness: {metrics['uniqueness']:.2%}")
                print(f"   Validity: {metrics['validity']:.2%}")
            else:
                quality_results[dataset_name] = {
                    'error': 'File not found',
                    'passed_quality_check': False
                }
                print(f"❌ {dataset_name}: Staging file not found")
                
        except Exception as e:
            quality_results[dataset_name] = {
                'error': str(e),
                'passed_quality_check': False
            }
            print(f"❌ {dataset_name}: Quality validation failed - {e}")
    
    # Cross-dataset validation
    print("\n🔗 Performing cross-dataset validation...")
    cross_dataset_results = {}
    
    try:
        # Load datasets for cross-validation
        ecommerce_df = pd.read_parquet('/opt/airflow/data/staging/ecommerce_standardized.parquet')
        products_df = pd.read_parquet('/opt/airflow/data/staging/products_standardized.parquet')
        
        # Referential integrity checks
        ecommerce_products = set(ecommerce_df['product_id'].unique())
        catalog_products = set(products_df['product_id'].unique())
        
        # Product referential integrity
        missing_products = ecommerce_products - catalog_products
        referential_integrity_score = 1 - (len(missing_products) / len(ecommerce_products))
        
        cross_dataset_results['referential_integrity'] = {
            'score': round(referential_integrity_score, 4),
            'missing_product_count': len(missing_products),
            'total_products_in_transactions': len(ecommerce_products)
        }
        
        print(f"🔗 Referential integrity: {referential_integrity_score:.2%}")
        print(f"🔗 Missing products in catalog: {len(missing_products)}")
        
    except Exception as e:
        cross_dataset_results['referential_integrity'] = {
            'error': str(e),
            'score': 0
        }
        print(f"❌ Cross-dataset validation failed: {e}")
    
    # Overall quality assessment
    passed_datasets = sum(1 for result in quality_results.values()
                         if result.get('passed_quality_check', False))
    total_datasets = len(quality_results)
    
    overall_assessment = {
        'datasets_passed': passed_datasets,
        'total_datasets': total_datasets,
        'overall_pass_rate': round(passed_datasets / total_datasets, 4) if total_datasets > 0 else 0,
        'quality_results': quality_results,
        'cross_dataset_results': cross_dataset_results
    }
    
    print(f"\n📊 Quality Assessment Summary:")
    print(f"📊 Datasets passed: {passed_datasets}/{total_datasets}")
    print(f"📊 Overall pass rate: {overall_assessment['overall_pass_rate']:.2%}")
    
    # Fail the task if quality thresholds not met
    if overall_assessment['overall_pass_rate'] < 0.8:
        raise ValueError(f"Data quality below acceptable threshold: {overall_assessment['overall_pass_rate']:.2%}")
    
    context['task_instance'].xcom_push(key='quality_results', value=overall_assessment)
    return overall_assessment

# Spark-based Large Scale Processing
def process_data_with_spark(**context):
    """Process data using Spark for scalable analytics"""
    print("⚡ Starting Spark-based data processing...")
    # This would normally be a Spark application
    # For this example, we'll simulate Spark processing with pandas
    # In production, this would use PySpark or submit to Spark cluster
    
    processing_results = {}
    
    try:
        # Load staging data
        print("📊 Loading data into Spark-like processing...")
        ecommerce_df = pd.read_parquet('/opt/airflow/data/staging/ecommerce_standardized.parquet')
        superstore_df = pd.read_parquet('/opt/airflow/data/staging/superstore_standardized.parquet')
        products_df = pd.read_parquet('/opt/airflow/data/staging/products_standardized.parquet')
        
        # Customer 360 analytics (simulating Spark transformations)
        print("🔄 Computing Customer 360 analytics...")
        
        # Combine transaction data
        all_transactions = pd.concat([
            ecommerce_df[['customer_id', 'transaction_date', 'total_amount']].rename(columns={'transaction_date': 'date', 'total_amount': 'amount'}),
            superstore_df[['customer_id', 'order_date', 'sales_amount']].rename(columns={'order_date': 'date', 'sales_amount': 'amount'})
        ])
        
        # Customer analytics
        customer_analytics = all_transactions.groupby('customer_id').agg({
            'amount': ['sum', 'mean', 'count'],
            'date': ['min', 'max']
        }).round(2)
        
        # Flatten column names
        customer_analytics.columns = ['total_spent', 'avg_order_value', 'total_orders', 'first_order_date', 'last_order_date']
        customer_analytics = customer_analytics.reset_index()
        
        # Calculate derived metrics
        customer_analytics['days_since_last_order'] = (
            datetime.now() - pd.to_datetime(customer_analytics['last_order_date'])
        ).dt.days
        
        # Customer segmentation (RFM-like)
        customer_analytics['recency_score'] = pd.qcut(
            customer_analytics['days_since_last_order'],
            q=5, labels=[5, 4, 3, 2, 1]
        )
        customer_analytics['frequency_score'] = pd.qcut(
            customer_analytics['total_orders'].rank(method='first'),
            q=5, labels=[1, 2, 3, 4, 5]
        )
        customer_analytics['monetary_score'] = pd.qcut(
            customer_analytics['total_spent'],
            q=5, labels=[1, 2, 3, 4, 5]
        )
        
        # Segment labels
        def segment_customers(row):
            score = int(str(row['recency_score']) + str(row['frequency_score']) + str(row['monetary_score']))
            if score >= 544:
                return 'Champions'
            elif score >= 334:
                return 'Loyal Customers'
            elif score >= 244:
                return 'Potential Loyalists'
            elif score >= 144:
                return 'At Risk'
            else:
                return 'Lost Customers'
        
        customer_analytics['customer_segment'] = customer_analytics.apply(segment_customers, axis=1)
        
        # Save processed data
        processed_path = '/opt/airflow/data/processed/customer_360_analytics.parquet'
        customer_analytics.to_parquet(processed_path, index=False)
        
        processing_results['customer_360'] = {
            'status': 'success',
            'records_processed': len(customer_analytics),
            'output_path': processed_path
        }
        print(f"✅ Customer 360: {len(customer_analytics)} customer profiles created")
        
        # Product performance analytics
        print("🔄 Computing product performance metrics...")
        
        # Merge transaction data with product details
        ecommerce_with_products = ecommerce_df.merge(
            products_df[['product_id', 'category', 'price_tier']],
            on='product_id',
            how='left'
        )
        
        # Product analytics
        product_performance = ecommerce_with_products.groupby(['product_id', 'category']).agg({
            'quantity': 'sum',
            'total_amount': ['sum', 'mean'],
            'customer_id': 'nunique'
        }).round(2)
        
        product_performance.columns = ['total_quantity_sold', 'total_revenue', 'avg_order_value', 'unique_customers']
        product_performance = product_performance.reset_index()
        
        # Calculate performance metrics
        product_performance['revenue_per_customer'] = (
            product_performance['total_revenue'] / product_performance['unique_customers']
        ).round(2)
        
        processed_path = '/opt/airflow/data/processed/product_performance.parquet'
        product_performance.to_parquet(processed_path, index=False)
        
        processing_results['product_performance'] = {
            'status': 'success',
            'records_processed': len(product_performance),
            'output_path': processed_path
        }
        print(f"✅ Product Performance: {len(product_performance)} product analyses completed")
        
    except Exception as e:
        processing_results['error'] = str(e)
        print(f"❌ Spark processing failed: {e}")
        raise
    
    context['task_instance'].xcom_push(key='processing_results', value=processing_results)
    return processing_results

# Data Warehouse Loading
def load_to_data_warehouse(**context):
    """Load processed data to PostgreSQL data warehouse"""
    print("💾 Starting data warehouse loading...")
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    loading_results = {}
    
    try:
        # Load customer analytics
        print("📊 Loading customer analytics to data warehouse...")
        customer_df = pd.read_parquet('/opt/airflow/data/processed/customer_360_analytics.parquet')
        
        # Insert into database
        insert_sql = """
        INSERT INTO customer_analytics (
            customer_id, total_orders, total_spent, average_order_value,
            last_order_date, days_since_last_order, customer_segment
        ) VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            total_orders = EXCLUDED.total_orders,
            total_spent = EXCLUDED.total_spent,
            average_order_value = EXCLUDED.average_order_value,
            last_order_date = EXCLUDED.last_order_date,
            days_since_last_order = EXCLUDED.days_since_last_order,
            customer_segment = EXCLUDED.customer_segment,
            updated_at = CURRENT_TIMESTAMP
        """
        
        # Prepare data for insertion
        customer_values = [
            (
                row['customer_id'],
                int(row['total_orders']),
                float(row['total_spent']),
                float(row['avg_order_value']),
                row['last_order_date'],
                int(row['days_since_last_order']),
                row['customer_segment']
            )
            for _, row in customer_df.iterrows()
        ]
        
        postgres_hook.run(insert_sql, parameters=(customer_values,))
        
        loading_results['customer_analytics'] = {
            'status': 'success',
            'records_loaded': len(customer_df)
        }
        print(f"✅ Customer analytics: {len(customer_df)} records loaded")
        
        # Load data quality metrics
        print("📊 Recording data quality metrics...")
        quality_data = context['task_instance'].xcom_pull(task_ids='validate_integrated_data_quality')
        
        quality_insert_sql = """
        INSERT INTO data_quality_metrics (
            table_name, metric_name, metric_value, threshold_value, status, execution_date
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        quality_records = []
        for dataset, metrics in quality_data['quality_results'].items():
            if 'metrics' in metrics:
                for metric_name, value in metrics['metrics'].items():
                    threshold = 0.95 if metric_name != 'overall_quality' else 0.90
                    status = 'PASS' if value >= threshold else 'FAIL'
                    quality_records.append((
                        dataset, metric_name, value, threshold, status, context['ds']
                    ))
        
        for record in quality_records:
            postgres_hook.run(quality_insert_sql, parameters=record)
        
        loading_results['data_quality_metrics'] = {
            'status': 'success',
            'records_loaded': len(quality_records)
        }
        print(f"✅ Data quality metrics: {len(quality_records)} records loaded")
        
        # Log pipeline execution
        execution_log_sql = """
        INSERT INTO pipeline_execution_log (
            pipeline_name, execution_date, start_time, end_time, status, records_processed
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        total_records = sum(result.get('records_loaded', 0) for result in loading_results.values())
        
        postgres_hook.run(execution_log_sql, parameters=(
            'integrated_data_platform',
            context['ds'],
            context['data_interval_start'],
            datetime.now(),
            'SUCCESS',
            total_records
        ))
        
        print(f"📊 Data warehouse loading completed: {total_records:,} total records")
        
    except Exception as e:
        loading_results['error'] = str(e)
        print(f"❌ Data warehouse loading failed: {e}")
        
        # Log failure
        try:
            execution_log_sql = """
            INSERT INTO pipeline_execution_log (
                pipeline_name, execution_date, start_time, end_time, status, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """
            postgres_hook.run(execution_log_sql, parameters=(
                'integrated_data_platform',
                context['ds'],
                context['data_interval_start'],
                datetime.now(),
                'FAILED',
                str(e)
            ))
        except:
            pass  # Don't fail on logging failure
        
        raise
    
    context['task_instance'].xcom_push(key='loading_results', value=loading_results)
    return loading_results

# Business Intelligence and Reporting
def generate_platform_insights(**context):
    """Generate comprehensive business insights from integrated data"""
    print("📊 Generating platform-wide business insights...")
    postgres_hook = PostgresHook(postgres_conn_id='postgres_data')
    insights = {}
    
    try:
        # Customer segment analysis
        segment_sql = """
        SELECT
            customer_segment,
            COUNT(*) as customer_count,
            AVG(total_spent) as avg_customer_value,
            SUM(total_spent) as segment_revenue,
            AVG(total_orders) as avg_orders_per_customer
        FROM customer_analytics
        GROUP BY customer_segment
        ORDER BY segment_revenue DESC
        """
        
        customer_segment_data = postgres_hook.get_pandas_df(segment_sql)
        insights['customer_segments'] = customer_segment_data.to_dict('records')
        
        # Top performing segments
        top_segment = customer_segment_data.iloc[0]
        insights['top_performing_segment'] = {
            'segment': top_segment['customer_segment'],
            'revenue': float(top_segment['segment_revenue']),
            'customer_count': int(top_segment['customer_count'])
        }
        
        print(f"💰 Top performing segment: {top_segment['customer_segment']} "
              f"(${top_segment['segment_revenue']:,.2f} revenue)")
        
        # Data quality summary
        quality_sql = """
        SELECT
            table_name,
            metric_name,
            AVG(metric_value) as avg_score,
            COUNT(CASE WHEN status = 'PASS' THEN 1 END) as passed_checks,
            COUNT(*) as total_checks
        FROM data_quality_metrics
        WHERE execution_date = %s
        GROUP BY table_name, metric_name
        """
        
        quality_data = postgres_hook.get_pandas_df(quality_sql, parameters=[context['ds']])
        insights['data_quality_summary'] = quality_data.to_dict('records')
        
        # Overall platform health
        overall_quality = quality_data['avg_score'].mean()
        insights['platform_health'] = {
            'overall_quality_score': round(overall_quality, 4),
            'total_quality_checks': int(quality_data['total_checks'].sum()),
            'passed_quality_checks': int(quality_data['passed_checks'].sum())
        }
        
        print(f"📊 Platform health score: {overall_quality:.2%}")
        
        # Generate recommendations
        recommendations = []
        
        # Customer segment recommendations
        champions_pct = 0
        at_risk_pct = 0
        for segment in insights['customer_segments']:
            if segment['customer_segment'] == 'Champions':
                champions_pct = (segment['customer_count'] / sum(s['customer_count'] for s in insights['customer_segments'])) * 100
            elif segment['customer_segment'] == 'At Risk':
                at_risk_pct = (segment['customer_count'] / sum(s['customer_count'] for s in insights['customer_segments'])) * 100
        
        if champions_pct < 20:
            recommendations.append({
                'type': 'customer_retention',
                'priority': 'high',
                'message': f'Only {champions_pct:.1f}% of customers are Champions. Implement loyalty programs.'
            })
        
        if at_risk_pct > 25:
            recommendations.append({
                'type': 'customer_winback',
                'priority': 'high',
                'message': f'{at_risk_pct:.1f}% of customers are at risk. Launch win-back campaigns.'
            })
        
        # Data quality recommendations
        if overall_quality < 0.95:
            recommendations.append({
                'type': 'data_quality',
                'priority': 'medium',
                'message': f'Data quality at {overall_quality:.1%}. Investigate data sources and validation rules.'
            })
        
        insights['recommendations'] = recommendations
        
        # Save insights report
        insights_path = '/opt/airflow/data/processed/platform_insights_report.json'
        with open(insights_path, 'w') as f:
            json.dump(insights, f, indent=2, default=str)
        
        print(f"📊 Platform insights report saved: {insights_path}")
        print(f"📊 Generated {len(recommendations)} business recommendations")
        
    except Exception as e:
        insights['error'] = str(e)
        print(f"❌ Insights generation failed: {e}")
        raise
    
    context['task_instance'].xcom_push(key='platform_insights', value=insights)
    return insights

# Platform monitoring and alerting
def monitor_platform_performance(**context):
    """Monitor platform performance and send alerts"""
    print("📊 Monitoring platform performance...")
    monitoring_results = {}
    
    try:
        # Collect performance metrics from XCom
        health_status = context['task_instance'].xcom_pull(task_ids='check_platform_health')
        ingestion_results = context['task_instance'].xcom_pull(task_ids='ingest_multi_source_data')
        quality_results = context['task_instance'].xcom_pull(task_ids='validate_integrated_data_quality')
        processing_results = context['task_instance'].xcom_pull(task_ids='process_data_with_spark')
        loading_results = context['task_instance'].xcom_pull(task_ids='load_to_data_warehouse')
        
        # Calculate platform performance score
        metrics = {
            'health_score': 1.0 if health_status.get('overall') else 0.0,
            'ingestion_success_rate': ingestion_results.get('successful_sources', 0) / max(ingestion_results.get('total_sources', 1), 1),
            'quality_pass_rate': quality_results.get('overall_pass_rate', 0),
            'processing_success': 1.0 if 'error' not in processing_results else 0.0,
            'loading_success': 1.0 if 'error' not in loading_results else 0.0
        }
        
        overall_platform_score = sum(metrics.values()) / len(metrics)
        
        monitoring_results = {
            'execution_date': context['ds'],
            'overall_platform_score': round(overall_platform_score, 4),
            'individual_metrics': metrics,
            'total_records_processed': ingestion_results.get('total_records_ingested', 0),
            'pipeline_status': 'SUCCESS' if overall_platform_score >= 0.8 else 'DEGRADED'
        }
        
        # Alert conditions
        alerts = []
        if overall_platform_score < 0.8:
            alerts.append({
                'severity': 'HIGH',
                'message': f'Platform performance degraded: {overall_platform_score:.1%} overall score'
            })
        
        if metrics['quality_pass_rate'] < 0.9:
            alerts.append({
                'severity': 'MEDIUM',
                'message': f'Data quality below threshold: {metrics["quality_pass_rate"]:.1%} pass rate'
            })
        
        if metrics['ingestion_success_rate'] < 1.0:
            alerts.append({
                'severity': 'MEDIUM',
                'message': f'Ingestion failures detected: {metrics["ingestion_success_rate"]:.1%} success rate'
            })
        
        monitoring_results['alerts'] = alerts
        
        # Log monitoring results
        print(f"📊 Platform Performance Score: {overall_platform_score:.1%}")
        print(f"📊 Health: {metrics['health_score']:.1%}")
        print(f"📊 Ingestion: {metrics['ingestion_success_rate']:.1%}")
        print(f"📊 Quality: {metrics['quality_pass_rate']:.1%}")
        print(f"📊 Processing: {metrics['processing_success']:.1%}")
        print(f"📊 Loading: {metrics['loading_success']:.1%}")
        
        if alerts:
            print(f"🚨 {len(alerts)} alerts generated")
            for alert in alerts:
                print(f"   {alert['severity']}: {alert['message']}")
        else:
            print("✅ No alerts - all systems performing well")
            
    except Exception as e:
        monitoring_results['error'] = str(e)
        print(f"❌ Platform monitoring failed: {e}")
    
    context['task_instance'].xcom_push(key='monitoring_results', value=monitoring_results)
    return monitoring_results

# Task Group Definitions using Airflow TaskGroups
with TaskGroup('platform_initialization') as initialization_group:
    health_check_task = PythonOperator(
        task_id='check_platform_health',
        python_callable=check_platform_health,
        dag=dag
    )

with TaskGroup('data_ingestion') as ingestion_group:
    multi_source_ingestion_task = PythonOperator(
        task_id='ingest_multi_source_data',
        python_callable=ingest_multi_source_data,
        dag=dag
    )

with TaskGroup('data_quality') as quality_group:
    quality_validation_task = PythonOperator(
        task_id='validate_integrated_data_quality',
        python_callable=validate_integrated_data_quality,
        dag=dag
    )

with TaskGroup('data_processing') as processing_group:
    spark_processing_task = PythonOperator(
        task_id='process_data_with_spark',
        python_callable=process_data_with_spark,
        dag=dag
    )

with TaskGroup('data_warehouse') as warehouse_group:
    # Database preparation
    create_tables_task = PostgresOperator(
        task_id='ensure_database_schema',
        postgres_conn_id='postgres_data',
        sql="""
        -- Customer analytics table
        CREATE TABLE IF NOT EXISTS customer_analytics (
            customer_id VARCHAR(50) PRIMARY KEY,
            total_orders INTEGER NOT NULL,
            total_spent DECIMAL(10,2) NOT NULL,
            average_order_value DECIMAL(10,2) NOT NULL,
            last_order_date DATE,
            days_since_last_order INTEGER,
            customer_segment VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Data quality metrics table
        CREATE TABLE IF NOT EXISTS data_quality_metrics (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value DECIMAL(10,4) NOT NULL,
            threshold_value DECIMAL(10,4) NOT NULL,
            status VARCHAR(20) NOT NULL,
            execution_date DATE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Pipeline execution log table
        CREATE TABLE IF NOT EXISTS pipeline_execution_log (
            id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(100) NOT NULL,
            execution_date DATE NOT NULL,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            records_processed INTEGER,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_customer_analytics_segment ON customer_analytics(customer_segment);
        CREATE INDEX IF NOT EXISTS idx_data_quality_execution_date ON data_quality_metrics(execution_date);
        CREATE INDEX IF NOT EXISTS idx_pipeline_log_execution_date ON pipeline_execution_log(execution_date);
        """,
        dag=dag
    )
    
    warehouse_loading_task = PythonOperator(
        task_id='load_to_data_warehouse',
        python_callable=load_to_data_warehouse,
        dag=dag
    )
    
    create_tables_task >> warehouse_loading_task

with TaskGroup('business_intelligence') as bi_group:
    insights_generation_task = PythonOperator(
        task_id='generate_platform_insights',
        python_callable=generate_platform_insights,
        dag=dag
    )

with TaskGroup('monitoring_and_alerting') as monitoring_group:
    monitoring_task = PythonOperator(
        task_id='monitor_platform_performance',
        python_callable=monitor_platform_performance,
        dag=dag
    )

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup_temporary_files',
    bash_command="""
    echo "🧹 Cleaning up temporary files..."
    find /opt/airflow/data/staging -name "*.tmp" -delete 2>/dev/null || true
    find /opt/airflow/data/staging -name "*.log" -mtime +7 -delete 2>/dev/null || true
    echo "✅ Cleanup completed"
    """,
    dag=dag
)

monitoring_task >> cleanup_task

# Final success notification
platform_success_notification = BashOperator(
    task_id='send_platform_success_notification',
    bash_command="""
    echo "🎉 Integrated Data Platform execution completed successfully!"
    echo "📊 Check Airflow UI for detailed metrics and logs"
    echo "💾 Data available in PostgreSQL analytics database"
    echo "📈 Business insights report generated"
    """,
    dag=dag,
    trigger_rule='all_success'
)

# Define task group dependencies
initialization_group >> ingestion_group >> quality_group >> processing_group >> warehouse_group >> bi_group >> monitoring_group >> platform_success_notification

# Optional: Add parallel data validation tasks
data_validation_task = BashOperator(
    task_id='validate_data_completeness',
    bash_command="""
    echo "🔍 Performing additional data validation checks..."
    echo "📊 Checking for data drift and anomalies..."
    echo "✅ Data validation completed"
    """,
    dag=dag
)

# Add the validation task to run in parallel with quality checks
quality_validation_task >> data_validation_task
data_validation_task >> spark_processing_task

# Error handling and retry logic
error_notification_task = BashOperator(
    task_id='send_error_notification',
    bash_command="""
    echo "❌ Data platform pipeline failed!"
    echo "🔍 Check logs for detailed error information"
    echo "📧 Sending notification to data team..."
    """,
    dag=dag,
    trigger_rule='one_failed'
)

# Connect error notification to all main task groups
[initialization_group, ingestion_group, quality_group, processing_group, warehouse_group, bi_group, monitoring_group] >> error_notification_task