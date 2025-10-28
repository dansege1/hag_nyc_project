from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta 

# Default arguments
default_args = {
    "owner": "Oluwasegun",
    "start_date": days_ago(0),
    "email": "danielsegun2010@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    dag_id='logistics_analytics_pipeline',
    default_args=default_args,
    description='Analytics pipeline for logistics data - reads from DB, analyzes, writes results',
    schedule_interval='0 11 * * *',  # Daily at 11 AM (after ETL at 10 AM)
    catchup=False,
    tags=['analytics', 'logistics', 'postgresql', 'reporting']
)

def check_source_data(**context):
    """Check if we have data in the source table"""
    postgres_hook = PostgresHook(postgres_conn_id='hag_postgres')
    
    sql = "SELECT COUNT(*) FROM shipping.shipments"
    result = postgres_hook.get_first(sql)
    record_count = result[0]
    
    print(f"Found {record_count} total records in source table")
    
    if record_count == 0:
        raise ValueError("No data found in source table")
    
    return f"Data check passed: {record_count} records found"

# Create analytics tables
create_analytics_tables_task = PostgresOperator(
    task_id='create_analytics_tables',
    postgres_conn_id='hag_postgres',
    sql="""
    -- Create simple analytics tables for results
    
    DROP TABLE IF EXISTS shipping.company_performance;
    CREATE TABLE shipping.company_performance (
        shipping_company VARCHAR(100),
        avg_shipping_days NUMERIC(10,2),
        avg_cost_per_kg NUMERIC(10,2),
        total_shipments INTEGER,
        analysis_date DATE DEFAULT CURRENT_DATE
    );
sql = """
INSERT INTO shipping.company_performance (...)
SELECT shipping_company,
       AVG(EXTRACT(DAY FROM delivery_date - shipment_date)) AS avg_shipping_days,
       AVG(cost_per_kg) AS avg_cost_per_kg,
       COUNT(*) AS total_shipments,
       CURRENT_DATE
FROM shipping.shipments
GROUP BY shipping_company;
"""

 DROP TABLE IF EXISTS shipping.country_summary;
    CREATE TABLE shipping.country_summary (
        origin_country VARCHAR(100),
        avg_shipping_days NUMERIC(10,2),
        avg_package_weight NUMERIC(10,2),
        total_shipments INTEGER,
        analysis_date DATE DEFAULT CURRENT_DATE
    );

 DROP TABLE IF EXISTS shipping.monthly_stats;
    CREATE TABLE shipping.monthly_stats (
        year_month VARCHAR(10),
        total_shipments INTEGER,
        avg_shipping_days NUMERIC(10,2),
        total_weight NUMERIC(10,2),
        analysis_date DATE DEFAULT CURRENT_DATE
    );
    """,
    dag=dag
)

# Analyze shipping company performance
analyze_companies_task = PostgresOperator(
    task_id='analyze_shipping_companies',
    postgres_conn_id='hag_postgres',
    sql="""
    -- Calculate simple stats by shipping company
    INSERT INTO shipping.company_performance 
    (shipping_company, avg_shipping_days, avg_cost_per_kg, total_shipments)
    SELECT 
        shipping_company,
        ROUND(AVG(shipping_duration_days), 2) as avg_shipping_days,
        ROUND(AVG(cost_per_kg), 2) as avg_cost_per_kg,
        COUNT(*) as total_shipments
    FROM shipping.shipments
    WHERE shipment_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY shipping_company
    HAVING COUNT(*) >= 2
    ORDER BY total_shipments DESC;
    """,
    dag=dag
)

# Analyze by country
analyze_countries_task = PostgresOperator(
    task_id='analyze_countries',
    postgres_conn_id='hag_postgres',
    sql="""
    -- Calculate stats by origin country
    INSERT INTO shipping.country_summary 
    (origin_country, avg_shipping_days, avg_package_weight, total_shipments)
    SELECT 
        origin_country,
        ROUND(AVG(shipping_duration_days), 2) as avg_shipping_days,
        ROUND(AVG(package_weight), 2) as avg_package_weight,
        COUNT(*) as total_shipments
    FROM shipping.shipments
    WHERE shipment_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY origin_country
    ORDER BY total_shipments DESC;
    """,
    dag=dag
)

# Create monthly summary
analyze_monthly_task = PostgresOperator(
    task_id='analyze_monthly_data',
    postgres_conn_id='hag_postgres',
    sql="""
    -- Create monthly summary stats
    INSERT INTO shipping.monthly_stats 
    (year_month, total_shipments, avg_shipping_days, total_weight)
    SELECT 
        TO_CHAR(shipment_date, 'YYYY-MM') as year_month,
        COUNT(*) as total_shipments,
        ROUND(AVG(shipping_duration_days), 2) as avg_shipping_days,
        ROUND(SUM(package_weight), 2) as total_weight
    FROM shipping.shipments
    WHERE shipment_date >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY TO_CHAR(shipment_date, 'YYYY-MM')
    ORDER BY year_month;
    """,
    dag=dag
)

def print_summary_results(**context):
    """Print a simple summary of what we analyzed"""
    postgres_hook = PostgresHook(postgres_conn_id='hag_postgres')
    
    # Get counts from each analysis table
    companies = postgres_hook.get_first("SELECT COUNT(*) FROM shipping.company_performance WHERE analysis_date = CURRENT_DATE")[0]
    countries = postgres_hook.get_first("SELECT COUNT(*) FROM shipping.country_summary WHERE analysis_date = CURRENT_DATE")[0]
    months = postgres_hook.get_first("SELECT COUNT(*) FROM shipping.monthly_stats WHERE analysis_date = CURRENT_DATE")[0]
    
    print("Analytics Summary:")
    print(f"- Analyzed {companies} shipping companies")
    print(f"- Analyzed {countries} origin countries") 
    print(f"- Created stats for {months} months")
    
    return f"Analysis complete: {companies} companies, {countries} countries, {months} months"

# Create tasks
check_data_task = PythonOperator(
    task_id='check_source_data',
    python_callable=check_source_data,
    dag=dag
)

summary_task = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary_results,
    dag=dag
)

# Set up task dependencies
check_data_task>>create_analytics_tables_task>>[analyze_companies_task, analyze_countries_task, analyze_monthly_task]>>summary_task
