from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd
import os

# Default arguments
default_args = {
    "owner": "FarsH",
    "start_date": days_ago(0),
    "email": "random@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='pandas_dummy_superstore_etl',
    default_args=default_args,
    description='ETL pipeline using pandas with dummy_superstore data',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'pandas', 'dummy_superstore']
)

# File paths 
RAW_DATA_PATH = 'input_data/dummy_superstore_raw.csv'
PROCESSED_DATA_PATH = 'input_data/dummy_superstore_processed.csv'
SUMMARY_DATA_PATH = 'output_data/dummy_superstore_summary.csv'

def extract_data(**context):
    """Extract: Simulate reading from source and basic validation"""
    print("Extracting dummy_superstore data...")
    
    # In real scenario, this could be reading from database, API, etc.
    # For demo, we're create sample data similar to dummy_superstore
    sample_data = {
        'Order_Date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18'] * 25,
        'Category': ['Technology', 'Furniture', 'Office Supplies'] * 33 + ['Technology'],
        'Sub_Category': ['Phones', 'Chairs', 'Paper', 'Laptops'] * 25,
        'Sales': [1500.50, 800.25, 45.30, 2200.75] * 25,
        'Profit': [300.10, 120.40, 5.25, 440.15] * 25,
        'Region': ['East', 'West', 'Central', 'South'] * 25,
        'State': ['New York', 'California', 'Texas', 'Florida'] * 25
    }
    
    df = pd.DataFrame(sample_data)
    df.to_csv(RAW_DATA_PATH, index=False)
    print(f"Extracted {len(df)} records to {RAW_DATA_PATH}")
    return f"Extracted {len(df)} records"

def transform_filter(**context):
    """Transform: Filter data based on business rules"""
    print("Filtering data based on business rules...")
    
    df = pd.read_csv(RAW_DATA_PATH)
    print(f"Original data shape: {df.shape}")
    
    # Filter: Only profitable transactions
    df_filtered = df[df['Profit'] > 0]
    
    # Filter: Only recent dates (you can adjust this logic)
    df_filtered['Order_Date'] = pd.to_datetime(df_filtered['Order_Date'])
    
    df_filtered.to_csv(PROCESSED_DATA_PATH, index=False)
    print(f"Filtered data shape: {df_filtered.shape}")
    return f"Filtered to {len(df_filtered)} profitable records"

def transform_fillup(**context):
    """Transform: Handle missing data and data cleaning"""
    print("Handling missing data and cleaning...")
    
    df = pd.read_csv(PROCESSED_DATA_PATH)
    
    # Simulate some data cleaning
    df['Profit_Margin'] = (df['Profit'] / df['Sales'] * 100).round(2)
    df['Order_Date'] = pd.to_datetime(df['Order_Date'])
    df['Year'] = df['Order_Date'].dt.year
    df['Month'] = df['Order_Date'].dt.month
    
    # Handle any missing values (even though our sample data is clean)
    df = df.fillna(0)
    
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    print(f"Data cleaning completed. Added profit margin calculations.")
    return "Data cleaning and feature engineering completed"

def transform_aggregate(**context):
    """Transform: Aggregate data for analysis"""
    print("Aggregating data for business insights...")
    
    df = pd.read_csv(PROCESSED_DATA_PATH)
    df['Order_Date'] = pd.to_datetime(df['Order_Date'])
    
    # Create summary statistics
    summary = df.groupby(['Category', 'Region']).agg({
        'Sales': ['sum', 'mean', 'count'],
        'Profit': ['sum', 'mean'],
        'Profit_Margin': 'mean'
    }).round(2)
    
    # Flatten column names
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    summary = summary.reset_index()
    
    summary.to_csv(SUMMARY_DATA_PATH, index=False)
    print(f"Created summary with {len(summary)} aggregate records")
    return f"Created {len(summary)} summary records"

def load_data(**context):
    """Load: Save processed data and generate report"""
    print("Loading processed data and generating reports...")
    
    # Read the processed data
    df_processed = pd.read_csv(PROCESSED_DATA_PATH)
    df_summary = pd.read_csv(SUMMARY_DATA_PATH)
    
    print("=== PROCESSING COMPLETE ===")
    print(f"Processed Records: {len(df_processed)}")
    print(f"Summary Records: {len(df_summary)}")
    print("\nTop 5 Category-Region combinations by total sales:")
    print(df_summary.nlargest(5, 'Sales_sum')[['Category', 'Region', 'Sales_sum']])
    
    return "Data loading completed successfully"

# task definition
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

filter_task = PythonOperator(
    task_id='filter',
    python_callable=transform_filter,
    dag=dag
)

fillup_task = PythonOperator(
    task_id='fillup',
    python_callable=transform_fillup,
    dag=dag
)

aggregate_task = PythonOperator(
    task_id='aggregation',
    python_callable=transform_aggregate,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

# task dependencies 
extract_task >> filter_task >> fillup_task >> aggregate_task >> load_task