from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd
import numpy as np
import os
import json

# Default arguments
default_args = {
    "owner": "Segun",
    "start_date": days_ago(0),
    "email": "danielsegun2010@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# DAG definition
dag = DAG(
    dag_id='superstore_etl_pipeline',
    default_args=default_args,
    description='Complete ETL pipeline for Superstore dataset with date-based outputs',
    schedule_interval='@daily',  # This will trigger automatically every day
    catchup=False,
    tags=['etl', 'pandas', 'superstore', 'lightweight']
)

# File paths
INPUT_PATH = 'input_data/superstore_dataset.csv'
# Date-based output paths - will be created dynamically
def get_output_paths():
    today = datetime.now()
    date_str = today.strftime('%Y_%m_%d')
    
    return {
        'sales_summary': f'output_data/sales_summary_{date_str}.csv',
        'customer_analysis': f'output_data/customer_analysis_{date_str}.json',
        'regional_performance': f'output_data/regional_performance_{date_str}.csv',
        'product_insights': f'output_data/product_insights_{date_str}.json'
    }

def extract_data(**context):
    """Extract: Load data from local CSV file with basic validation"""
    print(" EXTRACT PHASE ")
    print(f"Reading data from: {INPUT_PATH}")
    
    try:
        df = pd.read_csv(INPUT_PATH, encoding = 'latin1')
        print(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")
        print(f"Data shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Store raw data temporarily for next tasks
        df.to_csv('/tmp/raw_superstore.csv', index=False)
        
        return f"Extracted {len(df)} records from Superstore dataset"
        
    except Exception as e:
        print(f"Error reading file: {str(e)}")
        raise

def transform_clean_data(**context):
    """Transform: Data cleaning, type conversion, and null handling"""
    print(" TRANSFORM PHASE: DATA CLEANING ")
    
    df = pd.read_csv('/tmp/raw_superstore.csv')
    print(f"Starting with {len(df)} records")
    
    # 1. Column renaming for consistency
    column_mapping = {
        'Row ID': 'row_id',
        'Order ID': 'order_id', 
        'Order Date': 'order_date',
        'Ship Date': 'ship_date',
        'Ship Mode': 'ship_mode',
        'Customer ID': 'customer_id',
        'Customer Name': 'customer_name',
        'Segment': 'segment',
        'Country': 'country',
        'City': 'city',
        'State': 'state',
        'Postal Code': 'postal_code',
        'Region': 'region',
        'Product ID': 'product_id',
        'Category': 'category',
        'Sub-Category': 'sub_category',
        'Product Name': 'product_name',
        'Sales': 'sales',
        'Quantity': 'quantity',
        'Discount': 'discount',
        'Profit': 'profit'
    }
    
    df = df.rename(columns=column_mapping)
    print(" Columns renamed to lowercase with underscores")
    
    # 2. Data type conversions
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['ship_date'] = pd.to_datetime(df['ship_date'])
    df['sales'] = pd.to_numeric(df['sales'], errors='coerce')
    df['profit'] = pd.to_numeric(df['profit'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
    df['postal_code'] = df['postal_code'].astype(str)
    
    print(" Data types converted")
    
    # 3. Handle null values
    null_counts_before = df.isnull().sum().sum()
    print(f"Null values before cleaning: {null_counts_before}")
    
    #   nulls with appropriate values
    df['postal_code'] = df['postal_code'].fillna('00000')
    df['sales'] = df['sales'].fillna(0)
    df['profit'] = df['profit'].fillna(0)
    df['quantity'] = df['quantity'].fillna(1)
    df['discount'] = df['discount'].fillna(0)
    
    null_counts_after = df.isnull().sum().sum()
    print(f" Null values after cleaning: {null_counts_after}")
    
    # 4. Remove duplicates
    duplicates_before = df.duplicated().sum()
    df = df.drop_duplicates()
    duplicates_after = len(df)
    print(f" Removed {duplicates_before} duplicate records")
    
    # 5. Data validation - remove invalid records
    initial_count = len(df)
    df = df[
        (df['sales'] >= 0) & 
        (df['quantity'] > 0) & 
        (df['discount'] >= 0) & 
        (df['discount'] <= 1) &  # Discount should be between 0-1
        (df['ship_date'] >= df['order_date'])  # Ship date should be after order date
    ]
    
    print(f" Data validation: removed {initial_count - len(df)} invalid records")
    
    # Save cleaned data
    df.to_csv('/tmp/cleaned_superstore.csv', index=False)
    print(f"Cleaned data saved: {len(df)} records, {len(df.columns)} columns")
    
    return f"Data cleaning completed: {len(df)} clean records"

def transform_add_calculated_fields(**context):
    """Transform: Add calculated fields and derived metrics"""
    print(" TRANSFORM PHASE: CALCULATED FIELDS ")
    
    df = pd.read_csv('/tmp/cleaned_superstore.csv')
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['ship_date'] = pd.to_datetime(df['ship_date'])
    
    # 1. Calculated fields
    df['profit_margin'] = ((df['profit'] / df['sales']) * 100).round(2)
    df['revenue_per_item'] = (df['sales'] / df['quantity']).round(2)
    df['discount_amount'] = (df['sales'] * df['discount'] / (1 - df['discount'])).round(2)
    df['total_revenue'] = (df['sales'] + df['discount_amount']).round(2)
    
    # 2. Date-based calculations
    df['shipping_days'] = (df['ship_date'] - df['order_date']).dt.days
    df['order_year'] = df['order_date'].dt.year
    df['order_month'] = df['order_date'].dt.month
    df['order_quarter'] = df['order_date'].dt.quarter
    df['order_day_of_week'] = df['order_date'].dt.day_name()
    
    # 3. Business categorizations
    df['profit_category'] = pd.cut(
        df['profit_margin'], 
        bins=[-np.inf, 0, 10, 25, np.inf], 
        labels=['Loss', 'Low Profit', 'Good Profit', 'High Profit']
    )
    
    df['order_size'] = pd.cut(
        df['quantity'],
        bins=[0, 2, 5, 10, np.inf],
        labels=['Small', 'Medium', 'Large', 'Bulk']
    )
    
    df['sales_tier'] = pd.cut(
        df['sales'],
        bins=[0, 100, 500, 1000, np.inf],
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    print(" Added calculated fields:")
    print("  - profit_margin, revenue_per_item, discount_amount, total_revenue")
    print("  - shipping_days, order_year, order_month, order_quarter")
    print("  - profit_category, order_size, sales_tier")
    
    # Handle any new nulls from calculations
    df['profit_margin'] = df['profit_margin'].fillna(0)
    df['revenue_per_item'] = df['revenue_per_item'].fillna(0)
    df['shipping_days'] = df['shipping_days'].fillna(0)
    
    # Save enhanced data
    df.to_csv('/tmp/enhanced_superstore.csv', index=False)
    print(f"Enhanced data saved: {len(df)} records with calculated fields")
    
    return f"Added calculated fields to {len(df)} records"

def transform_create_aggregations(**context):
    """Transform: Create various aggregations for reporting"""
    print(" TRANSFORM PHASE: AGGREGATIONS ")
    
    df = pd.read_csv('/tmp/enhanced_superstore.csv')
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    output_paths = get_output_paths()
    
    
    # 1. SALES SUMMARY REPORT (CSV)
    print("Creating Sales Summary Report...")
    sales_summary = df.groupby(['region', 'category', 'order_year']).agg({
        'sales': ['sum', 'mean', 'count'],
        'profit': ['sum', 'mean'],
        'quantity': 'sum',
        'profit_margin': 'mean',
        'customer_id': 'nunique'
    }).round(2)
    
    # Flatten column names
    sales_summary.columns = ['_'.join(col).strip() for col in sales_summary.columns.values]
    sales_summary = sales_summary.reset_index()
    sales_summary = sales_summary.rename(columns={'customer_id_nunique': 'unique_customers'})
    
    sales_summary.to_csv(output_paths['sales_summary'], index=False)
    print(f" Sales Summary saved to: {output_paths['sales_summary']}")
    
    # 2. CUSTOMER ANALYSIS REPORT (JSON)
    print("Creating Customer Analysis Report...")
    customer_analysis = df.groupby('customer_name').agg({
        'sales': 'sum',
        'profit': 'sum', 
        'quantity': 'sum',
        'order_id': 'nunique',
        'category': lambda x: list(x.unique()),
        'region': 'first'
    }).round(2)
    
    customer_analysis = customer_analysis.rename(columns={'order_id': 'total_orders'})
    customer_analysis['avg_order_value'] = (customer_analysis['sales'] / customer_analysis['total_orders']).round(2)
    
    # Get top 20 customers
    top_customers = customer_analysis.nlargest(20, 'sales').reset_index()
    customer_report = {
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'total_customers': len(customer_analysis),
        'top_customers': top_customers.to_dict('records')
    }
    
    with open(output_paths['customer_analysis'], 'w') as f:
        json.dump(customer_report, f, indent=2, default=str)
    print(f" Customer Analysis saved to: {output_paths['customer_analysis']}")
    
    # 3. REGIONAL PERFORMANCE REPORT (CSV)
    print("Creating Regional Performance Report...")
    regional_performance = df.groupby(['region', 'segment', 'order_quarter']).agg({
        'sales': ['sum', 'mean'],
        'profit': ['sum', 'mean'],
        'profit_margin': 'mean',
        'shipping_days': 'mean',
        'order_id': 'nunique',
        'customer_id': 'nunique'
    }).round(2)
    
    regional_performance.columns = ['_'.join(col).strip() for col in regional_performance.columns.values]
    regional_performance = regional_performance.reset_index()
    regional_performance = regional_performance.rename(columns={
        'order_id_nunique': 'total_orders',
        'customer_id_nunique': 'unique_customers'
    })
    
    regional_performance.to_csv(output_paths['regional_performance'], index=False)
    print(f" Regional Performance saved to: {output_paths['regional_performance']}")
    
    # 4. PRODUCT INSIGHTS REPORT (JSON)
    print("Creating Product Insights Report...")
    
    # Product performance by category and sub-category
    product_performance = df.groupby(['category', 'sub_category']).agg({
        'sales': ['sum', 'mean', 'count'],
        'profit': ['sum', 'mean'],
        'profit_margin': 'mean',
        'quantity': 'sum',
        'discount': 'mean'
    }).round(2)
    
    product_performance.columns = ['_'.join(col).strip() for col in product_performance.columns.values]
    product_performance = product_performance.reset_index()
    
    # Top and bottom performers
    top_products = product_performance.nlargest(10, 'sales_sum')
    bottom_products = product_performance.nsmallest(5, 'profit_sum')
    
    product_insights = {
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'summary_stats': {
            'total_categories': df['category'].nunique(),
            'total_sub_categories': df['sub_category'].nunique(),
            'avg_profit_margin': df['profit_margin'].mean().round(2),
            'total_products_sold': int(df['quantity'].sum())
        },
        'top_performing_products': top_products.to_dict('records'),
        'underperforming_products': bottom_products.to_dict('records')
    }
    
    with open(output_paths['product_insights'], 'w') as f:
        json.dump(product_insights, f, indent=2, default=str)
    print(f" Product Insights saved to: {output_paths['product_insights']}")
    
    print(f"All 4 reports created successfully!")
    return "Created 4 aggregated reports with date-based file names"

def load_final_summary(**context):
    """Load: Generate final summary and cleanup"""
    print(" LOAD PHASE: FINAL SUMMARY ")
    
    df = pd.read_csv('/tmp/enhanced_superstore.csv')
    output_paths = get_output_paths()
    
    # Verify all output files exist
    files_created = []
    for report_name, file_path in output_paths.items():
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            files_created.append(f"{report_name}: {file_path} ({file_size} bytes)")
    
    print(" ETL PIPELINE COMPLETED SUCCESSFULLY ")
    print(f"Processed Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total Records Processed: {len(df):,}")
    print(f"Date Range: {df['order_date'].min()} to {df['order_date'].max()}")
    print(f"Total Sales: ${df['sales'].sum():,.2f}")
    print(f"Total Profit: ${df['profit'].sum():,.2f}")
    print(f"Average Profit Margin: {df['profit_margin'].mean():.2f}%")
    print(f"Unique Customers: {df['customer_id'].nunique():,}")
    print(f"Unique Products: {df['product_id'].nunique():,}")
    
    print(" OUTPUT FILES CREATED ")
    for file_info in files_created:
        print(f" {file_info}")
    
    # Cleanup temporary files
    temp_files = ['/tmp/raw_superstore.csv', '/tmp/cleaned_superstore.csv', '/tmp/enhanced_superstore.csv']
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    print(f" Temporary files cleaned up")
    
    return f"ETL Pipeline completed successfully. Created {len(files_created)} reports."

# define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=transform_clean_data,
    dag=dag
)

add_fields_task = PythonOperator(
    task_id='add_calculated_fields',
    python_callable=transform_add_calculated_fields,
    dag=dag
)

create_aggregations_task = PythonOperator(
    task_id='create_aggregations',
    python_callable=transform_create_aggregations,
    dag=dag
)

load_summary_task = PythonOperator(
    task_id='load_summary',
    python_callable=load_final_summary,
    dag=dag
)

# Define dependencies
extract_task >> clean_data_task >> add_fields_task >> create_aggregations_task >> load_summary_task