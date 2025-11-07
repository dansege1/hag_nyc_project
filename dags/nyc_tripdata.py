from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#from airflow.providers.amazon.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import warnings
import os
import io
import numpy as np
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import psycopg2
from io import StringIO
from sqlalchemy import create_engine
warnings.filterwarnings('ignore')

# Default arguments
default_args = {
    "owner": "IG",
    "start_date": days_ago(0),
    "email": "random@gmail.com",
    "retries": 0,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    }

# DAG definition
dag = DAG(
    dag_id='test_nyc_tripdata_etl',
    default_args=default_args,
    description='pipeline for nyc tlc data from Minio to PostgreSQL',
    schedule_interval='0 10 * * *',  # Daily at 10 AM
    catchup=False,
    tags=['etl', 'transportation', 'postgresql',
          'parquet', 'pandas', 'minio', 's3']
)

MINIO_CONN_ID = 'hag_proj_1'
POSTGRES_CONN_ID = 'hag_postgres'
BUCKET_NAME = 'hagitalproject1'
MONTH_NAME = 'jan'
FOLDER_NAME = "1_jan"
SCHEMA_NAME = "nyc_tripdata"


#first step
create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
    """,
    dag=dag,
)

# Second step: Stream and load data
def load_fhvhv_tripdata_table_from_minio():
    """Load fhvhv_tripdata from MinIO and insert into PostgreSQL efficiently using COPY FROM STDIN.
    .
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Find parquet file ---
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{FOLDER_NAME}/fhvhv_tripdata_")
    parquet_key = [k for k in keys if k.endswith(".parquet")][0]
    print(f"Found file in MinIO: {parquet_key}")

    # --- Download parquet into memory ---
    obj = s3_hook.get_key(
    key = [k for k in keys if k.startswith(f"{FOLDER_NAME}/fhvhv_tripdata_") and k.endswith(".parquet")][0],
        bucket_name=BUCKET_NAME
    )
    print(f"Found file in MinIO: {keys}")

    file_content = obj.get()["Body"].read()
    parquet_file = pq.ParquetFile(io.BytesIO(file_content))
    print(f"Parquet file has {parquet_file.num_row_groups} row groups")

    # --- Drop and recreate table ---
    drop_table_sql = f"""
    DROP TABLE IF EXISTS {SCHEMA_NAME}.fhvhv_2024;
    """
    pg_hook.run(drop_table_sql)
    print("fhvhv tripdata dropped in PostgreSQL.")
 

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.fhvhv_2024 (
        hvfhs_license_num VARCHAR,
        dispatching_base_num VARCHAR,
        originating_base_num VARCHAR,
        request_datetime TIMESTAMP,
        on_scene_datetime TIMESTAMP,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        PULocationID INT,
        DOLocationID INT,
        trip_miles DOUBLE PRECISION,
        trip_time BIGINT,
        base_passenger_fare DOUBLE PRECISION,
        tolls DOUBLE PRECISION,
        bcf DOUBLE PRECISION,
        sales_tax DOUBLE PRECISION,
        congestion_surcharge DOUBLE PRECISION,
        airport_fee DOUBLE PRECISION,
        tips DOUBLE PRECISION,
        driver_pay DOUBLE PRECISION,
        shared_request_flag VARCHAR,
        shared_match_flag VARCHAR,
        access_a_ride_flag VARCHAR,
        wav_request_flag VARCHAR,
        wav_match_flag VARCHAR
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("fhvhv_tripdata table created successfully in PostgreSQL")


    # --- Stream data into PostgreSQL ---
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for i in range(parquet_file.num_row_groups):
        print(f"Reading row group {i+1}/{parquet_file.num_row_groups}")
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()
   
        #replace Nan with None
        df = df.where(pd.notnull(df), None) 
        df = df.replace([float('inf'), float('-inf')], None)



        # --- Data cleaning ---
        int_columns = ['PULocationID', 'DOLocationID', 'trip_time']

        # Fill NaN with 0 and ensure within BIGINT range
        for col in int_columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna(0)      
                    .astype(np.int64, errors='ignore')
                )
                df[col] = np.clip(df[col], -9223372036854775808, 9223372036854775807)

        print("Integer columns safely converted and clipped within BIGINT range.")

        # --- Stream to Postgres via COPY ---
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = f"COPY {SCHEMA_NAME}.fhvhv_2024 FROM STDIN WITH (FORMAT CSV)"
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f" Loaded {len(df)} rows from row group {i+1}")

    cursor.close()
    conn.close()
    print("fhvhv tripdata successfully loaded into PostgreSQL!")

def load_fhv_tripdata_table_from_minio():
    """Load fhv_tripdata from MinIO and insert into PostgreSQL efficiently using COPY FROM STDIN.
    .
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Find parquet file ---
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{FOLDER_NAME}/fhv_tripdata_")
    parquet_key = [k for k in keys if k.endswith(".parquet")][0]
    print(f"Found file in MinIO: {parquet_key}")

    # --- Download parquet into memory ---
    obj = s3_hook.get_key(
    key = [k for k in keys if k.startswith(f"{FOLDER_NAME}/fhv_tripdata_") and k.endswith(".parquet")][0],
        bucket_name=BUCKET_NAME
    )
    print(f"Found file in MinIO: {keys}")

    file_content = obj.get()["Body"].read()
    parquet_file = pq.ParquetFile(io.BytesIO(file_content))
    print(f"Parquet file has {parquet_file.num_row_groups} row groups")

    # --- Drop and recreate table ---
    drop_table_sql = f"""
    DROP TABLE IF EXISTS {SCHEMA_NAME}.fhv_2024;
    """
    pg_hook.run(drop_table_sql)
    print("fhv tripdata dropped in PostgreSQL.")
 

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.fhv_2024 (
        dispatching_base_num VARCHAR,
        pickup_datetime TIMESTAMP,
        dropoff_datetime TIMESTAMP,
        PUlocationID INT,
        DOlocationID INT,
        SR_Flag VARCHAR,
        Affiliated_base_number VARCHAR
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("fhv_tripdata table created successfully in PostgreSQL")


    # --- Stream data into PostgreSQL ---
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for i in range(parquet_file.num_row_groups):
        print(f"Reading row group {i+1}/{parquet_file.num_row_groups}")
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()
   
        #replace Nan with None
        df = df.where(pd.notnull(df), None) 
        df = df.replace([float('inf'), float('-inf')], None)



        # --- Data cleaning ---
        int_columns = ['PUlocationID', 'DOlocationID', 'SR_Flag']

        # Fill NaN with 0 and ensure within BIGINT range
        for col in int_columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna(0)      
                    .astype(np.int64, errors='ignore')
                )
                df[col] = np.clip(df[col], -9223372036854775808, 9223372036854775807)

        print("Integer columns safely converted and clipped within BIGINT range.")

        # --- Stream to Postgres via COPY ---
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = f"COPY {SCHEMA_NAME}.fhv_2024 FROM STDIN WITH (FORMAT CSV)"
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f" Loaded {len(df)} rows from row group {i+1}")

    cursor.close()
    conn.close()
    print("fhv tripdata successfully loaded into PostgreSQL!")


def load_yellow_tripdata_table_from_minio():
    """Load yellow_tripdata from MinIO and insert into PostgreSQL efficiently using COPY FROM STDIN.
    .
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Find parquet file ---
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{FOLDER_NAME}/yellow_tripdata_")
    parquet_key = [k for k in keys if k.endswith(".parquet")][0]
    print(f"Found file in MinIO: {parquet_key}")

    # --- Download parquet into memory ---
    obj = s3_hook.get_key(
    key = [k for k in keys if k.startswith(f"{FOLDER_NAME}/yellow_tripdata_") and k.endswith(".parquet")][0],
        bucket_name=BUCKET_NAME
    )
    print(f"Found file in MinIO: {keys}")

    file_content = obj.get()["Body"].read()
    parquet_file = pq.ParquetFile(io.BytesIO(file_content))
    print(f"Parquet file has {parquet_file.num_row_groups} row groups")

    # --- Drop and recreate table ---
    drop_table_sql = f"""
    DROP TABLE IF EXISTS {SCHEMA_NAME}.yellow_2024;
    """
    pg_hook.run(drop_table_sql)
    print("yellow tripdata dropped in PostgreSQL.")
 

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.yellow_2024 (
        VendorID INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE PRECISION,
        trip_distance DOUBLE PRECISION,
        RatecodeID DOUBLE PRECISION,
        store_and_fwd_flag VARCHAR,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        payment_type BIGINT,
        fare_amount DOUBLE PRECISION,
        extra DOUBLE PRECISION,
        mta_tax DOUBLE PRECISION,
        tip_amount DOUBLE PRECISION,
        tolls_amount DOUBLE PRECISION,
        improvement_surcharge DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        congestion_surcharge DOUBLE PRECISION,
        Airport_fee DOUBLE PRECISION
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("yellow_tripdata table created successfully in PostgreSQL")


    # --- Stream data into PostgreSQL ---
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for i in range(parquet_file.num_row_groups):
        print(f"Reading row group {i+1}/{parquet_file.num_row_groups}")
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()
   
        #replace Nan with None
        df = df.where(pd.notnull(df), None) 
        df = df.replace([float('inf'), float('-inf')], None)



        # --- Data cleaning ---
        int_columns = ['VendorID', 'PULocationID', 'DOLocationID', 'payment_type']

        # Fill NaN with 0 and ensure within BIGINT range
        for col in int_columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna(0)      
                    .astype(np.int64, errors='ignore')
                )
                df[col] = np.clip(df[col], -9223372036854775808, 9223372036854775807)

        print("Integer columns safely converted and clipped within BIGINT range.")

        # --- Stream to Postgres via COPY ---
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = f"COPY {SCHEMA_NAME}.yellow_2024 FROM STDIN WITH (FORMAT CSV)"
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f" Loaded {len(df)} rows from row group {i+1}")

    cursor.close()
    conn.close()
    print("yellow tripdata successfully loaded into PostgreSQL!")


def load_green_tripdata_table_from_minio():
    """Load green_tripdata from MinIO and insert into PostgreSQL efficiently using COPY FROM STDIN.
    .
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # --- Find parquet file ---
    keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{FOLDER_NAME}/green_tripdata_")
    parquet_key = [k for k in keys if k.endswith(".parquet")][0]
    print(f"Found file in MinIO: {parquet_key}")

    # --- Download parquet into memory ---
    obj = s3_hook.get_key(
    key = [k for k in keys if k.startswith(f"{FOLDER_NAME}/green_tripdata_") and k.endswith(".parquet")][0],
        bucket_name=BUCKET_NAME
    )
    print(f"Found file in MinIO: {keys}")

    file_content = obj.get()["Body"].read()
    parquet_file = pq.ParquetFile(io.BytesIO(file_content))
    print(f"Parquet file has {parquet_file.num_row_groups} row groups")

    # --- Drop and recreate table ---
    drop_table_sql = f"""
    DROP TABLE IF EXISTS {SCHEMA_NAME}.green_2024;
    """
    pg_hook.run(drop_table_sql)
    print("green tripdata dropped in PostgreSQL.")
 

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.green_2024 (
        VendorID INT,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag VARCHAR,
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("green_tripdata table created successfully in PostgreSQL")


    # --- Stream data into PostgreSQL ---
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for i in range(parquet_file.num_row_groups):
        print(f"Reading row group {i+1}/{parquet_file.num_row_groups}")
        table = parquet_file.read_row_group(i)
        df = table.to_pandas()
   
        #replace Nan with None
        df = df.where(pd.notnull(df), None) 
        df = df.replace([float('inf'), float('-inf')], None)



        # --- Data cleaning ---
        int_columns = ['VendorID', 'PULocationID', 'DOLocationID',
                    'RatecodeID', 'passenger_count', 'trip_distance',
                       'payment_type', 'trip_type']

        # Fill NaN with 0 and ensure within BIGINT range
        for col in int_columns:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .fillna(0)      
                    .astype(np.int64, errors='ignore')
                )
                df[col] = np.clip(df[col], -9223372036854775808, 9223372036854775807)

        print("Integer columns safely converted and clipped within BIGINT range.")

        # --- Stream to Postgres via COPY ---
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = f"COPY {SCHEMA_NAME}.green_2024 FROM STDIN WITH (FORMAT CSV)"
        cursor.copy_expert(copy_sql, buffer)
        conn.commit()

        print(f" Loaded {len(df)} rows from row group {i+1}")

    cursor.close()
    conn.close()
    print("green tripdata successfully loaded into PostgreSQL!")

# create tasks
load_fhvhv_tripdata_table = PythonOperator(
    task_id='load_fhvhv_tripdata_table',
    python_callable=load_fhvhv_tripdata_table_from_minio,
    dag=dag,
)    

load_fhv_tripdata_table = PythonOperator(
    task_id='load_fhv_tripdata_table',
    python_callable=load_fhv_tripdata_table_from_minio,
    dag=dag,
) 

load_yellow_tripdata_table = PythonOperator(
    task_id='load_yellow_tripdata_table',
    python_callable=load_yellow_tripdata_table_from_minio,
    dag=dag,
)   

load_green_tripdata_table = PythonOperator(
    task_id='load_green_tripdata_table',
    python_callable=load_green_tripdata_table_from_minio,
    dag=dag,
) 

# Define task dependencies
create_schema >> load_green_tripdata_table >> load_fhv_tripdata_table >> load_yellow_tripdata_table >> load_fhvhv_tripdata_table