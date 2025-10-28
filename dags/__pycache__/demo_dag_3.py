from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'FarsH',
    'email': ['trialmail@randommail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(0)
}

# DAG definition
dag = DAG(
    dag_id='my_second_dag',
    default_args=default_args,
    description='This is another trial to understand Airflow DAG',
    schedule_interval=timedelta(hours=4),
    catchup=False,
    tags=['etl', 'tutorial']
)

# Extract task
extract_task = BashOperator(
    task_id='extract',
    bash_command='echo "Connecting to datasource..."',
    dag=dag
)

# Transform tasks
transform_a = BashOperator(
    task_id='filter',
    bash_command='echo "Filtering data based on business rules"',
    dag=dag
)

transform_b = BashOperator(
    task_id='fillup',
    bash_command='echo "Replacing empty records by business rules"',
    dag=dag
)

transform_c = BashOperator(
    task_id='aggregation',
    bash_command='echo "Grouping and ranking data"',
    dag=dag
)

# Load task
load_task = BashOperator(
    task_id='load',
    bash_command='echo "Loading processed data to destination"',
    dag=dag
)

# Define task dependencies
extract_task >> [transform_a, transform_b, transform_c] >> load_task