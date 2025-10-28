from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator

#default argument
default_args = {
    "owner" : "Segun",
    #"start_date" : days_ago(0),
    "email" : "danielsegun2010@gmail.com",
    "retries" : 1,
    "retry_delay" : timedelta(minutes= 3),
    }

#definition of dag and instantiation of the default argument
dag = DAG( 
    dag_id = 'my_first_dag_2',
    default_args = default_args,
    description = 'my first time writing airflow dag',
    start_date = datetime(2025, 9, 1, 21, 22, 53),
    schedule_interval = '@daily'
)

#creation of tasks
task_1 = BashOperator(
    task_id = 'task_one',
    bash_command =  "echo hello here is my first task",
    dag = dag
)


task_2 = BashOperator(
    task_id = 'task_two',
    bash_command = "echo here is the second task, it depends on the first task to execute",
    dag = dag
)

task3 = BashOperator(
    task_id = 'task_three',
    bash_command = "echo hello, here is my third task, running concurently with the second"
)

#pipeline and task dependencies.
task_1 >> [task_2, task3]

#task_1 >> task_2 >> task3