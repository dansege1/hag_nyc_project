#default argument
"owner" : ""

#definition of dig


def greet():
    print("Hello from Apache Airflow")

def extract():
    print("Extracting data... from multiple sources")
    print("data extracted from source side successfully")

def extract():
    print("Transforming data... from multiple sources")
    print("source data trasformed successfully")

def load ():
    print("loading data... for better insight")
    print("source data transformed successfully")

#creation of tasks
greettask = pythonOperator (
    task_id = 'greet_task',
    python__callable = greet,
    dag = dag 
)

extract_task = pythonOperator (
    task_id = 'extract_',
    python__callable = extract,
    dag = dag 
)
transform_task = pythonOperator (
     task_id = 'transform_',
    python__callable = transform,
    dag = dag 
)
Load (function) task_id: any (
     task_id = 'load_',
    python__callable = load,
    dag = dag 
)

