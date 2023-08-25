import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta
from airflow.decorators import dag,task

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

#Default Arguments
default_args = {
    'start_date' : yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}
#Python Operatör Func
def python_func():
    pass

with DAG(
    dag_id = 'my_first_cloud_dag',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
) as dag:
    
    start_task = EmptyOperator(
        task_id='Start'
    )

    bash_task = BashOperator(
        task_id='bash_operator',
        bash_command = "date ; echo 'Hello this is my first bash task'"

    )

    python_task = PythonOperator(
        task_id='Python_task',
        python_callable=python_func
    )
start_task >> bash_task >> python_task


