import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta
from airflow.decorators import dag,task
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'start_date':yesterday,
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'DataFlowExample',
    schedule_interval = timedelta(days=1),
    default_args = default_args
    ) as dag :
    
    start_task=EmptyOperator(
        task_id='Start'
    )

    dataflow_task = DataFlowPythonOperator(
        task_id='dataflow',
        py_file='gs://us-central1-composer-demo-dca5c7c8-bucket/dags/scripts/writetobigquery.py'
    )

start_task >> dataflow_task