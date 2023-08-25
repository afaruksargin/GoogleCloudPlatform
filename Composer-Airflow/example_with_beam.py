import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime,timedelta
from airflow.decorators import dag,task
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'start_date':yesterday,
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'BigQuery_operators_example',
    schedule_interval = timedelta(days=1),
    default_args = default_args
    ) as dag :
    
    start_task=EmptyOperator(
        task_id='Start'
    )

    cloud_to_bigquery_task=GoogleCloudStorageToBigQueryOperator(
        task_id = 'gcs_to_BigQuery_task',
        bucket ='data-iris',
        source_objects=['IRIS.csv'],
        destination_project_dataset_table= 'jovial-evening-394610.gcs_demo_dataset.gcs_to_bigquery_table',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE'
    )

    create_aggr_bg_table = BigQueryOperator(
        task_id = 'BigQuery_task',
        allow_large_results=True,
        use_legacy_sql=False,
        sql = """CREATE OR REPLACE TABLE gcs_demo_dataset.bg_table_aggr as 
            SELECT   
                sepal_length,sepal_width,petal_length,petal_width,species
                from jovial-evening-394610.gcs_demo_dataset.gcs_to_bigquery_table """
    )
start_task >> cloud_to_bigquery_task >>create_aggr_bg_table