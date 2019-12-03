import os
import logging
import datetime
from datetime import timedelta

from airflow import DAG
# from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from users_roles import fetch_data_from_snowflake, upload_file_to_s3


# Airflow variables
# tmpl_search_path = Variable.get('sql_path')
# s3_dev_bucket = Variable.get('s3_dev_bucket')

# Connections
# SNOWFLAKE_CONN_ID = 'edw_etl'
# S3_CONN_ID = 'edw_dev_s3'


# dag args
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2019, 7, 16),
    'provide_context': True,
    'execution_timeout': timedelta(minutes=30),
    'retries': 0,
}

# dag
with DAG(
    dag_id='users_roles_grant',
    schedule_interval="@once",
    # dagrun_timeout=timedelta(hours=1),
    # template_searchpath=tmpl_search_path,
    default_args=default_args,
    # max_active_runs=1,
    # catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start")

    fetch_from_snowflake = PythonOperator(
        task_id="fetch_from_snowflake",
        python_callable=fetch_data_from_snowflake,
        op_kwargs={},
        dag=dag,
    )

    upload_to_s3_task = PythonOperator(
        task_id="upload_file_to_s3",
        python_callable=upload_file_to_s3,
        op_kwargs={"bucket_name": "s3_dev_bucket",
                   "s3_key": "snowflakes/users_roles/"},
        dag=dag
    )
    start_task >> fetch_from_snowflake >> upload_to_s3_task
