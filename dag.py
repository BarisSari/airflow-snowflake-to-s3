from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helper import fetch_data_from_snowflake, upload_file_to_s3_with_hook

default_args = {
    'owner': 'baris',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}
with DAG('snowflake_to_s3', default_args=default_args,
         schedule_interval='@once') as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    fetch_from_snowflake = PythonOperator(
        task_id="fetch_from_snowflake",
        python_callable=fetch_data_from_snowflake,
        op_kwargs={},
        dag=dag,
    )

    upload_to_s3_task = PythonOperator(
        task_id='upload_file_to_s3',
        python_callable=upload_file_to_s3_with_hook,
        op_kwargs={
            'directory': './tmp/',
            'bucket_name': 'import-snowflake',
        },
        dag=dag)

    start_task >> fetch_from_snowflake >> upload_to_s3_task
