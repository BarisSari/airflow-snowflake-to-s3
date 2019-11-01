from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from helper import upload_file_to_s3_with_hook

default_args = {
    'owner': 'baris',
    'start_date': datetime(2019, 1, 1),
    'retry_delay': timedelta(minutes=5)
}
with DAG('S3_dag_test', default_args=default_args,
         schedule_interval='@once') as dag:
    start_task = DummyOperator(
        task_id='dummy_start'
    )

    upload_to_S3_task = PythonOperator(
        task_id='upload_file_to_s3',
        python_callable=upload_file_to_s3_with_hook,
        op_kwargs={
            'filename': '/home/baris/Desktop/Untitled.csv',
            'key': 'test.csv',
            'bucket_name': 'import-snowflake',
        },
        dag=dag)

    # Use arrows to set dependencies between tasks
    start_task >> upload_to_S3_task
