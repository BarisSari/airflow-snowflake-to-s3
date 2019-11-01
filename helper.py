from airflow.contrib.hooks import snowflake_hook
from airflow.hooks import S3_hook


def fetch_data_from_snowflake():
    hook = snowflake_hook.SnowflakeHook("snowflake_conn")
    conn = hook.get_conn()
    with conn.cursor() as cursor:
        cursor.execute('USE DATABASE MY_AUDIT_DB;')
        cursor.execute("SHOW roles")
        print(cursor.fetchall())
    # check data is correct or not
    # create csv and return it


def upload_file_to_s3_with_hook(filename, key, bucket_name):
    hook = S3_hook.S3Hook('aws_s3_conn')
    hook.load_file(filename, key, bucket_name)
