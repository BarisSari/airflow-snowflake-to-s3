import airflow.hooks.S3_hook


def upload_file_to_s3_with_hook(filename, key, bucket_name):
    hook = airflow.hooks.S3_hook.S3Hook('aws_s3_conn')
    hook.load_file(filename, key, bucket_name)
