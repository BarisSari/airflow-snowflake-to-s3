import csv
import os

from airflow.contrib.hooks import snowflake_hook
from airflow.hooks import S3_hook
from scripts import sf_grant, sf_role


def fetch_data_from_snowflake():
    hook = snowflake_hook.SnowflakeHook("snowflake_conn")
    conn = hook.get_conn()
    roles = []
    with conn.cursor() as cursor:
        cursor.execute('USE DATABASE MY_AUDIT_DB;')
        cursor.execute("SHOW roles")
        rec_set = cursor.fetchall()
        for rec in rec_set:
            roles.append(sf_role(rec[1], rec[9]))
        for role in roles:
            cursor.execute("SHOW GRANTS TO ROLE " + role.name)
            grant_set = cursor.fetchall()
            for cur_grant in grant_set:
                role.add_grant(
                    sf_grant(cur_grant[1], cur_grant[2], cur_grant[3]),
                    roles)

    with open("./tmp/roles.csv", "w")as f:
        writer = csv.writer(f, delimiter=',')
        for role in roles:
            writer.writerows(','.join([role.name, role.comment]))

    with open("./tmp/role_grants.csv", "w")as f:
        for role in roles:
            role.write_grants(role.name, 'ROOT', f)


def upload_file_to_s3_with_hook(directory, bucket_name):
    hook = S3_hook.S3Hook('aws_s3_conn')
    files = os.listdir(directory)
    base_path = os.path.abspath(directory)
    for file in files:
        file_path = os.path.join(base_path+file)
        key = file[:-3]
        hook.load_file(file_path, key, bucket_name)
