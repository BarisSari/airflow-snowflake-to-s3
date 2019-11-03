import csv
import os

from airflow.contrib.hooks import snowflake_hook
from airflow.hooks import S3_hook


def fetch_data_from_snowflake():
    hook = snowflake_hook.SnowflakeHook("snowflake_conn")
    conn = hook.get_conn()
    roles = []
    with conn.cursor() as cursor:
        cursor.execute("USE DATABASE MY_AUDIT_DB;")
        cursor.execute("SHOW roles")
        rec_set = cursor.fetchall()
        for rec in rec_set:
            roles.append(Role(rec[1], rec[9]))
        for role in roles:
            cursor.execute("SHOW GRANTS TO ROLE " + role.name)
            grant_set = cursor.fetchall()
            for cur_grant in grant_set:
                role.add_grant(
                    Grant(cur_grant[1], cur_grant[2], cur_grant[3]), roles
                )

    with open("./tmp/roles.csv", "w") as f:
        writer = csv.writer(f, delimiter=",")
        for role in roles:
            writer.writerows(",".join([role.name, role.comment]))

    with open("./tmp/role_grants.csv", "w") as f:
        for role in roles:
            role.write_grants(role.name, "ROOT", f)


def upload_file_to_s3_with_hook(directory, bucket_name):
    hook = S3_hook.S3Hook("aws_s3_conn")
    files = os.listdir(directory)
    base_path = os.path.abspath(directory)
    for file in files:
        file_path = os.path.join(base_path, file)
        key = file[:-3]
        hook.load_file(file_path, key, bucket_name)


class Grant(object):
    def __init__(self, in_privilege, in_object_type, in_object_name):
        self.privilege = in_privilege
        self.object_type = in_object_type
        self.object_name = in_object_name


class Role(object):
    def __init__(self, in_name, in_comment):
        self.name = in_name
        self.comment = in_comment
        self.child_roles = set()
        self.grants = set()

    def add_grant(self, in_grant, all_roles):
        if in_grant.object_type == "ROLE":
            for role in all_roles:
                if role.name == in_grant.object_name:
                    self.child_roles.add(role)
                    break
            else:
                self.grants.add(in_grant)
        else:
            self.grants.add(in_grant)

    def write_grants(self, root, branch, file_handle):
        current_path = branch + "->" + self.name
        for cur_grant in self.grants:
            print(
                root
                + ","
                + current_path
                + ","
                + cur_grant.privilege
                + ","
                + cur_grant.object_type
                + ",'"
                + cur_grant.object_name
                + "'",
                file=file_handle
            )
        for cur_role in self.child_roles:
            cur_role.write_grants(root, current_path, file_handle)
