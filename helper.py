import csv
import os
import tempfile

from airflow.contrib.hooks import snowflake_hook
from airflow.hooks import S3_hook

TMP_DIRECTORY = tempfile.TemporaryDirectory()
ROLES_PATH = os.path.join(TMP_DIRECTORY.name, "roles.csv")
ROLE_GRANTS_PATH = os.path.join(TMP_DIRECTORY.name, "role_grants.csv")
USERS_PATH = os.path.join(TMP_DIRECTORY.name, "users.csv")
USER_GRANTS_PATH = os.path.join(TMP_DIRECTORY.name, "user_grants.csv")


def fetch_data_from_snowflake():
    hook = snowflake_hook.SnowflakeHook("snowflake_conn")
    conn = hook.get_conn()
    roles = []
    users = []
    with conn.cursor() as cursor:
        cursor.execute("USE DATABASE MY_AUDIT_DB;")
        cursor.execute("USE ROLE ACCOUNTADMIN;")

        # Queries for Roles and Role Grants
        cursor.execute("SHOW roles")
        rec_set = cursor.fetchall()
        for rec in rec_set:
            roles.append(Role(rec[1], rec[9]))
        for role in roles:
            cursor.execute("SHOW GRANTS TO ROLE " + role.name)
            grant_set = cursor.fetchall()
            for cur_grant in grant_set:
                role.add_grant(RoleGrant(cur_grant[1], cur_grant[2], cur_grant[3]), roles)

        # Queries for User and User Grants
        cursor.execute("SHOW users")
        user_set = cursor.fetchall()
        for user in user_set:
            users.append(User(user[0], user[1]))
        for user in users:
            cursor.execute("SHOW GRANTS TO USER " + user.name)
            grant_set = cursor.fetchall()
            for cur_grant in grant_set:
                user.add_grant(UserGrant(cur_grant[1], cur_grant[2], cur_grant[3], cur_grant[4]))

    with open(ROLES_PATH, "w") as f:
        writer = csv.writer(f, delimiter=",")
        for role in roles:
            writer.writerow([role.name, role.comment])

    with open(ROLE_GRANTS_PATH, "w") as f:
        for role in roles:
            role.write_grants(role.name, "ROOT", f)

    with open(USERS_PATH, "w") as f:
        writer = csv.writer(f, delimiter=",")
        for user in users:
            writer.writerow([user.name, user.created_on])

    with open(USER_GRANTS_PATH, "w") as f:
        for user in users:
            user.write_grants(f)


def upload_file_to_s3_with_hook(bucket_name):
    hook = S3_hook.S3Hook("aws_s3_conn")
    files = os.listdir(TMP_DIRECTORY.name)
    base_path = os.path.abspath(TMP_DIRECTORY.name)
    for file in files:
        file_path = os.path.join(base_path, file)
        hook.load_file(file_path, file, bucket_name)

    TMP_DIRECTORY.cleanup()


class RoleGrant(object):
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
                file=file_handle,
            )
        for cur_role in self.child_roles:
            cur_role.write_grants(root, current_path, file_handle)


class UserGrant(object):
    def __init__(self, role, granted_to, grantee_name, granted_by):
        self.role = role
        self.granted_to = granted_to
        self.grantee_name = grantee_name
        self.granted_by = granted_by


class User(object):
    def __init__(self, name, created_on):
        self.name = name
        self.created_on = str(created_on)
        self.grants = set()

    def add_grant(self, grant):
        self.grants.add(grant)

    def write_grants(self, file_handle):
        for grant in self.grants:
            print(
                grant.role,
                grant.granted_to,
                grant.grantee_name,
                grant.granted_by,
                file=file_handle,
            )
