import csv
import os
import tempfile

from airflow.contrib.hooks import snowflake_hook
from airflow.hooks import S3_hook

# If you want to use temporary folder, comment out following paths
# TMP_DIRECTORY = tempfile.TemporaryDirectory()
# ROLES_PATH = os.path.join(TMP_DIRECTORY.name, "roles.csv")
# ROLE_GRANTS_PATH = os.path.join(TMP_DIRECTORY.name, "role_grants.csv")
# USERS_PATH = os.path.join(TMP_DIRECTORY.name, "users.csv")
# USER_GRANTS_PATH = os.path.join(TMP_DIRECTORY.name, "user_grants.csv")

TMP_DIRECTORY = os.path.join(os.path.abspath("."), "tmp")
ROLES_PATH = os.path.join(TMP_DIRECTORY, "roles.csv")
ROLE_GRANTS_PATH = os.path.join(TMP_DIRECTORY, "role_grants.csv")
USERS_PATH = os.path.join(TMP_DIRECTORY, "users.csv")
USER_ROLES_PATH = os.path.join(TMP_DIRECTORY, "user_roles.csv")


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
                role.add_grant(
                    RoleGrant(cur_grant[1], cur_grant[2], cur_grant[3]), roles
                )

        # Queries for User and User Roles
        cursor.execute("SHOW users")
        user_set = cursor.fetchmany(1000)
        for user in user_set:
            users.append(User(user))
        while len(user_set) > 0:
            user_set = cursor.fetchmany(1000)
            for user in user_set:
                users.append(User(user))
        for user in users:
            cursor.execute("SHOW GRANTS TO USER " + user.user_name)
            user.get_roles(cursor.fetchall())

    with open(ROLES_PATH, "w") as roles_file, open(ROLE_GRANTS_PATH, "w") as role_grants_file:
        for role in roles:
            role.write_roles(roles_file)
            role.write_grants(role.name, "ROOT", role_grants_file)

    with open(USERS_PATH, "w") as users_file, open(
        USER_ROLES_PATH, "w"
    ) as user_roles_file:
        for user in users:
            user.write_user_record(users_file)
            user.write_roles(user_roles_file)


def upload_file_to_s3_with_hook(bucket_name):
    hook = S3_hook.S3Hook("aws_s3_conn")
    files = [ROLES_PATH, ROLE_GRANTS_PATH, USERS_PATH, USER_ROLES_PATH]
    print(files)
    for file in files:
        # file_path = os.path.join(TMP_DIRECTORY.name, file)
        hook.load_file(file, file[-10:], bucket_name)

    # TMP_DIRECTORY.cleanup()


class User(object):
    def __init__(self, user_rec):
        self.user_name = user_rec[0]
        self.user_login_name = user_rec[2]
        self.user_full_name = user_rec[3]
        self.user_first_name = user_rec[4]
        self.user_last_name = user_rec[5]
        self.user_email = user_rec[6]
        self.user_comment = user_rec[9]
        self.default_wh = user_rec[13]
        self.default_role = user_rec[15]
        self.user_roles = []

    def get_roles(self, role_rec_set):
        for role in role_rec_set:
            self.user_roles.append(role[1])

    def write_roles(self, file_name):
        for role in self.user_roles:
            print(self.user_name + ", " + role, file=file_name)

    def write_user_record(self, file_name):
        print(
            self.user_name + "," + self.user_login_name + ","
            "" + self.user_full_name + ","
            "" + self.user_first_name + ","
            "" + self.user_last_name + "," + self.user_email + ","
            "" + self.user_comment + "," + self.default_role + "," + self.default_wh,
            file=file_name,
        )


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

    def write_roles(self, file):
        print(self.name + ",'" + self.comment + "'", file=file)

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
