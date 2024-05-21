#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.connector import connect

CONNECTION_PARAMETERS_ACCOUNT_ADMIN = {
    "account": os.environ["snowflake_account"],
    "user": os.environ["snowflake_user"],
    "password": os.environ["snowflake_password"],
    "database": os.environ["snowflake_database"],
    "schema": os.environ["snowflake_schema"],
    "warehouse": os.environ["snowflake_warehouse"],
    "role": "ACCOUNTADMIN",
}

CONNECTION_PARAMETERS_CONTAINER_USER_ROLE = {
    "account": os.environ["snowflake_account"],
    "user": os.environ["snowflake_user"],
    "password": os.environ["snowflake_password"],
    "database": os.environ["snowflake_database"],
    "schema": os.environ["snowflake_schema"],
    "warehouse": os.environ["snowflake_warehouse"],
    "role": "CONTAINER_USER_ROLE",
}

# Connect as CONTAINER_USER_ROLE
connection_container_user_role = connect(**CONNECTION_PARAMETERS_CONTAINER_USER_ROLE)

try:
    # create a root as the entry point for all object
    root = Root(connection_container_user_role)

    # ALTER COMPUTE POOL CONTAINER_HOL_POOL STOP ALL;
    root.compute_pools["CONTAINER_HOL_POOL"].stop_all_services()

    # ALTER COMPUTE POOL CONTAINER_HOL_POOL SUSPEND;
    root.compute_pools["CONTAINER_HOL_POOL"].suspend()

    # DROP SERVICE CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE;
    root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].services["JUPYTER_SNOWPARK_SERVICE"].delete()

    # DROP SERVICE CONTAINER_HOL_DB.PUBLIC.CONVERT_API;
    root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].services["CONVERT_API"].delete()

    # DROP COMPUTE POOL CONTAINER_HOL_POOL;
    root.compute_pools["CONTAINER_HOL_POOL"].delete()

    # DROP DATABASE CONTAINER_HOL_DB;
    root.databases["CONTAINER_HOL_DB"].delete()

    # DROP WAREHOUSE CONTAINER_HOL_WH;
    root.warehouses["CONTAINER_HOL_WH"].delete()

    # create a SnowflakeConnection instance
    connection_acct_admin = connect(**CONNECTION_PARAMETERS_ACCOUNT_ADMIN)

    # create a root as the entry point for all object
    root = Root(connection_acct_admin)

    try:
        # DROP ROLE CONTAINER_USER_ROLE;
        root.roles["CONTAINER_USER_ROLE"].delete()
    finally:
        connection_acct_admin.close()

finally:
    connection_container_user_role.close()

