#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.connector import connect

CONNECTION_PARAMETERS_CONTAINER_USER_ROLE = {
    "account": os.environ["snowflake_account"],
    "user": os.environ["snowflake_user"],
    "password": os.environ["snowflake_password"],
    "database": os.environ["snowflake_database"],
    "schema": os.environ["snowflake_schema"],
    "warehouse": os.environ["snowflake_warehouse"],
    "role": "CONTAINER_USER_ROLE",
}

# Connect as CONTANTAINER_USE_ROLE
connection_container_user_role = connect(**CONNECTION_PARAMETERS_CONTAINER_USER_ROLE)

try:
    # create a root as the entry point for all object
    root = Root(connection_container_user_role)

    # ALTER COMPUTE POOL CONTAINER_HOL_POOL STOP ALL;
    root.compute_pools["CONTAINER_HOL_POOL"].stop_all_services()

    # ALTER COMPUTE POOL CONTAINER_HOL_POOL SUSPEND;
    root.compute_pools["CONTAINER_HOL_POOL"].suspend()

finally:
    connection_container_user_role.close()
