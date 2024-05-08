# First, install the Docker CLI on your system.
# Then pip install docker to install the Docker SDK for Python.
import os

from snowflake.core import Root
from snowflake.core.service import Service, ServiceSpecStageFile

from snowflake.connector import connect

CONNECTION_PARAMETERS_CONTAINER_USER_ROLE = {
    "account": os.environ["snowflake_account"],
    "user": os.environ["snowflake_user"],
    "password": os.environ["snowflake_password"],
    "database": "CONTAINER_HOL_DB",
    "schema": "PUBLIC",
    "warehouse": "CONTAINER_HOL_WH",
    "role": "CONTAINER_USER_ROLE",
}

# Connect as CONTANTAINER_USE_ROLE
connection_container_user_role = connect(**CONNECTION_PARAMETERS_CONTAINER_USER_ROLE)

try:

    root = Root(connection_container_user_role)

    #USE ROLE CONTAINER_USER_ROLE;
    #LS @CONTAINER_HOL_DB.PUBLIC.SPECS;
    stageFiles = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].stages["SPECS"].listFiles()
    for stageFile in stageFiles:
        print(stageFile)

finally:
    connection_container_user_role.close()

