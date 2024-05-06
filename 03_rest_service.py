#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.core.service import Service, ServiceSpecStageFile
from snowflake.core.table import Table, TableColumn
from snowflake.core._common import CreateMode
from snowflake.core.function import Function, FunctionArgument, ServiceFunctionParams

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
    # create a root as the entry point for all object
    root = Root(connection_container_user_role)

    # create service CONTAINER_HOL_DB.PUBLIC.convert_api
    #     in compute pool CONTAINER_HOL_POOL
    #     from @specs
    #     specification_file='convert-api.yaml'
    #     external_access_integrations = (ALLOW_ALL_EAI);
    s = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].services.create(Service(
        name="convert_api",
        compute_pool="CONTAINER_HOL_POOL",
        spec=ServiceSpecStageFile(stage="@specs", spec_file="convert-api.yaml"),
        external_access_integrations=["ALLOW_ALL_EAI"],
    ))

    # CALL SYSTEM$GET_SERVICE_STATUS('CONTAINER_HOL_DB.PUBLIC.CONVERT-API');
    status = s.get_service_status()
    print(status)

    # CALL SYSTEM$GET_SERVICE_LOGS('CONTAINER_HOL_DB.PUBLIC.CONVERT_API', '0', 'convert-api',10);
    # numb_lines is not supported in Python API
    logs = s.get_service_logs("0", "convert-api")
    print(logs)

    # CREATE OR REPLACE TABLE WEATHER (
    #     DATE DATE,
    #     LOCATION VARCHAR,
    #     TEMP_C NUMBER,
    #     TEMP_F NUMBER
    # );
    root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].tables.create(
        Table(
            name="WEATHER",
            columns=[
                    TableColumn(name="DATE", datatype="DATE"),
                    TableColumn(name="LOCATION", datatype="VARCHAR"),
                    TableColumn(name="TEMP_C", datatype="NUMBER"),
                    TableColumn(name="TEMP_F", datatype="NUMBER"),
                ],
        ),
        mode=CreateMode.or_replace
    )

    # CREATE OR REPLACE FUNCTION convert_udf (input float)
    # RETURNS float
    # SERVICE=CONVERT_API      //Snowflake container service
    # ENDPOINT='convert-api'   //The endpoint within the container
    # MAX_BATCH_ROWS=5         //limit the size of the batch
    # AS '/convert';           //The API endpoint
    root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].functions.create(Function(
        name="convert_udf",
        arguments=[
                    FunctionArgument(name="input", datatype="float")
        ],
        returns="float",
        service_function_params=(ServiceFunctionParams(
                                    service="CONVERT_API",
                                    endpoint="convert-api",
                                    path="/convert"
                                )
        ),
        max_batch_rows=5
    ))


finally:
    connection_container_user_role.close()
