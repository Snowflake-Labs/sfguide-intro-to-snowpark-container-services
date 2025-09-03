#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.core._common import CreateMode
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
    # create a root as the entry point for all object
    root = Root(connection_container_user_role)

    # create service CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE
    # in compute pool CONTAINER_HOL_POOL
    # from @specs
    # specification_file='jupyter-snowpark.yaml'
    # external_access_integrations = (ALLOW_ALL_EAI);
    s = (
        root.databases["CONTAINER_HOL_DB"]
        .schemas["PUBLIC"]
        .services.create(
            Service(
                name="JUPYTER_SNOWPARK_SERVICE",
                compute_pool="CONTAINER_HOL_POOL",
                spec=ServiceSpecStageFile(
                    stage="specs", spec_file="jupyter-snowpark.yaml"
                ),
                external_access_integrations=["ALLOW_ALL_EAI"],
            ),
            mode=CreateMode.if_not_exists,
        )
    )

    # SHOW SERVICE CONTAINERS IN SERVICE JUPYTER_SNOWPARK_SERVICE;
    containers = s.get_containers()
    for container in containers:
        print(container.service_status)

    # CALL SYSTEM$GET_SERVICE_LOGS('CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE', '0', 'jupyter-snowpark',10);
    logs = s.get_service_logs("0", "jupyter-snowpark", 10)
    print(logs)

    # SHOW ENDPOINTS IN SERVICE JUPYTER_SNOWPARK_SERVICE;
    endpoints = s.get_endpoints()
    for endpoint in endpoints:
        print(endpoint)

    # --- After we make a change to our Jupyter notebook,
    # --- we will suspend and resume the service
    # --- and you can see that the changes we made in our Notebook are still there!
    # ALTER SERVICE CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE SUSPEND;
    s.suspend()

    # ALTER SERVICE CONTAINER_HOL_DB.PUBLIC.JUPYTER_SNOWPARK_SERVICE RESUME;
    s.resume()

finally:
    connection_container_user_role.close()
