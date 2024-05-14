
#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.warehouse import Warehouse
from snowflake.core.stage import (
    Stage,
    StageEncryption,
    StageDirectoryTable,
)

from snowflake.core.grants import (
    Grant,
    Grantees,
    Privileges,
    Securables,
    User,
)

from snowflake.core.role import Role
from snowflake.core.database import Database

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

# create a SnowflakeConnection instance
connection_acct_admin = connect(**CONNECTION_PARAMETERS_ACCOUNT_ADMIN)

try:
    # create a root as the entry point for all object
    root = Root(connection_acct_admin)

    # CREATE ROLE CONTAINER_USER_ROLE
    root.roles.create(Role(
        name='CONTAINER_USER_ROLE',
        comment='My role to use container',
    ))

    # GRANT CREATE DATABASE ON ACCOUNT TO ROLE CONTAINER_USER_ROLE
    # GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE CONTAINER_USER_ROLE;
    # GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE CONTAINER_USER_ROLE;
    # GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE CONTAINER_USER_ROLE;
    # GRANT MONITOR USAGE ON ACCOUNT TO  ROLE  CONTAINER_USER_ROLE;
    # GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE CONTAINER_USER_ROLE;
    root.grants.grant(Grant(
        grantee=Grantees.role('CONTAINER_USER_ROLE'),
        securable=Securables.current_account,
        privileges=[Privileges.create_database,
                    Privileges.create_warehouse,
                    Privileges.create_compute_pool,
                    Privileges.create_integration,
                    Privileges.monitor_usage,
                    Privileges.bind_service_endpoint
                    ],
    ))

    # GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE CONTAINER_USER_ROLE;
    root.grants.grant(Grant(
        grantee=Grantees.role('CONTAINER_USER_ROLE'),
        securable=Securables.database('snowflake'),
        privileges=[Privileges.imported_privileges
                    ],
    ))

    # grant role CONTAINER_USER_ROLE to role ACCOUNTADMIN;
    root.grants.grant(Grant(
        grantee=Grantees.role('ACCOUNTADMIN'),
        securable=Securables.role('CONTAINER_USER_ROLE')
    ))

    # USE ROLE CONTANTAINER_USE_ROLE
    root.session.use_role("CONTANTAINER_USE_ROLE")

    # CREATE OR REPLACE DATABASE CONTAINER_HOL_DB;
    root.databases.create(Database(
        name="CONTAINER_HOL_DB",
        comment="This is a Container Quick Start Guide database"
    ), mode=CreateMode.or_replace)

    # CREATE OR REPLACE WAREHOUSE CONTAINER_HOL_WH
    #   WAREHOUSE_SIZE = XSMALL
    #   AUTO_SUSPEND = 120
    #   AUTO_RESUME = TRUE;
    root.warehouses.create(Warehouse(
        name="CONTAINER_HOL_WH",
        warehouse_size="XSMALL",
        auto_suspend=120,
        auto_resume="true",
        comment="This is a Container Quick Start Guide warehouse"
    ), mode=CreateMode.or_replace)

    # CREATE STAGE IF NOT EXISTS specs
    # ENCRYPTION = (TYPE='SNOWFLAKE_SSE');
    root.schemas[CONNECTION_PARAMETERS_ACCOUNT_ADMIN.get("schema")].stages.create(
        Stage(
            name="specs",
            encryption=StageEncryption(type="SNOWFLAKE_SSE")
    ))

    # CREATE STAGE IF NOT EXISTS volumes
    # ENCRYPTION = (TYPE='SNOWFLAKE_SSE')
    # DIRECTORY = (ENABLE = TRUE);
    root.schemas[CONNECTION_PARAMETERS_ACCOUNT_ADMIN.get("schema")].stages.create(
        Stage(
            name="volumes",
            encryption=StageEncryption(type="SNOWFLAKE_SSE"),
            directory_table=StageDirectoryTable(enable="true")
    ))
    # create collection objects as the entry
finally:
    connection_acct_admin.close()
