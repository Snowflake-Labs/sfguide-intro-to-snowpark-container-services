#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.core.compute_pool import ComputePool
from snowflake.core.image_repository import ImageRepository

from snowflake.core.grants import (
    DeletionMode,
    Grant,
    Grantees,
    Privileges,
    Role,
    Securables,
    User,
)

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

    connection_acct_admin.cursor().execute("""CREATE SECURITY INTEGRATION IF NOT EXISTS snowservices_ingress_oauth
        TYPE=oauth
        OAUTH_CLIENT=snowservices_ingress
        ENABLED=true;""")

    connection_acct_admin.cursor().execute("""CREATE OR REPLACE NETWORK RULE ALLOW_ALL_RULE
        TYPE = 'HOST_PORT'
        MODE = 'EGRESS'
        VALUE_LIST= ('0.0.0.0:443', '0.0.0.0:80');""")

    connection_acct_admin.cursor().execute("""CREATE EXTERNAL ACCESS INTEGRATION ALLOW_ALL_EAI
        ALLOWED_NETWORK_RULES = (ALLOW_ALL_RULE)
        ENABLED = true;""")

    # GRANT USAGE ON INTEGRATION ALLOW_ALL_EAI TO ROLE CONTAINER_USER_ROLE;
    root.grants.grant(Grant(
        grantee=Grantees.role('CONTAINER_USER_ROLE'),
        securable=Securables.integration("ALLOW_ALL_EAI"),
        privileges=[Privileges.Usage]
    ))

    # USE ROLE CONTANTAINER_USE_ROLE
    root.session.use_role("CONTANTAINER_USE_ROLE")

    # CREATE COMPUTE POOL IF NOT EXISTS CONTAINER_HOL_POOL
    # MIN_NODES = 1
    # MAX_NODES = 1
    # INSTANCE_FAMILY = standard_1;
    root.compute_pools.create(ComputePool(
      name="CONTAINER_HOL_POOL",
      min_nodes=1,
      max_nodes=1,
      instance_family="STANDARD_1",
    ))

    # CREATE IMAGE REPOSITORY CONTAINER_HOL_DB.PUBLIC.IMAGE_REPO;
    root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].image_repositories.create(ImageRepository(
      name="IMAGE_REPO",
    ))

    # SHOW IMAGE REPOSITORIES IN SCHEMA CONTAINER_HOL_DB.PUBLIC;
    itr_data = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].image_repositories.iter()
    for image_repo in itr_data:
        print(image_repo)
finally:
        connection_acct_admin.close()
