#!/opt/conda/bin/python3
import os

from snowflake.core import Root
from snowflake.core.compute_pool import ComputePool
from snowflake.core.image_repository import ImageRepository

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
    connection_container_user_role.close()
