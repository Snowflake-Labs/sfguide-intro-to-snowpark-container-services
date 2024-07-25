# First, install the Docker CLI on your system.
# Then pip install docker to install the Docker SDK for Python.
import os
import re
import docker

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

    # Get the image repository URL
    #   use role CONTAINER_user_role;
    #   show image repositories in schema CONTAINER_HOL_DB.PUBLIC;
    #   // COPY the repository_url field, e.g. org-account.registry.snowflakecomputing.com/container_hol_db/public/image_repo
    #   ```
    #   ```bash
    #   # e.g. if repository_url = org-account.registry.snowflakecomputing.com/container_hol_db/public/image_repo, snowflake_registry_hostname = org-account.registry.snowflakecomputing.com
    repos = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].image_repositories
    repo = repos["IMAGE_REPO"].fetch()

    # Extract the registry hostname from the repository URL
    pattern = r'^[^/]+'

    repository_url = repo.repository_url
    match = re.match(pattern, repository_url)
    registry_hostname = match.group(0)

    # Docker client
    client = docker.from_env()

    #   docker login <snowflake_registry_hostname> -u <user_name>
    #   > prompt for password
    # Login to the remote registry. Give user name and password for docker login
    client.login(username = "<username>",
                        password = "<password>",
                        registry = registry_hostname,
                        reauth = True)


    # Build the Docker Image in the Example
    # cd .../sfguide-intro-to-snowpark-container-services/src/jupyter-snowpark
    # docker build --platform=linux/amd64 -t <local_repository>/python-jupyter-snowpark:latest .
    client.images.build(path='sfguide-intro-to-snowpark-container-services/src/jupyter-snowpark', platform='linux/aarch64', tag='<local_repository>/python-jupyter-snowpark:latest')

    # Check to see if the image is there
    # Verify the image built successfully:
    # docker image list
    client.images.list()

    # Test running the image
    # docker run -d -p 8888:8888 <local_repository>/python-jupyter-snowpark:latest
    container = client.containers.run(image='<local_repository>/python-jupyter-snowpark:latest', detach=True, ports={8888: 8888})

    # Use CURL to test the service
    # Open up a browser and navigate to [localhost:8888/lab](http://localhost:8888/lab) to verify
    # your notebook service is working locally. Once you've verified that the service is working,
    # you can stop the container: `docker stop python-jupyter-snowpark`.
    os.system("""curl -X GET  http://localhost:8888/lab""")

    # Grab the image
    image = next(i for i in client.images.list() if "<local_repository>/python-jupyter-snowpark:latest" in i.tags)

    # Tag it
    #  # e.g. if repository_url = org-account.registry.snowflakecomputing.com/container_hol_db/public/image_repo,
    #  snowflake_registry_hostname = org-account.registry.snowflakecomputing.com
    #   docker login <snowflake_registry_hostname> -u <user_name>
    #   > prompt for password
    #   docker tag <local_repository>/python-jupyter-snowpark:latest <repository_url>/python-jupyter-snowpark:dev
    image.tag(f"{repository_url}/python-jupyter-snowpark", 'dev')

    # Push the image to the remote registry
    # docker push <repository_url>/python-jupyter-snowpark:dev
    client.api.push(repository_url + '/python-jupyter-snowpark:dev')

    # USE ROLE CONTAINER_USER_ROLE;
    # CALL SYSTEM$REGISTRY_LIST_IMAGES('/CONTAINER_HOL_DB/PUBLIC/IMAGE_REPO');
    images = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].image_repositories["IMAGE_REPO"].list_images_in_repository()
    for image in images:
        print(image)


    # you can stop the container: `docker stop python-jupyter-snowpark`.
    container.stop()

finally:
    connection_container_user_role.close()

