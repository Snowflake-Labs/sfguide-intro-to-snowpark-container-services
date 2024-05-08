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
    # cd .../sfguide-intro-to-snowpark-container-services/src/convert-api
    # docker build --platform=linux/amd64 -t <local_repository>/convert-api:latest .
    client.images.build(path='sfguide-intro-to-snowpark-container-services/src/convert-api', platform='linux/aarch64', tag='<local_repository>/convert-api:latest')

    # Check to see if the image is there
    # Verify the image built successfully:
    # docker image list
    client.images.list()

    # Test running the image
    # docker run -d -p 9090:9090 <local_repository>/convert-api:latest
    container = client.containers.run(image='<local_repository>/convert-api:latest', detach=True, ports={9090: 9090})

    # Use CURL to test the service
    # curl -X POST -H "Content-Type: application/json" -d '{"data": [[0, 12],[1,19],[2,18],[3,23]]}' http://localhost:9090/convert
    # you can stop the container: `docker stop convert-api`.
    os.system("""curl -X POST 
                -H "Content-Type: application/json" 
                -d '{"data": [[0, 12],[1,19],[2,18],[3,23]]}' 
                http://localhost:9090/convert """)

    # Grab the image
    image = next(i for i in client.images.list() if "<local_repository>/convert-api:latest" in i.tags)

    # Tag it
    #  # e.g. if repository_url = org-account.registry.snowflakecomputing.com/container_hol_db/public/image_repo,
    #  snowflake_registry_hostname = org-account.registry.snowflakecomputing.com
    #   docker login <snowflake_registry_hostname> -u <user_name>
    #   > prompt for password
    #  docker tag <local_repository>/convert-api:latest <repository_url>/convert-api:dev
    image.tag(repository_url, 'dev')

    # Push the image to the remote registry
    # docker push <repository_url>/convert-api:dev
    client.api.push(repository_url + '/convert-api:dev')

    # USE ROLE CONTAINER_USER_ROLE;
    # CALL SYSTEM$REGISTRY_LIST_IMAGES('/CONTAINER_HOL_DB/PUBLIC/IMAGE_REPO');
    images = root.databases["CONTAINER_HOL_DB"].schemas["PUBLIC"].image_repositories["IMAGE_REPO"].listImagesInRepository()
    for image in images:
        print(image)

    # you can stop the container: `docker stop convert-api`.
    container.stop()

finally:
    connection_container_user_role.close()

