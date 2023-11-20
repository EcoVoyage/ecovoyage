# GeoNode Meets Docker
## Mapping the Future of Geospatial Data Management

> Can you give me the instructions how to run the latest version of GeoNode using Docker?

To run the latest version of GeoNode using Docker, follow these steps:

1. Install prerequisites:
   - Install Docker: Follow the instructions for your platform at https://docs.docker.com/engine/install/
   - Install Docker Compose: Follow the instructions at https://docs.docker.com/compose/install/

2. Clone the GeoNode repository:
   ```
   git clone https://github.com/GeoNode/geonode-project.git
   cd geonode-project
   ```

3. Checkout the latest stable branch (replace `3.x.x` with the latest stable version):
   ```
   git checkout tags/3.x.x
   ```

4. Build and start the GeoNode Docker containers:
   ```
   docker-compose up --build -d
   ```

   This step may take a while as it downloads and builds the necessary Docker images. Once it's finished, you should see the message "Starting geonode ... done" in the terminal.

5. Initialize the GeoNode database:
   ```
   docker-compose exec geonode /bin/bash -c "python manage.py migrate"
   docker-compose exec geonode /bin/bash -c "python manage.py collectstatic --noinput"
   docker-compose exec geonode /bin/bash -c "python manage.py loaddata sample_admin"
   docker-compose exec geonode /bin/bash -c "python manage.py loaddata default_oauth_apps"
   docker-compose exec geonode /bin/bash -c "python manage.py loaddata initial_data"
   ```

   This step may take a few minutes to complete.

6. You should now have the latest version of GeoNode running on your machine. Open your web browser and navigate to http://localhost/. You should see the GeoNode homepage.

7. To log in to the GeoNode admin interface, use the following credentials:
   - Username: `admin`
   - Password: `admin`

8. To stop the GeoNode Docker containers, run the following command in the terminal:
   ```
   docker-compose down
   ```

For more information and advanced configuration options, refer to the official GeoNode documentation at https://docs.geonode.org/en/master/usage/index.html.


