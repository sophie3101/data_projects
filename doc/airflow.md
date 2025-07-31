1. Setting up: using astro cli

Install:
`brew install astro`

Go to empty directory and run `astro dev init` to create an Astro project

run `astro dev start` to run the project 


Troubleshoot: By default, the Astro CLI uses port 8080 for the Airflow webserver and port 5432 for the Airflow metadata database 
Change the default ports for these components. For example, you can use `astro config set webserver.port 8081` for the webserver and `astro config set postgres.port 5433` for Postgres.
run `astro dev restart` to run the project

Apache Airflow uses a PostgreSQL database (or other supported databases like MySQL) to store its metadata, which includes information like:

- Task run history
- DAG runs
- Variables, connections
- Logs (if configured)
- User authentication (if using RBAC UI)
To view the airflow database using pgADmin. Register server with information from astro dev restart
➤ Postgres Database: postgresql://localhost:5433/postgres
➤ The default Postgres DB credentials are: postgres:postgres
in which: username is localhost, port is 5433, password is postgres

Terminology
- DAG: airflow DAG is a workflow defined as a graph
- DAG run: instance of a DAG running at specific time pont. history of previous DAG runs is stored in Airflow metadata database
- Task: a step in DAG describing single unit of work. Tasks are defined using Airlofw building blocks
- Task instance: execution of a task at specific time point
- Dysnamtic task: Airflow task. that serves as a blueprint


the `astro dev start` command builds your project and spins up 4 Docker containers on your machine, each for a different Airflow component:

Postgres: Airflow's metadata database
Webserver: The Airflow component responsible for rendering the Airflow UI
Scheduler: The Airflow component responsible for monitoring and triggering tasks
Triggerer: The Airflow component responsible for running Triggers and signaling tasks to resume when their conditions have been met. The triggerer is used exclusively for tasks that are run with deferrable operators

use `docker ps ` to see all running containers 

use `astro dev restart` to rerun astro after you make changes
Astro CLI is built on top of Docker Compose

working with metadata and REST API: https://www.astronomer.io/docs/learn/airflow-database/
2. how to add files:
https://www.astronomer.io/docs/software/customize-image
basically, you modify any files or add files and folder, then run `astro dev stop` then `astro dev start`. Confirm the changes by running: `astro dev bash` or `docker exec -it <scheduler-container-id> /bin/bash` 

3. use airflow cli on astromer: `docker exec -it SCHEDULER_CONTAINER bash -c "airflow connections`
or go to sheduler cotnainer by run `astro deb bash` and then u can run any airflow command

or to use sql on database (pgAdmin)


4. decorator or operator: basically, decorator makes the script look cleaner https://www.astronomer.io/docs/learn/airflow-decorators/

5. connect to postgreSQL:
- create connection on airflowUI (admin => create connections)
-  use PostgreSQL Hook in DAG script

6. s3 conenction
in requirements.txt file, put apache-airflow-providers-amazon
then run astro dev restart
to create connection, either manually create in UI (admin-> create connections) or define in airflow_setttings.yaml
to verify connection: 
astro dev bash -> airflow connections get aws_connection 
