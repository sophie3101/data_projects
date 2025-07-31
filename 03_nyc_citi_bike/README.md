Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

Start Airflow on your local machine by running 'astro dev start'.

This command will spin up five Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- DAG Processor: The Airflow component responsible for parsing DAGs
- API Server: The Airflow component responsible for serving the Airflow UI and API
- Triggerer: The Airflow component responsible for triggering deferred tasks

When all five containers are ready the command will open the browser to the Airflow UI at http://localhost:8080/. You should also be able to access your Postgres Database at 'localhost:5432/postgres' with username 'postgres' and password 'postgres'.

Note: If you already have either of the above ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.

https://grok.com/share/c2hhcmQtMw%3D%3D_952be65a-7328-4e9d-a82d-7ad3c28760ad

set s3 bucket name
set variable in airflow_settings.yaml

test glue job locally
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python

1. docker pull public.ecr.aws/glue/aws-glue-libs:5 
2. 
WORKSPACE_LOCATION=/Users/u249637/Documents/Personal/data_projects/03_nyc_citi_bike/
SCRIPT_FILE_NAME=dags/utils/glue_spark_test.py

3.
docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v "$WORKSPACE_LOCATION:/home/hadoop/workspace" \
    -v "$WORKSPACE_LOCATION/temp:/home/hadoop/temp" \
    -e AWS_PROFILE=son \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/"$SCRIPT_FILE_NAME"

docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v "$WORKSPACE_LOCATION:/home/hadoop/workspace" \
    -v "$WORKSPACE_LOCATION/temp:/home/hadoop/temp" \
    -e AWS_PROFILE=son \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/dags/utils/glue_spark.py --job_name my_test_glue_job --s3_input_path s3://nyc-citi-bikes-6dfe261a97b83ccc/raw_zones/year=2025/month=03/ --s3_output_path s3://nyc-citi-bikes-6dfe261a97b83ccc/clean_zones/year=2025/month=03/