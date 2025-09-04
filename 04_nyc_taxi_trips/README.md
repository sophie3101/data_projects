# NYC taxi city - Data Engineer approach
This project is a modern data pipeline built to ingest, transform, store, and analyze NYC taxi data. It leverages modern data engineering tools including Apache Airflow, Terraform, AWS Glue, Amazon S3, Redshift, dbt for end-to-end orchestration, data transformation and warehousing.

## Workflow Summary

### 1. Use **Terraform** to set up infrastucture
Deploy infrastructure with Terraform using secrets:

`terraform apply -var-file="secret.tfvars" `

This command will spin following AWS services:
- Amazon S3 bucket
- IAM roles and policies
- Redshift cluster
- AWS Glue  catalog

### 2. Airflow Setup

- download Airflow Docker Compose: 

  ```curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml'```

- prepare Environment

  ```echo -e "AIRFLOW_UID=$(id -u)" >> .env```

- build Docker services: 

  ```
    docker-compose up airflow-init #Initialize the Airflow scheduler, DB, and other config

    docker-compose up -d   #Kick up the all the services from the container
  ``` 

Initialize the Airflow scheduler, DB, and other config: `docker-compose up airflow-init`


### 3. DBT

create a fact table that includes trip info plus the pickup and dropoff zone names.

