terraform: `terraform apply -var-file="secret.tfvars" `
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.4/docker-compose.yaml'
`echo -e "AIRFLOW_UID=$(id -u)" >> .env`
`docker build .` : # to install aws services for airflow
Initialize the Airflow scheduler, DB, and other config: `docker-compose up airflow-init`
Kick up the all the services from the container: `docker-compose up -d`
to go inside the docker contaienr: docker-compose exec 

username and password use default(airflow)

how to set variable
1. either using UI
2. docker-compose exec airflow-scheduler airflow variables set my_var "my_value"
OR `docker-compose exec -it airflow-scheduler airflow variables import variables.json`
confirm:  docker-compose exec -it airflow-scheduler airflow variables list
Note: all components (scheduler, workers, webserver) can access those variables because they pull them from the shared metadata database. so
setting in airflow-webserver is sufficient
3. at beginning, in docker-compose.yml, in airflow init service:     airflow variables import variables.json 

to create aws connection: airflow connections add aws_default \
          --conn-type aws \
          --conn-extra '{"region_name": "${AWS_DEFAULT_REGION}"}'

verify: docker-compose exec -it airflow-scheduler bash airflow connections list
4. how to set up redhisft connection on UI: https://www.astronomer.io/docs/learn/connections/redshift/
or use the command
airflow connections add redshift_default --conn-uri "aws://<your_aws_access_key_id>:<your_aws_secret_access_key>@<your_redshift_endpoint>:5439/<your_redshift_database>"
docker-compose exec -it airflow-scheduler  airflow connections list
airflow connections get postgres_conn --output json
4. i use redshfit spectrum not redshift so I need to create external schema
the redshift cluster need to be publically available so i can connect from my laptop locally
this create extera schema in database 'dev'
create external schema myspectrum_schema 
from data catalog 
database 'nyc_taxi_database' 
iam_role 'arn:aws:iam::459266566350:role/createdRedShiftRole-81d60579e425d845'
create external database if not exists;