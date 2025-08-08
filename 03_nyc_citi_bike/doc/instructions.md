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
    spark-submit /home/hadoop/workspace/terraform/my_glue_job.py --job_name my_test_glue_job --s3_input_path s3://nyc-citi-bikes-6dfe261a97b83ccc/raw_zones/year=2025/month=03/ --s3_output_path s3://nyc-citi-bikes-6dfe261a97b83ccc/clean_zones/year=2025/month=03/


use redshift spectrum because can directly query on s3: https://docs.aws.amazon.com/redshift/latest/dg/c-getting-started-using-spectrum.html
terraform to create redshfit cluster 
terraform: store sensitive info on secret.tfvars
terraform apply  -var-file="secret.tfvars"

go to redshfit query editor v2
create externa schema (use redhisft query editor v2)
create external schema citibike_schema 
from data catalog 
database 'citibike_db' 
iam_role 'arn:aws:iam::459266566350:role/createdRedShiftRole-6dfe261a97b83ccc'
create external database if not exists;

note: i tested and using glue crawler is more efficient because I don't have to add the partition by myself. Glue crawler will handler that

In redshift, to add partition you have to do each partion manually
create external table citibike_schema.citibikes_fact_table(
    rideable_type VARCHAR,
    started_at timestamp,
    ended_at timestamp,
    start_station_name VARCHAR,
    start_station_id VARCHAR,
    end_station_name VARCHAR,
    end_station_id VARCHAR,
    start_lat DOUBLE PRECISION,
    start_lng DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,
    end_lng DOUBLE PRECISION,
    member_casual VARCHAR,
    trip_duration bigint
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
location 's3://nyc-citi-bikes-6dfe261a97b83ccc/clean_zones/'

so I instead use aws glue crawler, which is triggered by dag
create the database and glue crawler via. terraform

then use dbt for sematic transformation
pip install dbt-core dbt-athena
go to dbt folder: dbt init dbt_athena

profile is saved in : /Users/u249637/.dbt/profiles.yml


#dbt
prifle.yml
because astro use docker, it need .profile.yml to be available in the docker container
so copy to dbt_athena/
my_dbt_athena:
  target: dev
  outputs:
    dev:
      type: athena
      database: citibike_database
      schema: dbt
      s3_data_dir: s3://nyc-citi-bikes-6dfe261a97b83ccc/dbt/
      s3_staging_dir: s3://nyc-citi-bikes-6dfe261a97b83ccc/athena_results/
      region_name: us-east-1
      catalog: AwsDataCatalog
      aws_profile_name: son
      threads: 4

dbt debug to check connection