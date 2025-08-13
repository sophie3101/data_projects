from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator    
from airflow.utils.task_group import TaskGroup
from airflow.sdk import Variable
from utils.aws_utils import *
from utils.file_utils import download_parquet_file
from utils.get_weather_data import *
from utils.dag_functions import get_raw_parquet_links, is_table_exists, get_months_to_partition, add_partition, get_month_partions
from datetime import datetime
import  os, json

AWS_CONN_ID = "aws_default"
REDSHIFT_CONN_ID = "redshift_conn"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")

RAW_DIR="raw_trips/"
os.makedirs(RAW_DIR, exist_ok=True)

@dag(
    schedule="@monthly",
    catchup=False,
    doc_md=__doc__,
    tags=["nyc_taxi"],
    max_active_tasks=3, 
)
def nyc_taxi_dag():
    
    @task 
    def verify_aws_connection():
        """Verify AWS connection is valid
        Returns:
            bool:True if connection is valid
        Raises:
            AirflowException: If the data retrieval fails or an error occurs during the process.
        """
        try:
            is_valid_conn=check_aws_connection(AWS_CONN_ID)
            is_valid_conn=check_redshift_conn(REDSHIFT_CONN_ID)
            return is_valid_conn
        except Exception as e:
            raise AirflowException(f"AWS connection verification failed: {str(e)}")
    
    @task(task_id="prepare_raw_links")
    def prepare_ingest_links(taxi_color):
        """
            Retrieves nyc taxi parquet file links from the NYC taxi website.
        """

        s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)
        parquet_configs = get_raw_parquet_links(taxi_color, s3_hook, S3_BUCKET_NAME, RAW_DIR)

        return parquet_configs
       
   
    @task(task_id="upload_raw_parquet_file")
    def upload_raw_trip(config_tuple):
        s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)
        link, local_file, s3_prefix = config_tuple
        download_parquet_file(link, local_file)
        if not check_s3_prefix_existence(s3_hook, S3_BUCKET_NAME, os.path.dirname(s3_prefix)):
            upload_file(s3_hook, S3_BUCKET_NAME, s3_prefix , local_file)

    @task.branch(task_id='check_external_schema')
    def check_redshift_schema(skip_task_id, create_schema_task_id):
        schema_name= Variable.get("redshift_external_schema")
        hook = RedshiftSQLHook(REDSHIFT_CONN_ID)
        print(schema_name)
        sql = f"""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = %s;
        """
        result = hook.get_first(sql, parameters=(schema_name,))
        print("RESULTS", result)
        if result is not None:
            print(f"Schema '{schema_name}' exists.")
            return skip_task_id 
        else:
            return create_schema_task_id
        
    @task(task_id='create_external_schema')
    def create_external_schema():
        # create external schema in dev database of redshift cluster
        hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name= Variable.get("redshift_external_schema")
        glue_database = Variable.get("glue_database")
        iam_role =  Variable.get("redshift_role")
        region=os.getenv("AWS_DEFAULT_REGION")
        hook.run(f"""
            CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name}
            FROM DATA CATALOG
            DATABASE '{glue_database}'
            IAM_ROLE '{iam_role}'
            REGION '{region}';
        """)
    
    @task(task_id="create_n_update_external_raw_trip_table")
    def create_n_update_external_table(taxi_color):
        try:
            cur_year = str(datetime.now().year)
            s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)
            s3_paths, months = get_months_to_partition(s3_hook, S3_BUCKET_NAME, taxi_color)

            # check if table exists, if not create table and add parition
            # if yes, just add partitions
            hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
            table_name = f"{taxi_color}_taxi_raw_trips"      
            schema_name= Variable.get("redshift_external_schema")

            if not is_table_exists(hook, schema_name, table_name):
                print("CREATING TABLE ")
                s3_path_root = s3_paths[0].split("/year")[0]
                create_stmt = f"""
                    create external table {schema_name}.{table_name}(     
                        vendorid INT,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count BIGINT,
                        trip_distance DOUBLE PRECISION,
                        ratecodeid BIGINT,
                        store_and_fwd_flag VARCHAR,
                        pulocationid INT,
                        dolocationid INT,
                        payment_type BIGINT,
                        fare_amount DOUBLE PRECISION,
                        extra DOUBLE PRECISION,
                        mta_tax DOUBLE PRECISION,
                        tip_amount DOUBLE PRECISION,
                        tolls_amount DOUBLE PRECISION,
                        improvement_surcharge DOUBLE PRECISION,
                        total_amount DOUBLE PRECISION,
                        congestion_surcharge DOUBLE PRECISION,
                        airport_fee DOUBLE PRECISION,
                        cbd_congestion_fee DOUBLE PRECISION
                    )
                    PARTITIONED BY (year INT, month INT)
                    STORED AS PARQUET
                    location '{s3_path_root}';
                """
                hook.run(sql=create_stmt, autocommit=True)

            
            parsed_months = get_month_partions(hook, schema_name, table_name)
            print("PARSED MONTH", parsed_months)
            new_months = [m for m in months if m not in parsed_months]
            print("NEW ", new_months)
            for month in new_months:
                print("ADDING PARTITION")
                s3_path =  f"{s3_paths[0].split("/month")[0]}/month={month}"
                print(s3_path)
                add_partition(hook, schema_name, table_name, cur_year, month, s3_path)
            
        except Exception as e:
            raise AirflowException(f"AWS connection verification failed: {str(e)}")

    with TaskGroup(group_id="yellow_taxi") as yellow_taxi_group:
        taxi_color="yellow"
        with TaskGroup(group_id="yellow_taxi_ingestion") as yellow_ingestion_group:
            ingested_configs = prepare_ingest_links(taxi_color)
            upload_tasks = upload_raw_trip.expand(config_tuple=ingested_configs)
            ingested_configs >> upload_tasks

        with TaskGroup(group_id="loading_raw_table_to_redshift_y") as yellow_raw_load_group:
            start_load = EmptyOperator( task_id="yellow_taxi.start_load",
                    trigger_rule=TriggerRule.ALL_DONE  # Runs if upstream succeeded OR skipped OR failed
                )
            skip_schema_task = EmptyOperator(task_id="skip_schema_creation_task")
            check_schema_task = check_redshift_schema("yellow_taxi.loading_raw_table_to_redshift_y.skip_schema_creation_task", "yellow_taxi.loading_raw_table_to_redshift_y.create_external_schema")
            create_schema_task = create_external_schema()
            create_or_update_table_task = create_n_update_external_table(taxi_color)
            empty_task = EmptyOperator( task_id="yellow_taxi.empty",
                    trigger_rule=TriggerRule.ALL_DONE  # Runs if upstream succeeded OR skipped OR failed
                )
            start_load >> check_schema_task >> [skip_schema_task, create_schema_task]>>empty_task >>create_or_update_table_task
            

        yellow_ingestion_group >> yellow_raw_load_group

    with TaskGroup(group_id="green_taxi") as green_taxi_group:
        taxi_color="green"
        with TaskGroup(group_id="green_taxi_ingestion") as green_ingestion_group:
            ingested_configs = prepare_ingest_links(taxi_color)
            upload_tasks = upload_raw_trip.expand(config_tuple=ingested_configs)
            ingested_configs >> upload_tasks

        with TaskGroup(group_id="loading_raw_table_to_redshift_gr") as green_raw_load_group:
            start_load = EmptyOperator( task_id="green_taxi.start_load",
                    trigger_rule=TriggerRule.ALL_DONE  # Runs if upstream succeeded OR skipped OR failed
                )
            skip_schema_task = EmptyOperator(task_id="skip_schema_creation_task")
            # empty_task = EmptyOperator(task_id="skip_task")
            check_schema_task = check_redshift_schema("green_taxi.loading_raw_table_to_redshift_gr.skip_schema_creation_task", "green_taxi.loading_raw_table_to_redshift_gr.create_external_schema")
            create_schema_task = create_external_schema()
            empty_task = EmptyOperator( task_id="green_taxi.empty",
                    trigger_rule=TriggerRule.ALL_DONE  # Runs if upstream succeeded OR skipped OR failed
                )
            create_or_update_table_task = create_n_update_external_table(taxi_color)
            start_load >> check_schema_task >> [skip_schema_task, create_schema_task]>>empty_task >>create_or_update_table_task
            # create_or_update_table_task = create_n_update_external_table("yellow")

        green_ingestion_group >> green_raw_load_group
        
        
    # Define DAG flow
    init = EmptyOperator(task_id="init")
    init >> [yellow_taxi_group, green_taxi_group] 
    
    
    # """test single task"""
    # create_or_update_table_task = create_n_update_external_table("yellow")
    # create_or_update_table_task

nyc_taxi_dag()