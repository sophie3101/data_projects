from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.standard.operators.empty import EmptyOperator    

from airflow.models import Variable
from utils.aws_utils import *
from utils.html_parser import *
from utils.file_utils import *
from datetime import datetime
import re, logging

AWS_CONN_ID = "aws_connection"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")
REDSHIFT_CONN_ID = "redshift_connection"

@dag(
    schedule="@monthly",
    catchup=False,
    doc_md=__doc__,
    tags=["nyc_citi_bikes"],
    max_active_tasks=3, 
)
def citi_bikes_etl():
    @task
    def verify_aws_connection():
        """Verify AWS connection is valid"""
        try:
            check_aws_connection(AWS_CONN_ID)
        
        except Exception as e:
            raise AirflowException(f"AWS connection verification failed: {str(e)}")
    
    @task
    def get_zip_files_to_process(url):
        """For inital load, extract all zip file links 
       For monthly runs, process only the previous month's data.
        """
        s3_raw_zones= "raw_zones"
        s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)
       
        zip_names = fetch_zip_files(url)
        filter_flag = Variable.get("filter", default_var="false", deserialize_json=True)

        if filter_flag==1: 
            now=datetime.now()
            cur_year = str(now.year)
            prev_month = f"{now.month-1:02d}"
            zip_names = [n for n in zip_names if n[1]==cur_year and n[2]==prev_month]
      
        return zip_names
    
    @task
    def extract_n_upload_one_ziplink(zip_file_tuple):
        """Download and extract zip files, prepare files for S3 upload."""
        s3_raw_zones= "raw_zones"
        s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)

        zip_file_name, year, month = zip_file_tuple
        tmp_folder = f"tmp_{year}"

        print("EXTRACTING ", zip_file_name, year, month)
    
        s3_prefix = f"{s3_raw_zones}/year={year}"
        is_prefix_exists=False
        if month is not None:
            cur_s3_prefix=f"{s3_prefix}/month={month}"
            is_prefix_exists=check_s3_prefix_existence(s3_hook, S3_BUCKET_NAME, cur_s3_prefix )
        else:
            cur_s3_prefix = s3_prefix

        if is_prefix_exists==False:
            extracted_files=download_n_extract(zip_file_name, tmp_folder )
            for extracted_filepath in extracted_files:
                # print("extracted file",extracted_filepath,os.path.basename(extracted_filepath))
                if ".csv" not in extracted_filepath:
                    continue
                if month is None: #for year like 2020, it is group in each month
                    pattern = r"^(\d{4})(\d{2}).*\.csv$"
                    match = re.match(pattern, os.path.basename(extracted_filepath))
                    if match:
                        cur_month=match.groups()[1]
                        cur_s3_prefix=f"{s3_prefix}/month={cur_month}"
                    else:
                        raise AirflowException(f"not be able to get the month from the file {os.path.basename(extracted_filepath)}")

                try:
                    print(f"Uploading {extracted_filepath} to {cur_s3_prefix}")
                    upload_file(s3_hook, S3_BUCKET_NAME, f"{cur_s3_prefix}/{os.path.basename(extracted_filepath)}", extracted_filepath)
                except Exception as e:
                    raise AirflowException(f"Failed to upload {extracted_filepath} to S3: {str(e)}")
            
            #after files are uploaded, remove the temp folder
            remove_folder(tmp_folder)

    @task
    def get_s3_raw_path_to_process():
        """Identify S3 raw paths that need processing."""
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_raw_path_to_process_list = []
        
        raw_prefixes = s3_hook.list_prefixes(bucket_name=S3_BUCKET_NAME, prefix="raw_zones/", delimiter="/")
        if not raw_prefixes:
            raise AirflowException("No raw prefixes found in S3 bucket.")

        for year_prefix in raw_prefixes:
            month_prefixes = s3_hook.list_prefixes(bucket_name=S3_BUCKET_NAME, prefix=year_prefix, delimiter="/")
            for raw_month_prefix in month_prefixes:
                clean_month_prefix = raw_month_prefix.replace("raw_zones", "clean_zones")
                if not check_s3_prefix_existence(s3_hook, S3_BUCKET_NAME, clean_month_prefix):
                    s3_raw_path_to_process_list.append(raw_month_prefix)
        
        return s3_raw_path_to_process_list
    
    @task
    def prepare_glue_job_configs(s3_raw_path_to_process_list):
        """Prepare configurations for Glue jobs."""
        glue_script = Variable.get("glue_script")
        glue_script_s3_uri = f"s3://{S3_BUCKET_NAME}/{glue_script}"
        glue_job_configs = []

        for i, s3_raw_prefix in enumerate(s3_raw_path_to_process_list):
            s3_raw_uri = f"s3://{S3_BUCKET_NAME}/{s3_raw_prefix}"
            s3_clean_uri = f"s3://{S3_BUCKET_NAME}/{s3_raw_prefix.replace('raw_zones', 'clean_zones')}"
            cur_glue_job_name = f"to_parquet_{i}"
            glue_job_configs.append({    
                "job_name":   cur_glue_job_name,        
                "script_location": glue_script_s3_uri,
                "task_id": f"run_glue_job_{i}",
                "iam_role_name":Variable.get("glue_iam_role"),
                "script_args":{'--job_name': cur_glue_job_name, 
                            '--s3_input_path':s3_raw_uri, 
                            '--s3_output_path': s3_clean_uri
                            },
            })

        return glue_job_configs
   
    
    @task.branch(task_id="should_run_glue_crawler")
    def should_run_glue_crawler():
        start_glue_crawler=False
        glue_partions=get_glue_partitions(AWS_CONN_ID)
        year_partitions=[item[0] for item in glue_partions]
        print("GLUE partions ", glue_partions)
        print("GLUE YEAR PATRTIONS ", year_partitions)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        clean_prefixes = s3_hook.list_prefixes(bucket_name=S3_BUCKET_NAME, prefix="clean_zones/", delimiter="/")
        print(clean_prefixes)
        for year_prefix in clean_prefixes:
            year=year_prefix.replace("clean_zones/year=",'').replace("/",'')
            print(year, type(year))
            if year not in year_partitions:
                start_glue_crawler=True 
                break
     
        if start_glue_crawler:
            return "glue_crawler_task" #task id of glue crawler task
        
        return "skip_task"


    verify_connection_task = verify_aws_connection()
    """EXTRACT TASKS"""
    ziplinks_to_process = get_zip_files_to_process("https://s3.amazonaws.com/tripdata")
    extract_n_upload_tasks = extract_n_upload_one_ziplink.expand(zip_file_tuple=ziplinks_to_process)
    
    """TRANSFORM TASK: raw zones to clean zones"""
    s3_raw_paths=get_s3_raw_path_to_process()
    glue_configs = prepare_glue_job_configs(s3_raw_paths)
    glue_jobs=GlueJobOperator.partial(
        task_id="run_glue_job_",
        region_name="us-east-1", 
        aws_conn_id=AWS_CONN_ID,
        create_job_kwargs={
        'GlueVersion': '4.0',
        'NumberOfWorkers': 2,
        'WorkerType': 'G.1X',
        },
        wait_for_completion=True,       
    ).expand_kwargs(glue_configs)
    
    """create glue crawler to run query on REDSHIFT SPECTRUM"""
    run_glue_crawler = GlueCrawlerOperator(
        task_id="glue_crawler_task",
        config={"Name": "citiBikeCrawler"},
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        region_name="us-east-1",
    )

    choose_task = should_run_glue_crawler()
    skip_task = EmptyOperator(task_id="skip_task")
    
    verify_connection_task >> ziplinks_to_process>> extract_n_upload_tasks >>s3_raw_paths >> glue_configs >>glue_jobs >>choose_task >>[skip_task, run_glue_crawler]

citi_bikes_etl()