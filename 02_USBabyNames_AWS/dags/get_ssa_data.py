from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

import requests,zipfile, io, os, json
from utils.get_logger import get_logger 
from utils.load_config import load_config
from utils.s3_utils import check_s3_file_exists, check_bucket_exists


logger = get_logger(__name__, "dag.log")
try:
    CONFIG = load_config("config.ini")
except Exception as e: 
    logger.error(e)
BUCKET=CONFIG.get("AWS", "bucket")
AWS_CONN_ID="s3_connection"
S3_HOOK = S3Hook(aws_conn_id=AWS_CONN_ID)
@dag (
    schedule=None,
    catchup=False,
)
def get_ssa_data_pipeline():
    @task
    def download_file_from_link():
        """
            This function is to download zip folder from link {url} to {des_folder}
            and the compressed link is extracted to {des_fodler}
            Returns:
                True if the downloading and extracting files are successful
        """
        try:
            
            headers = {
                'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            url = CONFIG.get("SSN", "url")
            des_folder = CONFIG.get("DATA_FOLDER", "res_folder")
            os.makedirs(des_folder, exist_ok=True )

            logger.info(f"URL: {url}")
            logger.info(des_folder)
            if os.path.exists(des_folder):
                logger.info("Files already present. Skipping download.")
                return 
            response = requests.get(url, stream=True, headers=headers)
            response.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                zip_file.extractall(des_folder)
                logger.info("ZIP file downloaded and extracted successfully.")
            return True
        except Exception as e:
            logger.error(f"Error in downloading the link {e} ")
            return False
    
    @task 
    def upload_to_s3( **kwargs):
        """
        The function upload file to s3 bucket. It check whether s3 bucket exists first, then check if the file exists in s3 bucket
        If not, upload the file to s3 bucket
        """
        bucket_name  = CONFIG.get("AWS", "bucket")
        s3_key = CONFIG.get("AWS", "prefix")
        
        uploaded_keys=[]
        if check_bucket_exists(bucket_name, S3_HOOK):
            in_dir = CONFIG.get("DATA_FOLDER", "res_folder")
            for file in os.listdir(in_dir):
                if 'pdf' not in file:
                    # logger.info(f"File: {file}")
                    cur_key = f"{s3_key}/{os.path.basename(file)}"
                    file_path = os.path.join(in_dir, file)
                    if not check_s3_file_exists(bucket_name, cur_key, S3_HOOK):
                        logger.info(f"Uploading {file} to {cur_key}")
                        S3_HOOK.load_file(file_path, cur_key, bucket_name, replace=True )
                    
                    
                    uploaded_keys.append(cur_key)
         # kwargs['ti'].xcom_push(key='raw_keys', value=uploaded_keys) # don't have to use xcome because i'm using decorators
        return uploaded_keys
    
    @task
    def generate_lambda_payload(uploaded_files):
        payloads = []
        for file_key in uploaded_files:            
            processed_file_key=file_key.replace("raw", "processed").replace(".txt", ".parquet")
            logger.info(f"{file_key} {processed_file_key}")
            if not check_s3_file_exists(BUCKET, processed_file_key, S3_HOOK):
                one_record={"Records":[{
                        "s3": {
                            "bucket": {
                                "name": CONFIG.get("AWS", "bucket"),
                            },
                            "object": {
                                "key": file_key,
                            }
                        }
                }
                    ],
                }
                payloads.append(json.dumps(one_record))
        
        return  payloads

    @task
    def get_processed_file(raw_file_keys):
        expected_keys = []
        for raw_file_key in raw_file_keys:
            processed_file_key = raw_file_key.replace("raw", "processed").replace(".txt", ".parquet")
            expected_keys.append(processed_file_key)
            # file_name, file_extension= os.path.splitext(raw_file_key)
            # processed_file_name, processed_file_extension = file_name.replace("raw", "processed"), 
        return expected_keys
    
    download_task = download_file_from_link()
    uploaded_files = upload_to_s3(aws_conn_id="s3_connection")
    payloads = generate_lambda_payload(uploaded_files)
    processed_files = get_processed_file(uploaded_files)

    wait_for_uploaded_s3_files = S3KeySensor.partial( #partial to set common arugyment shared by all task instances
        task_id="wait_for_uploaded_file",
        aws_conn_id="s3_connection",
        bucket_name=BUCKET,
        poke_interval=10,
        timeout=600,
        mode="poke",
    ).expand(
        bucket_key=uploaded_files
    )

    invoke_lambdas = LambdaInvokeFunctionOperator.partial(
        task_id="invoke_lambda",
        function_name=CONFIG.get("AWS", "lambda_func"),
        aws_conn_id=AWS_CONN_ID,
        region_name="us-east-1",
        invocation_type="Event", ## Asynchronous invocation
    ).expand(payload=payloads)

    wait_for_s3_processed_files = S3KeySensor.partial(
        task_id="wait_for_processed_file",
        aws_conn_id=AWS_CONN_ID,
        bucket_name=BUCKET,
        poke_interval=10,
        timeout=600,
        mode="poke",
    ).expand(
        bucket_key=processed_files
    )

    glue_crawler=GlueCrawlerOperator(
        task_id="glue_crawler_task",
        config={"Name": "babyNamesCrawler"},
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        region_name="us-east-1",
    )


    # chain(download_task, 
    #         uploaded_files,
    #         wait_for_raw_s3_files, 
    #         wait_for_s3_processed_files,
    #         glue_crawler)
    download_task >> uploaded_files >>  processed_files >> wait_for_uploaded_s3_files  >> payloads >> invoke_lambdas \
       >> wait_for_s3_processed_files >> glue_crawler


get_ssa_data_pipeline()