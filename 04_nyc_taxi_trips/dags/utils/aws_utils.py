from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
# from airflow.models.connection import Connection
from airflow.sdk import Connection
from airflow.providers.amazon.aws.hooks.glue_catalog import GlueCatalogHook
from io import BytesIO
def check_aws_connection(conn_id):
    """
    Checks whether the specified AWS connection is valid and accessible.
    Args:
        conn_id (str): The Airflow connection ID used to authenticate with AWS.
    Returns:
        bool: True if the connection is valid and AWS credentials work; False otherwise.
    Raises:
        Exception: If the connection is not found or AWS credentials are invalid.
    """
    try:
        aws_conn = S3Hook.get_connection(conn_id)
        print("s3 hook",aws_conn)
        conn = Connection(conn_id=aws_conn.conn_id, 
                          conn_type=aws_conn.conn_type,
                          login=aws_conn.login,
                          password=aws_conn.password
                          )
        conn_uri = conn.get_uri()
        print(f"conn uri {conn_uri}")
        print("S3 connection is successful")
        return True
    except Exception as e:
        print(f"AWS connection failed: {e}")
        raise
def check_redshift_conn(conn_id):
    try:
        redshift_conn = RedshiftSQLHook.get_connection(conn_id)
        conn = Connection(conn_id=redshift_conn.conn_id, 
                          conn_type=redshift_conn.conn_type,
                          login=redshift_conn.login,
                          password=redshift_conn.password
                          )
        conn_uri = conn.get_uri()
        print(f"conn uri {conn_uri}")
        return True
    except Exception as e:
        print(f"AWS connection failed: {e}")
        raise
def check_s3_prefix_existence(s3_hook,bucket_name, prefix, delimiter='/'):
    """
    Checks whether a given prefix exists in the specified S3 bucket.
    Args:
        s3_hook (S3Hook): An instance of Airflow's S3Hook used to interact with S3.
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix (path) to check for existence.
        delimiter (str, optional): Delimiter used to group keys. Defaults to '/'.
    Returns:
        bool: True if the prefix exists in the bucket; False otherwise.
    """
    return s3_hook.check_for_prefix(prefix, delimiter, bucket_name)

def upload_file_ob(s3_hook, s3_bucket_name, prefix, df):
    """
    Uploads a DataFrame as a file object to the specified location in an S3 bucket.
    Args:
        s3_hook (S3Hook): An instance of Airflow's S3Hook used to interact with S3.
        s3_bucket_name (str): The name of the S3 bucket where the file will be uploaded.
        prefix (str): The S3 key (including folder path and filename) where the file will be stored.
        df (pandas.DataFrame): The DataFrame to upload, typically converted to CSV or another format before upload.
    Returns:
        None
    """
    csv_bytes = df.to_csv(index=False).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    csv_buffer.seek(0)
    s3_hook.load_file_obj(
        file_obj=csv_buffer,
        key=prefix,
        bucket_name=s3_bucket_name,
        replace=True # Set to True to overwrite if the key already exists
    )

def upload_file(s3_hook, s3_bucket_name, prefix, local_file_path):
    """
    Uploads a file to the specified location in an S3 bucket.
    Args:
        s3_hook (S3Hook): An instance of Airflow's S3Hook used to interact with S3.
        s3_bucket_name (str): The name of the S3 bucket where the file will be uploaded.
        prefix (str): The S3 key (including folder path and filename) where the file will be stored.
        df (pandas.DataFrame): The DataFrame to upload, typically converted to CSV or another format before upload.
    Returns:
        None
    """
    s3_hook.load_file(
        filename=local_file_path,
        key=prefix,
        bucket_name=s3_bucket_name,
        replace=True
    )
    print(f"File '{local_file_path}' uploaded to s3://{s3_bucket_name}/{prefix}")

def get_glue_partitions(conn_id):
    """
    Retrieves partition information from AWS Glue Data Catalog
    Args:
        conn_id (str): The Airflow connection to access AWS Glue.
    Returns:
        list: A list of partition values or dictionaries
    """
    hook = GlueCatalogHook(aws_conn_id=conn_id)
    glue_partitions = hook.get_partitions(database_name='citibike_database', table_name='clean_zones')
    return glue_partitions