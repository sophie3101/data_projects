from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.connection import Connection

def check_aws_connection(conn_id):
    """Check AWS connection
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
        # print(f"conn uri {conn_uri}")
        print("S3 connection is successful")
       
    except Exception as e:
        print(f"AWS connection failed: {e}")
        raise

def check_s3_prefix_existence(s3_hook,bucket_name, prefix, delimiter='/'):
    return s3_hook.check_for_prefix(prefix, delimiter, bucket_name)

def upload_file(s3_hook, s3_bucket_name, prefix, local_file_path):
    s3_hook.load_file(
        filename=local_file_path,
        key=prefix,
        bucket_name=s3_bucket_name,
        replace=True
    )
    print(f"File '{local_file_path}' uploaded to s3://{s3_bucket_name}/{prefix}")
