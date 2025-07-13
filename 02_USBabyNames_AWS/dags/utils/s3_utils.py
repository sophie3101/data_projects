
from dags.utils.get_logger import get_logger

logger = get_logger(__name__)
def check_s3_file_exists(bucket_name, key, s3_hook):
    if s3_hook.check_for_key(key=key, bucket_name=bucket_name):
        # logger.info(f"File '{key}' found in bucket '{bucket_name}'.")
        return True
    else:
        logger.info(f"File '{key}' not found in bucket '{bucket_name}'.")
        return False
    
def check_bucket_exists(bucket_name, s3_hook):
    if s3_hook.check_for_bucket(bucket_name):
        logger.info(f"Bucket '{bucket_name}' exists.")
        return True
    else:
        logger.error(f" Bucket '{bucket_name}' does NOT exist!")
        return False