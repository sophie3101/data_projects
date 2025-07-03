"""
Functions for using AWS S3
"""
from urllib.parse import urlparse
import boto3
from utils.logger import get_logger

logger = get_logger(__name__)
def init_s3_client(profile_name_):
  """
  Initialize S3 client with AWS CLI profile
  Parameter:
    profile_name_ (str): name of AWS CLI proifle to use for authentication
  returns:
    s3_client
  raise:
    error
  """
  try:
    logger.info(profile_name_)
    session = boto3.Session(profile_name=profile_name_)
    s3_client = session.client('s3')
    # s3_client = session.client('s3', 
    #               aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    #               aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    #             )
    return s3_client
  except Exception as e:
    logger.error(e)
    raise


def get_bucket_n_key_from_s3uri(s3_uri):
  """
  Get bucket_name and prefix from S3_URI
  Parameters:
    s3_uri: S3 path (e.g s3://<bucket>/<something>)
  Return:
    tuple: contain bucket and prefix
        or None if s3_uri is not a valid s3 URI
  """
  parse = urlparse(s3_uri)
  if parse.scheme == 's3':
    bucket_name = parse.netloc
    key = parse.path.lstrip("/")
    return bucket_name, key
  else:
    return None,None

def check_s3_path(s3_client, bucket, key):
  """
    This function is to upload the file to S3_URI
    Parameters:
      s3_client (boto3.client): S3 object
      s3_uri (str): S3 URI path
    Returns:
      bool:True S3 URI exists otherwise False
    Raise:
      error
  """

  try:
    response = s3_client.head_object(Bucket=bucket, Key=key)
    return True
  except Exception as e:
    logger.error(e)
 

def uploadToS3(s3_client, file_name, s3_uri):
  """
  This function is to upload the file to S3_URI
  Parameters:
    s3_client (boto3.client): S3 object
    s3_uri (str): S3 URI path
    file_name (str) : path of local file to upload to S3 URI
  Returns:
    bool:True if file is uploaded to S3 URI
  Raise:
    error
  """
  try:
    bucket, key = get_bucket_n_key_from_s3uri(s3_uri)
    if bucket and key:
      if not check_s3_path(s3_client, bucket, key):
        s3_client.upload_file(file_name, bucket, key)
        logger.info(f"Finished uploading {file_name} to {s3_uri}")
      else:
        logger.info(f"{s3_uri} already exists")
  
  except Exception as e:
    logger.error(e)
    raise