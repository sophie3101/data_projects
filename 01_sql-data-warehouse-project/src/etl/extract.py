import requests, os
from urllib.parse import urljoin
from src.utils.logger import get_logger

logger = get_logger(__name__)
def download_one_file(url, dest_file_path):
  """
    Downloads a single file from a given URL to a specified destination path.
    Parameters:
      url (str): The URL of the file to download.
      dest_file_path (str): The absolute path where the file should be saved.
  """
  respond = requests.get(url, stream=True)
  try:
    logger.debug(f"Download {url} to {dest_file_path}")
    with open(dest_file_path, 'wb') as fh:
      fh.write(respond.content)

  except Exception as e:
    logger.error("Error ", e)
    raise

def download_raw_files(dataset_dict):
  """
    Downloads raw data files based on the provided configuration.
    Args:
        dataset_dict (dict): Configuration dictionary for raw datasets,
                            expected to have 'src_link', 'child_items', and 'dest_folder'.
  """
  logger.info("Beging to download")
  src_link = dataset_dict["src_link"]
  dest_folder = dataset_dict["dest_folder"]
  child_items = dataset_dict["child_items"]
  for child_key, child_values in child_items.items():
    sub_folder = os.path.join(dest_folder, child_key)
    os.makedirs(sub_folder,exist_ok=True)
    for file_name in child_values:
      link = urljoin(src_link, f"{child_key}/{file_name}")
      file_path = os.path.join(sub_folder,file_name)
      if not os.path.exists(file_path):
        try:
          download_one_file(link, file_path)
        except Exception as e:
          logger.error(f"Failed to download {file_name}. Error: {e}")
      
if __name__=='__main__':
  download_raw_files()