import requests, os
from urllib.parse import urljoin
from utils.logger import get_logger

logger = get_logger(__name__)
def download_one_file(url, dest_file_path):
  respond = requests.get(url, stream=True)
  try:
    logger.debug(f"Download {url} to {dest_file_path}")
    with open(dest_file_path, 'wb') as fh:
      fh.write(respond.content)

  except Exception as e:
    logger.error("Error ", e)

def download_raw_files(dataset_dict):
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
        download_one_file(link, file_path)
        
      
if __name__=='__main__':
  download_raw_files()