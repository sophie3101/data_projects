import requests, zipfile, io, os
from pathlib import Path 
import pandas as pd
from utils.logger import get_logger 

logger = get_logger(__name__)

def download_file_from_link(url, des_folder):
  """
    This function is to download zip folder from link {url} to {des_folder}
    and the compressed link is extracted to {des_fodler}
    Parameters:
      url: name of link to download
      des_folder: name of destination folder where the downloaded link is located
    Returns:
      True if the downloading and extracting files are successful
  """
  try:
    headers = {
    'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    response = requests.get(url, stream=True, headers=headers)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
      zip_file.extractall(des_folder)
      logger.info("ZIP file downloaded and extracted successfully.")
      return True
  except:
    logger.error(f"Error in downloading the link {url}")
    return False

def to_one_datafile(in_dir, out_file, columns_name, add_year=False):
  """
    This function is to merge all the files in {in_dir} to one file {out_file}
    Parameters:
      in_dir: path of input folders
      out_file: path of output file
    Returns:
      the dataframe
  """
  all_lines=[]
  for f in Path(in_dir).iterdir():
    if f.name.endswith(".pdf"):
      continue
    
    if add_year:
      year = f.name.replace('.txt','').replace('yob','')
      with open(f, 'r',  encoding='latin-1') as fh:
        for line in fh.readlines():
          new_line=[year] + line.strip().split(',')
          all_lines.append(new_line)

    else:
      with open(f, 'r',  encoding='latin-1') as fh:
        for line in fh.readlines():
          all_lines.append(line.strip().split(","))

  df = pd.DataFrame(all_lines, columns=columns_name)
  df.to_csv(out_file, index=False)
  return df
