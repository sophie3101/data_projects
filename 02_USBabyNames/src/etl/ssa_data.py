import requests, zipfile, io, os
from pathlib import Path 
import pandas as pd
from utils.logger import get_logger 

logger = get_logger(__name__)

def download_file_from_link(url, des_folder):
  try:
    headers = {
    'User-Agent':"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
    response = requests.get(url, stream=True, headers=headers)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
      zip_file.extractall(des_folder)
      logger.info("ZIP file downloaded and extracted successfully.")

  except:
    logger.error(f"Error in downloading the link {url}")

def to_one_datafile(in_dir, out_file, columns_name, add_year=False):
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

def main():

  output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/processed_data/babynames_by_year.csv"))
  to_one_datafile(os.path.join(os.path.dirname(__file__), "../data/raw_data/ssn_all_names"), output_file, ['Year', 'Name', 'Sex', 'Occurences'], add_year=True)

  output_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/processed_data/names_by_state.csv"))
  to_one_datafile(os.path.join(os.path.dirname(__file__), "../data/raw_data/ssn_by_states"), output_file, ['State', 'Sex','Year', 'Name', 'Occurences'])

if __name__ == "__main__":
  main()