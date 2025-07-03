import os, sys, configparser
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) #Python sets the working directory to scripts/, and it doesn't see the etl/ folder at the same level as scripts/
from etl import  ssa_data, historical_figure_names, name_meaning
from utils.logger import get_logger
from utils.s3_client import *
def main():
  logger = get_logger(__name__,'log.log')
  config = configparser.ConfigParser()
  config.read("config.ini")

  """DOWNLOAD NAMES DATA FROM SSN"""
  raw_data_folder = config.get("DATA_FOLDER", "raw_folder")
  processed_data_folder = config.get("DATA_FOLDER", "processed_folder")

  ssn_urls = config.get("SSN", "urls").split(",")
  ssn_raw_data_folders = config.get("SSN", "output_folders").split(",")
  ssn_output_files = config.get("SSN", "output_files").split(",")

  for url, rawdata_output_folder, output_file in zip(ssn_urls, ssn_raw_data_folders,ssn_output_files):
    raw_data_dir = os.path.abspath(os.path.join(raw_data_folder, rawdata_output_folder))
    output_data_filepath =  os.path.abspath(os.path.join(processed_data_folder, output_file))

    if not os.path.exists(raw_data_dir ):
      ssa_data.download_file_from_link(url, raw_data_dir)

    if not os.path.exists(output_data_filepath):
      if output_file==ssn_output_files[0]:
        ssa_data.to_one_datafile(raw_data_dir, output_data_filepath, ['Year', 'Name', 'Sex', 'Occurences'], add_year=True)
      else:
        ssa_data.to_one_datafile(raw_data_dir, output_data_filepath,  ['State', 'Sex','Year', 'Name', 'Occurences'])

  names_file_path = os.path.abspath(os.path.join(processed_data_folder, config.get("HISTORICAL_FIGURES", "in_file")))
  """Upload file to S3"""
  s3_client = init_s3_client(config.get("S3", "aws_profile"))
  uploadToS3(s3_client, names_file_path, config.get("S3", "s3_uri") )
  

  # """getting the top 1000 frequent names """
  # names_file_path = os.path.abspath(os.path.join(processed_data_folder, config.get("HISTORICAL_FIGURES", "in_file")))
  # ssa_df = pd.read_csv(names_file_path)
  # agg_occurences = ssa_df.groupby('Name').agg(AvgOccurences=('Occurences', 'sum')).sort_values("AvgOccurences", ascending=False).reset_index()
  # top_names = agg_occurences.loc[:1000].Name.unique()

  # """DOWNLOAD NAME ASSOCIATED DATA FROM NINJA NAME API"""
  # historical_file = os.path.abspath(os.path.join(processed_data_folder, config.get("HISTORICAL_FIGURES", "out_file")))
  # historical_figure_names.main(top_names, historical_file)

  # """get name meaning"""
  # name_meaning_file = os.path.abspath(os.path.join(processed_data_folder, config.get("NAMES_MEANING", "out_file")))
  # name_meaning.main(top_names, name_meaning_file)

if __name__=="__main__":
  main()