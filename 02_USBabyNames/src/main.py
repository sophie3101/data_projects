import os, sys
import pandas as pd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) #Python sets the working directory to scripts/, and it doesn't see the etl/ folder at the same level as scripts/
from etl import  ssa_data, historical_figure_names
from utils import logger as Logger



def main():
  logger = Logger.get_logger(__name__)
  """DOWNLOAD NAMES DATA FROM SSN"""
  ssn_urls = ["https://www.ssa.gov/oact/babynames/names.zip", "https://www.ssa.gov/oact/babynames/state/namesbystate.zip"]
  ssn_data_folders = ["ssn_all_names", "ssn_by_states"]
  ssn_output_files = ["names_by_year.csv", "names_by_state.csv"]

  for url, rawdata_output_folder, output_file in zip(ssn_urls, ssn_data_folders,ssn_output_files):
    raw_data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/raw_data/', rawdata_output_folder))
    output_data_filepath =  os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/processed_data/', output_file))
    if not os.path.exists(raw_data_dir ):
      ssa_data.download_file_from_link(url, raw_data_dir)
    if not os.path.exists(output_data_filepath):
      if output_file==ssn_output_files[0]:
        ssa_data.to_one_datafile(raw_data_dir, output_data_filepath, ['Year', 'Name', 'Sex', 'Occurences'], add_year=True)
      else:
        ssa_data.to_one_datafile(raw_data_dir, output_data_filepath,  ['State', 'Sex','Year', 'Name', 'Occurences'])

  """DOWNLOAD NAME ASSOCIATED DATA FROM NINJA NAME API"""
  # get the list of names 
  ssa_df = pd.read_csv(os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/processed_data/', 'names_by_year.csv')))
  names = ssa_df.Name.unique()
  logger.debug(len(names))
  # # getting the top 1000 frequent names 
  avg_occurences = ssa_df.groupby('Name').agg(AvgOccurences=('Occurences', 'mean')).sort_values("AvgOccurences", ascending=False).reset_index()
  top_names = avg_occurences.loc[:100].Name.unique()
  historical_file = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/processed_data/', "historical_figures.csv"))
  historical_figure_names.main(top_names, historical_file)

  """get name meaning"""


if __name__=="__main__":
  main()