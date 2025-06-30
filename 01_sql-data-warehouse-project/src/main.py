import json
from etl import extract, load, transform
from utils.logger import get_logger
from synthetic_data import  synthetic_data

def generate_synthetic_data():
  synthetic_data.generate_synthetic_data()
  return
def run_pipeline():
  logger = get_logger(__name__, "etl.log")
  logger.info('Starting ETL process') 
  try:
    with open("./config.json", 'r') as json_fh:
      config = json.load(json_fh)
      """STEP 1: EXTRACT DATA"""
      logger.info("Downloading files")
      extract.download_raw_files(config['raw_dataset'])
      logger.info("Downloading files is finished")

      """STEP 2: TRANSFORM DATA"""
      logger.info('Starting transformation') 
      transform.transform_datasets(**config)
      logger.info('transformation is finished') 

      """STEP 3: LOAD DATA"""
      logger.info('Starting load to database') 
      load.load_dataset(config)
      logger.info('Load completed') 
    
      logger.info('ETL process completed successfully') 
  except Exception as e:
    logger.error("ETL process failed")
    logger.error(e)

if __name__=="__main__":
  run_pipeline()
  # generate_synthetic_data()
