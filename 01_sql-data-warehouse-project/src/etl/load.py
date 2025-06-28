from dotenv import load_dotenv
import pandas as pd
import os 
from utils.database_utils import check_database, connect_to_database, close_db
from utils.logger import get_logger
logger = get_logger(__name__)
def load_dataset(config):

  postgressql_config = config["postgresql"]
  load_dotenv(dotenv_path=os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env")))
  user = postgressql_config['username']
  password = os.getenv("DB_PASS")
  host = postgressql_config["host"]
  port = postgressql_config["port"]
  db = postgressql_config["database_name"]
  ddl_file = postgressql_config['ddl']

  
  with open(ddl_file, 'r') as fh:
    ddl_query = fh.read()

  if check_database(db, user, password, host, port):
    conn,cursor = connect_to_database(db, user, password, host, port)
    cursor.execute(ddl_query)

    for child_key, child_values in config['processed_dataset']['child_items'].items():
      for file_name in child_values:
        file_path = os.path.join(config['processed_dataset']['dest_folder'], child_key, file_name)
        table_name = file_name.replace(".csv", "")

        with open(file_path, 'r') as f:
          cursor.copy_expert(f"COPY {table_name} \
                              FROM STDIN \
                              WITH CSV HEADER DELIMITER ',' ;", 
                          f)
    close_db(conn, cursor)
 
if __name__=="__main__":
  load_dataset()