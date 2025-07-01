from utils.logger import get_logger
from utils.sqlite import *
import os 
logger = get_logger(__name__)

def get_historical_figure_names(name_list):
  print(os.path.dirname(__file__))
  try:
    conn, cursor = init_db("names.db")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    print("Tables in database:")
    for table in tables:
        print(table[0])

    for name in name_list:
      logger.debug(name)
  except Exception as e:
    logger.error(e)

def get_historical_figure_names(name_list):
    try:
        print("Running get_historical_figure_names...")
        conn, cursor = init_db("names.db")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print("Tables in database:")
        for table in tables:
            print(table[0])

        for name in name_list:
            logger.debug(name)

        conn.close()
    except Exception as e:
        logger.error(f"Error in get_historical_figure_names: {e}")

if __name__ == "__main__":
    get_historical_figure_names(["Leonardo", "Cleopatra", "Socrates"])
