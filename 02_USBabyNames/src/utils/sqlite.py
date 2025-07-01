import sqlite3,os,traceback
from utils.logger import get_logger

logger = get_logger(__name__)
def init_db(db_file):
  logger.debug(f"Connecting to database {db_file}")
  try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS figures (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT,
          status TEXT)
      ''')

    conn.commit()
  except Exception as e:
    traceback.print_exc()
    logger.error(f"Error during database init: {e}")
    raise

  return conn,cursor

def check_name_exists(conn, cursor, name):
  logger.debug(cursor.execute(f''' SELECT 1 FROM figures WHERE name={name} AND status="queried" ''').fetchone())

def delete_table(conn, cursor, table):
  cursor.execute('''DELETE FROM {table} ''')
  conn.commit()
def insert_name(conn, cursor, *args):
  cursor.execute(''' INSERT INTO names VALUES()''')
  conn.commit()