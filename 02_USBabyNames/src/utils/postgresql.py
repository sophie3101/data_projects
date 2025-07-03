from utils.logger import get_logger
import psycopg2 
from psycopg2 import sql

logger = get_logger(__name__)

def init_db(database_name, username, password, host, port):
  """
    This function is to initialize a connection to the database. 
    It sets up and returns a connection to a specified database using the
    provided credentials and connection parameters.

    Parameters:
        database_name (str): The name of the database to connect to.
        username (str): Username for authenticating with the database.
        password (str): Password for authenticating with the database.
        host (str): Host address of the database server.
        port (int or str): Port number on which the database server is listening.
    Returns:
        db_connection (object): A database connection object.
    Raises:
        Exception: If the connection to the database fails.
    """

  try:
    conn = psycopg2.connect(
      dbname="postgres", #default database
      user = username,
      password = password,
      host = host,
      port = port
    )
    conn.autocommit = True 
    with conn.cursor() as cur:
      cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{database_name}'")
      res = cur.fetchone()
      if res is None: # database has not been created:
        query = sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
        cur.execute(query)
        logger.info(f"Database '{database_name}' created successfully!")
    conn.close()

    conn = psycopg2.connect(
      dbname=database_name,
      user = username,
      password = password,
      host = host,
      port = port
    )
    conn.autocommit = True
    logger.info(f"Successfully connected to the PostgreSQL database {database_name}!")
    return conn

  except Exception as e:
    logger.error(e)
    return None

def create_table(db_connection, table_name, sql_query):
  """
    This function is to create a table in the database if it does not already exist.
    Parameters:
        db_connection (object): A valid database connection or cursor object.
        table_name (str): The name of the table to be created (used for logging/debugging).
        sql_query (str): The SQL CREATE TABLE statement.
    Returns:
       True if table exists or created
    Raises:
        Exception: If the SQL execution fails.
  """
  try: 
    cursor=db_connection.cursor()
    cursor.execute(f"SELECT 1 FROM information_schema.tables WHERE table_name='{table_name}'")
    if cursor.fetchone() is None:
      logger.info(f"CREATE TABLE {table_name}")
      cursor.execute(sql_query)
      logger.info(f"Table {table_name} is created")
    else:
      logger.info(f"Table {table_name} already exists")
    return True
  except Exception as e:
    logger.error(e)

def check_name_exists(db_connection, person_name, table_name):
    """
    This function is to check if the input {person_name} exists in the table {table_name}
    Parameters:
        db_connection (object): A valid database connection or cursor object.
        table_name (str): The name of the table to be created (used for logging/debugging).
        person_name (str) : name of a person to check
    Returns:
       bool: True if {person_name} exists otherwise False
    Raises:
        Exception: If the SQL execution fails.
  """
    # logger.debug(f"CHECKING IF {person_name} in {table_name} exists")
    try: 
      cursor=db_connection.cursor()
      query = sql.SQL("SELECT 1 FROM {} WHERE name=%s AND status='filled'").format(sql.Identifier(table_name))
      cursor.execute(query, (person_name,)) #cursor.execute() expects the second argument to be a sequence
      if cursor.fetchone() is None:
        query = sql.SQL("DELETE FROM {table} WHERE name=%s").format(table=sql.Identifier(table_name))
        cursor.execute(query, (person_name,))
        logger.debug(f"DELETE {person_name} from table {table_name}")
        return False 
      else:
        logger.debug(f"{person_name} exists")
        return True
    except Exception as e:
      logger.error(e)

def check_namemeaning_exists(db_connection, person_name, table_name):
    """
    This fucntion is to check whether a name meaning entry exists in the given table.
    Parameters:
        db_connection (object): A valid database connection or cursor object.
        person_name (str): The name to check for existence in the table.
        table_name (str): The name of the table to search in.
    Returns:
        bool: True if the name exists in the table, False otherwise.
    Raises:
        Exception: If the query execution fails.
    """
    # logger.debug(f"CHECKING IF {person_name} in {table_name} exists")
    try: 
      cursor=db_connection.cursor()
      query = sql.SQL("SELECT 1 FROM {} WHERE name=%s").format(sql.Identifier(table_name))
      cursor.execute(query, (person_name,)) #cursor.execute() expects the second argument to be a sequence
      if cursor.fetchone() is None:
        return False
      return True
    except Exception as e:
      logger.error(e)

def insert_name(db_connection, table_name, column_names, values):
  """
    Insert a record into the specified table.
    Parameters:
        db_connection (object): A valid database connection or cursor object.
        table_name (str): The name of the table to insert the record into.
        column_names (list of str): A list of column names to insert values into.
        values (tuple): A tuple of values corresponding to the column names.
    Returns:
        bool: True if insertion is successful
    Raises:
        Exception: If the insertion fails.
    """
  try: 
    cursor=db_connection.cursor()
    query = sql.SQL("INSERT INTO {table_name} ({column_names}) VALUES ({value_placeholders})")\
                .format(table_name=sql.Identifier(table_name),
                        column_names=sql.SQL(', ').join(map(sql.Identifier, column_names)),
                        value_placeholders=sql.SQL(', ').join([sql.Placeholder()] * len(column_names))
                ) #PlaceHolder create a single placeholder for each columns
    cursor.execute(query, (values))
    return True

  except Exception as e:
    logger.error(e)
  
def update_status(db_connection, table_name, name):
  """
    Update a record into the specified table.
    Parameters:
        db_connection (object): A valid database connection or cursor object.
        table_name (str): The name of the table to insert the record into.
        column_names (list of str): A list of column names to insert values into.
        values (tuple): A tuple of values corresponding to the column names.
    Returns:
        bool: True if updation is successful
    Raises:
        Exception: If the updation fails.
    """
  try: 
    cursor=db_connection.cursor()
    query = sql.SQL("UPDATE {table} SET status='filled' WHERE name=%s").format(table=sql.Identifier(table_name))
               
    cursor.execute(query,(name,))
    return True
  except Exception as e:
    logger.error(e)

def table_to_file(db_connection, table_name, output_file):
  """
    Export the contents of a database table to a CSV file.
    Parameters:
      db_connection (object): A valid database connection or cursor object.
      table_name (str): The name of the table to export.
      output_file (str): The path to the output CSV file.
    Returns:
      bool: True if the export is successful
    Raises:
      Exception: If reading from the database or writing to the file fails.
  """
  try: 
    cursor=db_connection.cursor()
    query = sql.SQL("COPY (SELECT * FROM {table} ORDER BY id) TO STDOUT WITH CSV HEADER").format(table=sql.Identifier(table_name))
    with open(output_file, 'w', encoding='utf-8') as out_fh:         
      cursor.copy_expert(query, out_fh)
    return True
  except Exception as e:
    logger.error(e)

def file_to_table(db_connection, table_name, input_file):
  """
  Import CSV file into database
  Parameters:
    db_connection (object): A valid database connection or cursor object.
    table_name (str): The name of the table to import.
    iput_file (str): The path to the input CSV file.
  Returns:
    bool: True if the import is successful
  Raises:
    Exception: If reading from the database or writing to the file fails.
  """
  try:
    cursor = db_connection.cursor()
    with open(input_file, 'r') as fh:
      query = sql.SQL("COPY {table} FROM STDIN WITH CSV HEADER DELIMITER ',' ;").format(table=sql.Identifier(table_name))
      cursor.copy_expert(query, fh)
    return True
  except Exception as e:
    logger.error(e)