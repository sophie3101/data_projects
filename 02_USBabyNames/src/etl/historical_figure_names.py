from utils.logger import get_logger
import re, os
import aiohttp
import asyncio
from aiolimiter import AsyncLimiter
from utils.postgresql import init_db, create_table, check_name_exists, insert_name, update_status, table_to_file
from dotenv import load_dotenv

logger = get_logger(__name__)
MAX_CONCURRENT_REQUESTS=5
RETRIES_NUM=3
TABLE="historical_figures"
COLUMNS= ['name', 'full_name', 'title', 'dob', 'dod', 'nationality','status']

def get_date(date_string):
  if date_string is None or date_string=='N/A':
    return
  patterns = [r'\d{1,2} \w+ \d{4}', r'\b\w+ \d{1,2}, \d{4}\b'] #December 1, 1913 or 19 September 1964
  found=False
  for pattern in patterns:
    match = re.search(pattern, date_string)
    if match:
      return match.group(0)
  
  if not found:
    logger.error(f"Cannot find date  {date_string}")
    return None
  
async def make_one_request(async_session, input_name, url, custom_headers, semaphore, db_connection):
  """an API request yields multiple figures. For each figure, insert to the table and set status='not filled' initially
  After all the figures belong to a name are processed successfully, update the status to 'filled'
  """
  async with semaphore:
    try:
      logger.debug(f"Requesting {url}")
      async with async_session.get(url, headers = custom_headers) as response:
        response.raise_for_status()
        content = await response.json()
        for item in content:
          full_name = item.get("name")
          if full_name.split()[0].lower().strip()!=input_name:
            continue
          title = item.get("title")
          info=item.get("info")
          nationality = info.get("nationality", None)
          # occupation = info.get("occupation")  or  info.get("occupations") 
          # if type(occupation) is list:
          #   occupation_data=', '.join(occupation)
          # else:
          #   occupation_data =  occupation
          born=info.get("born")
          died=info.get("died", None)
          birth_date = get_date(born)
          date_of_death = get_date(died)
          if birth_date is None or date_of_death is None: #don't add the historical if the birth_date or date of death is not lited
            continue
          """insert to table"""
          insert_name(db_connection, TABLE, COLUMNS, ( input_name, full_name, title, birth_date, date_of_death, nationality,  "not filled"))
        update_status(db_connection, TABLE, input_name)
    except Exception as e:
      logger.error(f"Error {e}")
    

async def make_all_requests(name_list, api_key, db_connection):
  semaphore=asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
  headers={'X-Api-Key': api_key}

  async with aiohttp.ClientSession() as session:
    tasks=[]
    for idx,name in enumerate(name_list):
      logger.debug(f"{idx} {name}")
      name=name.strip().lower()
      """check is name exists in the database"""
      if not check_name_exists(db_connection, name, TABLE): 
        api_url =  f"https://api.api-ninjas.com/v1/historicalfigures?name={name}"
        task = asyncio.create_task(
          make_one_request(session, name, api_url, headers, semaphore, db_connection)
        )
        tasks.append(task)


    await asyncio.gather(*tasks)

def main(name_list, output_file):
  load_dotenv()
  ninja_api_key = os.getenv("NINJA_API")

  user = os.getenv("DB_USER")
  password = os.getenv("DB_PASS")
  host = os.getenv("DB_HOST")
  port = os.getenv("DB_PORT")
  db = os.getenv("DB_NAME")
  conn = init_db(db, user, password, host, port)

  # create_table_sql = '''CREATE TABLE {}(
  #     id SERIAL ,
  #     name VARCHAR(255) NOT NULL,
  #     full_name VARCHAR(255) NOT NULL,
  #     title VARCHAR(255) ,
  #     dob VARCHAR(255) ,
  #     dod VARCHAR(255),
  #     nationality VARCHAR(255) ,
  #     occupation  VARCHAR(255) ,
  #     status VARCHAR(10) NOT NULL,
  #     PRIMARY KEY (name, full_name)
  # )'''.format(TABLE)
  create_table_sql = '''CREATE TABLE {}(
      id SERIAL ,
      name VARCHAR(255) NOT NULL,
      full_name VARCHAR(255) NOT NULL,
      title VARCHAR(255) ,
      dob VARCHAR(255) ,
      dod VARCHAR(255),
      nationality VARCHAR(255) ,
      status VARCHAR(10) NOT NULL,
      PRIMARY KEY (name, full_name)
  )'''.format(TABLE)
  if conn is not None:
    create_table(conn, TABLE, create_table_sql)
    asyncio.run(make_all_requests(name_list, ninja_api_key, conn))

    """after all the requests are finished export to csv"""
    logger.info(f"Writing to {os.path.basename(output_file)}")
    table_to_file(conn, TABLE, output_file )