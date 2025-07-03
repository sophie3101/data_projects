from utils.logger import get_logger
import re, os
import aiohttp
import asyncio
from utils.postgresql import init_db, create_table, check_name_exists, insert_name, update_status, table_to_file
from dotenv import load_dotenv

logger = get_logger(__name__)
MAX_CONCURRENT_REQUESTS=5
RETRIES_NUM=3
TABLE="historical_figures"
COLUMNS= ['name', 'full_name', 'title', 'dob', 'dod', 'nationality','status']

def get_date(date_string):
  """
    This function is to extract date from the {date_string}
    e.g data_string is 15 March 44 BC Rome, Italy, return 15 March 44
    Parameters:
      date_string: text 
    Returns:
      string of the date or None if no date is found in {date_string}
  """
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
  """
    This function is to make an asynchronous API request to the given URL and process the response.

    The response is expected to contain multiple items. Each item is inserted into the database
    with a status of 'not filled'. After all items are saved, the last item's status is updated
    to 'filled'.
    Parameters:
      async_session (aiohttp.ClientSession): An active aiohttp session for making requests.
      input_name: A string identifier used for logging or database reference.
      url: The API endpoint to send the request to.
      custom_headers: Custom headers to be used in the HTTP request.
      semaphore: Semaphore to limit the number of concurrent requests.
      db_connection: A database connection or cursor object for executing queries.
    Returns:
      True if successfull otherwise throw an error
  """
  skip=False
  for i in range(RETRIES_NUM):
    async with semaphore:
      try:
        logger.debug(f"Requesting {url}, retry_time: {i+1}")
        async with async_session.get(url, headers = custom_headers) as response:
          if response.status != 200:
            retry_after = int(response.headers.get("Retry-After", "0"))
            await asyncio.sleep(max(retry_after, 5)) 
            continue # go to next RETRY TIME

          response.raise_for_status()
          content = await response.json()
          for item in content:
            full_name = item.get("name")
            if input_name not in full_name.lower():
              logger.debug(f"{full_name} not match {input_name}")
              skip=True
              continue
            title = item.get("title")
            info=item.get("info")
            nationality = info.get("nationality", None)
            born=info.get("born")
            died=info.get("died", None)
            birth_date = get_date(born)
            date_of_death = get_date(died)
            if birth_date is None or date_of_death is None: #don't add the historical if the birth_date or date of death is not lited
              skip=True
              continue
            """insert to table"""
            insert_name(db_connection, TABLE, COLUMNS, ( input_name, full_name, title, birth_date, date_of_death, nationality,  "not filled"))
          #update when all historical names of a name are inserted into the table
          update_status(db_connection, TABLE, input_name)
          if not skip:
            logger.debug(f"{input_name} is finished")
          return #exist the loop if successful
      except Exception as e:
        logger.error(f"Error {e}")
    

async def make_all_requests(name_list, api_key, db_connection):
  """ This function is to submit asynchronous API requests for a list of names.
    For each name in the list, it builds the corresponding URL and headers, then delegates
    the actual request handling to `make_one_request`. All requests are submitted concurrently.

    Parameters:
        name_list : List of names for which API requests should be sent.
        api_key: API key used in the authorization headers for each request.
        db_connection: A database connection or cursor object for executing queries.
    Returns:
        None
    Raises:
        Exception: If any individual request fails.
    """
  semaphore=asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
  headers={'X-Api-Key': api_key}

  async with aiohttp.ClientSession() as session:
    tasks=[]
    for idx,name in enumerate(name_list):
      # logger.debug(f"{idx} {name}")
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
    asyncio.run(make_all_requests(name_list, ninja_api_key, conn)) #creates a new event loop, runs the coroutine, and closes the loop after it’s done

    """after all the requests are finished export to csv"""
    logger.info(f"Writing to {os.path.basename(output_file)}")
    table_to_file(conn, TABLE, output_file )