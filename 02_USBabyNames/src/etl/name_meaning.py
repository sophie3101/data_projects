from utils.logger import get_logger
from utils.postgresql import *
from bs4 import BeautifulSoup
import aiohttp, asyncio, os, re
from dotenv import load_dotenv

logger = get_logger(__name__)

MAX_CONCURRENT_REQUESTS = 3
RETRIES_NUM = 3
URL = "https://www.behindthename.com/name/"
TABLE = "names_meaning"
COLUMNS = ["name", "meaning"]

async def get_name_meaning(async_session, name, db_connection, semaphore):
    """
        This function is to make a request to fetch the meaning of a given name and store it in the database.
        The request is made to a predefined URL using an aiohttp session. The returned HTML
        content is parsed using BeautifulSoup to extract the meaning of the name. The meaning
        is then inserted in the database.
        Parameters:
        async_session (aiohttp.ClientSession): An active aiohttp session for making requests.
        name: name of interest 
        url: The API endpoint to send the request to.
        semaphore: Semaphore to limit the number of concurrent requests.
        db_connection: A database connection or cursor object for executing queries.
        Returns:
        True if successfull otherwise throw an error
    """
    url = f"{URL}{name}"
    for i in range(RETRIES_NUM):
        async with semaphore:
            try:
                logger.info(f"Fetching: {url} (Attempt {i+1})")
                async with async_session.get(url) as response:
                    if response.status != 200:
                        retry_after = int(response.headers.get("Retry-After", "5"))
                        logger.error(f"Retrying {name} after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    definition_div = soup.find('div', class_='namedef')
                    if not definition_div:
                        logger.warning(f"No meaning found for {name}")
                        return

                    meaning = definition_div.get_text(strip=True)
                    insert_name(db_connection, TABLE, COLUMNS, (name, meaning))

                    return  # success, exit retry loop
            except Exception as e:
                logger.error(f"Error fetching {name}: {e}")

    logger.error(f"Failed to scrape {name} after {RETRIES_NUM} attempts.")

async def handle_multiple_scrapes(db_connection, names):
    """ This function is to submit aasynchronous API requests for a list of names.
    For each name in the list, it builds the corresponding URL and headers, then delegates
    the actual request handling to `get_name_meaning`. All requests are submitted concurrently.

    Parameters:
        name_list : List of names for which API requests should be sent.
        db_connection: A database connection or cursor object for executing queries.
    Returns:
        None
    Raises:
        Exception: If any individual request fails.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for name in names:
            name = name.strip().lower()
            if not check_namemeaning_exists(db_connection, name, TABLE):
                task = asyncio.create_task(
                    get_name_meaning(session, name, db_connection, semaphore)
                )
                tasks.append(task)
            else:
                logger.debug(f"{name} already exists in {TABLE}")
        await asyncio.gather(*tasks)

def main(names, output_file):
    load_dotenv()
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASS")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")
    conn = init_db(db, user, password, host, port)

    create_table_sql = f'''
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            meaning TEXT NOT NULL
        )
    '''
    if conn:
        create_table(conn, TABLE, create_table_sql)
        asyncio.run(handle_multiple_scrapes(conn, names))

        table_to_file(conn, TABLE, output_file)
