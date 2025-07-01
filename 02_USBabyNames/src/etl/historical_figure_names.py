from utils.logger import get_logger
import re
import aiohttp
import asyncio

logger = get_logger(__name__)
MAX_CONCURRENT_REQUESTS=30

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
    logger.error("Cannot find date ", date_string)
    return None
  
async def make_one_request(async_session, input_name, url, custom_headers, semaphore):
  async with semaphore:
    try:
      logger.debug(f"Requesting {url}")
      async with async_session.get(url, headers = custom_headers) as response:
        response.raise_for_status()
        content = await response.json()
        for item in content:
          name = item.get("name")
          if name.split()[0].lower()!=input_name:
            continue
          title = item.get("title")
          info=item.get("info")
          nationality = info.get("nationality", None)
          occupation = info.get("occupation")  or  info.get("occupations") 

          if type(occupation) is list:
            occupation_data=', '.join(occupation)
          else:
            occupation_data =  occupation
          born=info.get("born")
          died=info.get("died", None)
          birth_date = get_date(born)
          date_of_death = get_date(died)
          
          # rows.append({
          #         "Name": input_name, 
          #         "FullName":name,
          #         "Title":title, 
          #         "Born": birth_date, 
          #         "Died":date_of_death, 
          #         "Nationality":nationality, 
          #         "Occupation":occupation_data
          #       })
    except Exception as e:
      logger.error(f"Error {e}")
    

async def make_all_requests(name_list, api_key):
  semaphore=asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
  headers={'X-Api-Key': api_key}

  async with aiohttp.ClientSession() as session:
    tasks=[]
    for name in name_list:
      api_url =  f"https://api.api-ninjas.com/v1/historicalfigures?name={name}"
      task = asyncio.create_task(
        make_one_request(session, name, api_url, headers, semaphore)
      )
      tasks.append(task)


    await asyncio.gather(*tasks)


def get_all_historical_figure_names(names_list, api_key):
  asyncio.run(make_all_requests(names_list, api_key))
