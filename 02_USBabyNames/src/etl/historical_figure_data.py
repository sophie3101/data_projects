import requests, re, sys, time
from utils.logger import get_logger 

logger = get_logger(__name__)

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

def get_all_historical_figure_data(names, api_key):
  rows = []
  for idx,name in enumerate(names):
    name= name.lower()
    logger.debug(idx,name)
    api_url =  f"https://api.api-ninjas.com/v1/historicalfigures?name={name}"
    cur_rows  = get_one_historical_figure_data(name, api_url, headers={'X-Api-Key': api_key})
    rows.extend(cur_rows)
    time.sleep(1)
  
  return rows 

def get_one_historical_figure_data( input_name, url, headers):
  rows = []
  try:
    res = requests.get( url,headers=headers)
    if res.status_code == 200:
      data  = res.json()
      for item in data:
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
          
          rows.append({
                  "Name": input_name, 
                  "FullName":name,
                  "Title":title, 
                  "Born": birth_date, 
                  "Died":date_of_death, 
                  "Nationality":nationality, 
                  "Occupation":occupation_data
                })
  except Exception as e:
    logger.error("Error in fetching data ",e)
  return rows 

