import requests, re
import xml.etree.ElementTree as ET
import xmltodict
def fetch_zip_files(url):
    try:
        zip_names=[]
        response = requests.get(url)
        xml_data = response.content 
        data = xmltodict.parse(xml_data)
        for item in data['ListBucketResult']['Contents']:
            file_name = item['Key']
            if ".zip" not in file_name or "JC-" in file_name: #JC is for new jerseu
                continue
            
            year,month=get_timeline(file_name)
            file_name = f"{url}/{file_name}"
            zip_names.append((file_name, year, month))
        return zip_names
    except Exception as e:
        print(e)
        raise

def get_timeline(file_name):
    pattern = r"^(JC-)?(\d{4})(\d{2})?[-\s]cit(i)?bike-tripdata(\.csv)?\.zip$"
    match = re.match(pattern, file_name)
    if match:
        groups = match.groups()
        year,month=groups[1], groups[2]
        # print(year, month)
        return year,month 
    else:
        raise ValueError("Unable to parse year (and month) from file: ", file_name)
if __name__=="__main__":
    links=fetch_zip_files("https://s3.amazonaws.com/tripdata")
    print(links)
   
    # get_timeline("JC-202408-citibike-tripdata.csv.zip")
    # get_timeline("202506-citibike-tripdata.zip")
    # get_timeline("2013-citibike-tripdata.zip")
    # get_timeline("JC-201708 citibike-tripdata.csv.zip")