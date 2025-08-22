import requests, time
from producer import kalkaCustomized

URL= 'https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json'
TOPIC_NAME = "station_status"
INTERVAL=320
def get_data(url):
    res = requests.get(url)
    return res.json()

if __name__ == "__main__":
    kafka = kalkaCustomized(server="redpanda-1:29092")
    kafka.list_topic()
    kafka.create_topic(TOPIC_NAME)
    
    try:
        while True:
            data = get_data(URL)
            for item in data['data']['stations']:
                message = {
                "station_id": item["station_id"],
                    "num_docks_available": item["num_docks_available"],
                    "num_bikes_disabled": item["num_bikes_disabled"],
                    "num_ebikes_available": item["num_ebikes_available"],
                    "is_renting":item["is_renting"],
                    "num_docks_disabled":item["num_docks_disabled"],
                    "num_bikes_available":item["num_bikes_available"],
                    "is_returning":item["is_returning"]
                }
                kafka.send_message(TOPIC_NAME, message)
            time.sleep(INTERVAL)
            
    except KeyboardInterrupt:
        print("Stopping stream...Close stream")
    finally:
        kafka.close_producer()