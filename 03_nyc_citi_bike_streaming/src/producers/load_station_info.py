import requests
from producer import kalkaCustomized

URL="https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
TOPIC_NAME = "station_info"
def get_data(url):
    res = requests.get(url)
    return res.json()


if __name__ == "__main__":
    kafka = kalkaCustomized(server="redpanda-1:29092")
    kafka.list_topic()
    kafka.create_topic(TOPIC_NAME)
    
    data = get_data(URL)
    for item in data['data']['stations']:
        message = {
            "station_id": item["station_id"],
            "name": item["name"],
            "longitude": item["lon"],
            "latitude": item["lat"],
            "capacity": item["capacity"]
        }
        kafka.send_message(TOPIC_NAME, message)

    kafka.close_producer()