
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from kafka.admin import KafkaAdminClient, NewTopic
from datetime import datetime, timedelta
import requests
import json
from kafka import KafkaProducer

URL= 'https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json'
TOPIC_NAME = 'station_info'
SERVER="redpanda-1:29092"
def on_success(metadata):
  print(f"Message produced to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

def fetch_and_produce_station_name():
    """Fetch static station name data and produce to Redpanda"""
    try:
        server = 'redpanda-1:29092'
        producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        # print(data)
        for item in data['data']['stations']:
            message = {
                "station_id": item["station_id"],
                "name": item["name"],
                "longitude": item["lon"],
                "latitude": item["lat"],
                "capacity": item["capacity"]
            }
            # print(message)
            producer.send(TOPIC_NAME, message)

        producer.flush()
        producer.close()
    except requests.RequestException as e:
        print(f"Error fetching station name data: {e}")
        raise
    except Exception as e:
        print(f"Error producing station name message: {e}")
        raise

# Define the DAG
@dag(
    dag_id='station_name',
    description='Load station name once to Redpanda',
    start_date=datetime(2025, 8, 20, 14, 56),  # Set to current date/time
    max_active_runs=1,                        # Prevent overlapping runs
    catchup=False                             # No backfill for past runs
)
def station_name_dag():
    
    @task 
    def create_topic():
        admin_client = KafkaAdminClient(bootstrap_servers=SERVER) # Replace
        topic_names = admin_client.list_topics()
        print("topic names listed: ", topic_names)
        if len(topic_names)==0 or TOPIC_NAME not in topic_names:
            new_topic = NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{new_topic.name}' created successfully.")

    fetch_name_task = PythonOperator(
        task_id='fetch_station_name',
        python_callable=fetch_and_produce_station_name,
    )
 
    # Define task dependencies
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    create_topic_task = create_topic()
    start_pipeline >> create_topic_task >> fetch_name_task 

station_name_dag()