import csv
import json
from kafka import KafkaProducer
import pandas as pd 
def serialize_row(row):
    # Convert row to dict and ensure all datetime-like values are strings
    clean_row = {}
    for key, value in row.items():
        if pd.isna(value):
            clean_row[key] = None
        elif isinstance(value, pd.Timestamp):
            clean_row[key] = value.isoformat()
        elif isinstance(value, pd.Timedelta):
            clean_row[key] = str(value)
        else:
            clean_row[key] = value
    return clean_row
def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='redpanda-1:29092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    csv_file = '../data/green_tripdata_2025-02.parquet'  # change to your CSV file path if needed

    df = pd.read_parquet(csv_file)
    for i, row in df.iterrows():
        row_=serialize_row(row)
        # print(row_)
        producer.send('green-data', value=row_)
    # with open(csv_file, 'r', newline='', encoding='utf-8') as file:
    #     reader = csv.DictReader(file)

    #     for row in reader:
    #         # Each row will be a dictionary keyed by the CSV headers
    #         # Send data to Kafka topic "green-data"
    #         producer.send('green-data', value=row)

    # # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()