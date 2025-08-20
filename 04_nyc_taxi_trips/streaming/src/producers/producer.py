import json, time
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# server = 'localhost:9092'
server = 'redpanda-1:29092'
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)
t0 = time.time()

topic_name = 'test-topic'

for i in range(10, 1000):
    message = {'test_data': i, 'event_timestamp': time.time() * 1000}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

producer.flush()
producer.close()
t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')