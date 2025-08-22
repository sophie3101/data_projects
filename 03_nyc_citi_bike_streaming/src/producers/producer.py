from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import json

class kalkaCustomized:
    def __init__(self, server="redpanda-1:29092"):
        self.server=server
        self.admin_client = KafkaAdminClient(bootstrap_servers=self.server) 
        self.producer = KafkaProducer(
            bootstrap_servers=[server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def list_topic(self):
        topic_names = self.admin_client.list_topics()
        return topic_names 
    
    def create_topic(self, topic_name, num_partition=1, replication_factor=1):
        cur_topic_names = self.list_topic()

        if len(cur_topic_names)==0 or topic_name not in cur_topic_names:
            new_topic = NewTopic(name=topic_name, num_partitions=num_partition, replication_factor=replication_factor)
            self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{new_topic.name}' created successfully.")
        else:
            print(f"Topic '{topic_name}' has been created")

    def on_success(self, metadata):
        print(f"Message produced with the offset: {metadata.offset}")

    def on_error(self, error):
        print(f"An error occurred while publishing the message. {error}")
    
    def send_message(self, topic_name, message):
        print(f"Send message {message} to {topic_name}")
        future = self.producer.send(topic_name, message)
        future.add_callback(self.on_success)
        future.add_errback(self.on_error)

    def close_producer(self):   
        self.producer.flush()
        self.producer.close()