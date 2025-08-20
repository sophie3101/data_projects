redpanda instruction and docker file:
https://www.redpanda.com/blog/python-redpanda-kafka-api-tutorial
https://github.com/redpanda-data-blog/2023-python-gsg/blob/main/docker-compose.yml

to build the services
`docker compose up --build --remove-orphans  -d`ß


to connect to database: `psql -U postgres -h localhost -p 5433`

or use docker-compose exec...

create topic with redpanda:
docker-compose exec -it redpanda-1 bin/rpk topic
or 
`docker exec -it redpanda-1 rpk topic create test-topic` #test-toppic is topic name(user defined)
to run a script 
`docker exec -it flink-jobmanager python ../src/producers/producer.py`

`docker exec -it redpanda-1 rpk topic create green-data`
`docker exec -it flink-jobmanager python ../src/producers/produce_taxi_data.py`

to check if mesasge are sent to topic:
`docker exec -it redpanda-1 rpk topic consume <topic_name>`
# Kafka note
`Kafka topics` are powerful, immutable logs that allow you to store, process, and replay streams of events reliably, while keeping full historical context intact.
`Kafka Connector` is a plugin that helps Kafka automatically move data between Kafka topics and external systems, like:
Databases (e.g., PostgreSQL, MySQL)
Filesystems (e.g., S3, HDFS)

APIs or other services (e.g., Elasticsearch, Redis)
 - Source Connector: It is used to transfer data from an external source to the Kafka topic.
 - Sink Connector: It is used to transfer the data in the Kafka topic to the external source.  

# flink sql
https://www.confluent.io/blog/getting-started-with-apache-flink-sql/
create destination table manually:
CREATE TABLE processed_events (
  test_data INTEGER,
  event_timestamp TIMESTAMP
);

`docker exec -it flink-jobmanager ./bin/flink run -py ../src/producers/start_job.py `
DROP TABLE processed_events_aggregated 
CREATE TABLE processed_events_aggregated (
   event_hour TIMESTAMP(3),
   test_data INTEGER,
   num_hits BIGINT,
   PRIMARY KEY (event_hour, test_data)
);
`docker exec -it flink-jobmanager ./bin/flink run -py ../src/producers/aggregation_job.py `

CREATE OR REPLACE TABLE taxi_events  (
            VendorID INTEGER,
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            store_and_fwd_flag VARCHAR,
            RatecodeID INTEGER ,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE
)
`docker exec -it flink-jobmanager ./bin/flink run -py ../src/producers/taxi_job `
