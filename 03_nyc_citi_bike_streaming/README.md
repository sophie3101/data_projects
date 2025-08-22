# NYC Citi Bike Streaming Pipeline
This project sets up a real-time data pipeline to ingest, process, and visualize NYC Citi Bike streaing data

## Stacks

- Redpanda (Kafka-compatible streaming platform)
- Apache Spark Structured Streaming
- PostgreSQL
- Grafana

## Workflow Summary
          ----------------------
            NYC Citi Bike Feed 
          ----------+-----------
                     |
                     v
          ----------------------
               Redpanda (Kafka)
          ----------+-----------
                     |
                     v
       ----------------------------
         Spark Structured Streaming
        (reads from Redpanda topic)
       ------------+---------------
                     |
                     v
          ----------------------
              PostgreSQL tables     
          ----------+----------
                     |
                     v
          ----------------------
                Grafana       
              (dashboard)
          ----------------------

## Installation & Deployment
git clone 
1. `docker compose up --build --remove-orphans  -d `
This will spin up:
- Redpanda broker
- an env to run python
- Spark master and worker
- PostgresSQL server
- pgAdmin
- Grafana

2. to send streaming data using redpanda:
docker exec -it python-client python src/producers/load_station_info.py
docker exec -it python-client python src/producers/load_station_status.py

3. after that, use Spark to consume message and save the streaming data to postgres tables
docker run --rm -it  -e SPARK_MASTER="spark://spark-master:7077"  -v $PWD/src/spark:/app --network 03_nyc_citi_bike_streaming_default -p 4040:4040 docker.io/bitnami/spark /bin/bash
after going into the container

check if the master host can be connected: timeout 1 bash -c "echo > /dev/tcp/spark-master/7077" && echo "Port is open" || echo "Port is closed"