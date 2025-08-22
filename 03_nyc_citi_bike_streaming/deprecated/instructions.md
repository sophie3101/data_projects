1. build flink base image
docker build .

2. build docker images
docker-compose up airflow-init
docker compose up --build --remove-orphans  -d
shut down images:
docker compose down --remove-orphans

3. to run the flink script: 
<!-- docker exec -it  flink-jobmanager ...
e.g
docker exec -it flink-jobmanager python src/producers/load_station.py

how to use sql client flink
docker exec -it flink-jobmanager flink/bin/sql-client.sh -->

- create topic `station_name`:
docker exec -it  redpanda-1 rpk topic create station_info
docker exec -it  redpanda-1 rpk topic create station_status

- send message
docker exec -it python-client python src/producers/load_station_info.py
docker exec -it python-client python src/producers/load_station_status.py
<!-- -use redpanda to stream data:
docker exec -it flink-jobmanager python src/producers/load_station_info.py
docker exec -it flink-jobmanager python src/producers/load_station_status.py -->

<!-- - run job
docker exec -it flink-jobmanager flink/bin/flink run -py src/jobs/station_info_job.py -->
- spark UI
docker run --rm -it  -e SPARK_MASTER="spark://spark-master:7077"  -v $PWD/src/spark:/app --network 03_nyc_citi_bike_streaming_default -p 4040:4040 docker.io/bitnami/spark /bin/bash
after going into the container
check if the master host can be connected: timeout 1 bash -c "echo > /dev/tcp/spark-master/7077" && echo "Port is open" || echo "Port is closed"

<!-- spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
--master spark://spark-master:7077 \
/app/station_info_spark.py station_info -->

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
--master spark://spark-master:7077 \
--jars /app/postgresql-42.7.6.jar \
/app/station_info_spark.py station_info

spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
--master spark://spark-master:7077 \
--jars /app/postgresql-42.7.6.jar \
/app/station_status_spark.py station_status