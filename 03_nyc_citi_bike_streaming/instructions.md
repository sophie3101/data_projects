- build docker images

docker compose up --build --remove-orphans  -d

- shut down images:
docker compose down --remove-orphans

- 
<!-- - create topic `station_name`:
docker exec -it  redpanda-1 rpk topic create station_info
docker exec -it  redpanda-1 rpk topic create station_status -->

create and send send message 
docker exec -it python-client python src/producers/load_station_info.py
docker exec -it python-client python src/producers/load_station_status.py

4. SPARK STREAM to consume message from redpanda, write stream to postgres
docker run --rm -it  -e SPARK_MASTER="spark://spark-master:7077"  -v $PWD/src/spark:/app --network 03_nyc_citi_bike_streaming_default -p 4040:4040 docker.io/bitnami/spark /bin/bash

after going into the container

check if the master host can be connected: timeout 1 bash -c "echo > /dev/tcp/spark-master/7077" && echo "Port is open" || echo "Port is closed"

- station info

    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
    --master spark://spark-master:7077 \
    --jars /app/postgresql-42.7.6.jar \
    /app/station_info_spark.py station_info

- station_status
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
--master spark://spark-master:7077 \
--jars /app/postgresql-42.7.6.jar \
/app/station_status_spark.py station_status

5. grafana dashboard
go to localhost:3000
connect to data source, choose postgres. remember to disable SSL. Enter postgres for everything


WITH cte AS (
SELECT station_id, num_bikes_available,num_docks_available,
ROW_NUMBER()OVER(PARTITION BY station_id ORDER BY event_time DESC ) AS rnk
FROM station_status
)

#totaol dock available
SELECT SUM(num_docks_available)
FROM cte
JOIN station_info 
	ON station_info.station_id = cte.station_id 
WHERE cte.rnk =1
# total bik available
SELECT station_info.name, station_info.longitude, station_info.latitude, cte.num_bikes_available
FROM cte
JOIN station_info 
	ON station_info.station_id = cte.station_id 
WHERE cte.rnk =1
