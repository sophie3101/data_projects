from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, DoubleType, StringType, IntegerType, LongType
from pyspark.sql.functions import from_json, col
import sys 
def write_to_postgres(df, epoch_id, table="station_status", username="postgres", password="postgres"):
    df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://postgres:5432/postgres") \
        .option("dbtable", table) \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver")\
        .save()
    print("data loaded")

spark = SparkSession.builder \
    .appName("station_info_stream") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR") #suppress all the INFO logs
topic_name = sys.argv[1]
# print(topic_name)

schema = StructType() \
    .add("station_id", StringType()) \
    .add("num_docks_available", IntegerType()) \
    .add("num_bikes_disabled", IntegerType()) \
    .add("num_ebikes_available",  IntegerType()) \
    .add("is_renting", IntegerType()) \
    .add("num_docks_disabled", IntegerType()) \
    .add("num_bikes_available", IntegerType()) \
    .add("is_returning", IntegerType()) 
    
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-1:29092") \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), schema).alias("data")) \
#     .select("data.*") ## Select all fields from the parsed 'data' struct
json_df = df.select(
    col("value").cast("string").alias("json_str"),
    col("timestamp").alias("kafka_timestamp")
)

# 4. Parse JSON and attach the timestamp
parsed_df = json_df.select(
    from_json(col("json_str"), schema).alias("data"),
    col("kafka_timestamp").cast("timestamp").alias("event_time")
).select("data.*", "event_time")
# # print to console first 10 rows
# query = parsed_df.writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .option("numRows", 100) \
#     .start()
# query.awaitTermination()

# write to postgres
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/station_status_stream") \
    .start()

query.awaitTermination()