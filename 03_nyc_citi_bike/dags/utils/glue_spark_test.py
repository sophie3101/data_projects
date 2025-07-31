from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from functools import reduce

spark = SparkSession.builder \
    .appName("ReadAllCSVs") \
    .getOrCreate()
dir = "temp/*.csv"

df=spark.read.csv(dir, header=True, inferSchema=True)
print(df.columns)
print("original size :",df.count())

#rename column for cross consistency
newColumns = []
for column in df.columns:
    column = column.lower()
    column = column.replace(' ', '_')
    if column=='startime' or column=='start_time':
        column='started_at'
    if column=='stoptime' or column=='stop_time':
        column='ended_at'
    if column=='start_station_latitude':
        column='start_lat'
    if column=='start_station_longitude':
        column='start_lng'
    if column=='end_station_latitude':
        column='end_lat'
    if column=='end_station_longitude':
        column='end_lng'
    if column=='user_type':
        column='member_casual'

    newColumns.append(column)
df = df.toDF(*newColumns)

# drop columns for cross consistency
columns_to_drop = ["ride_id","bike_id", "birth_year", "gender"]
for col in columns_to_drop:
    if col in df.columns:
        df=df.drop(col)

print(df.columns)
#make lowercase value in whole column
df=df.withColumn("member_casual", sf.lower(sf.col("member_casual")))

# drop row with na value in selected columns
df=df.dropna(subset=['started_at','ended_at', 'start_station_name', 'start_lat', 'end_lat', 'start_lng', 'end_lng'])
print("size after drop duplicated ride_id and remove na values: ",df.count())

# filter trip that last less than a minute
df = df.withColumn("started_at", sf.to_timestamp(sf.col("started_at"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("ended_at", sf.to_timestamp(sf.col("ended_at"), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("trip_duration", sf.unix_timestamp("ended_at") - sf.unix_timestamp("started_at")) \
       .filter(sf.col("trip_duration") > 60)  # keep trips longer than 1 minute
print("size after filter trip with less than a minute: ", df.count())

# filter trip that start and end at same station id
df=df.filter((sf.col('start_station_id')==sf.col('end_station_id')) & (sf.col('trip_duration')<=600))
print("size after filter trip with start and stop at the same station: " ,df.count())

# for member_casual col, if there is null value , fill null
df = df.withColumn(
    "member_casual",
    sf.when(sf.col("member_casual") == "customer", "casual")
     .when(sf.col("member_casual") == "subscriber", "member")
     .when(sf.col("member_casual").isNull(), "unknown")
     .otherwise(sf.col("member_casual"))
)
# #fill other column with na value as unknown
# null_condition = [sf.col(c).isNull() for c in df.columns]
# df.filter(reduce(lambda a, b: a | b, null_condition)).show(5)

df.select("started_at", "ended_at", "trip_duration", "member_casual").show(5)

spark.stop()