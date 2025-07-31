# Ingesting data from relational database
intended for doing either one-off ingestion of historical data from a database or rep-
licating change data (often referred to as Change Data Capture (CDC)) from a relational database
on an ongoing basis. When using AWS DMS, the target is either a different database engine (such
as an Oracle-to-PostgreSQL migration), or an Amazon S3-based data lake

While AWS DMS was originally a managed service only (meaning that DMS provisioned one or
more EC2 servers as replication instances), AWS announced an AWS DMS serverless option in
June 2023.

AW glue can make connections to JDBC sources
an be configured to make a connection to a database and download data from tables.
Glue effectively does a select * from the table, reading the table contents into the memory of
the Spark cluster. At that point, you can use Spark to write out the data to Amazon S3, optionally
in an optimized format such as Apache Parquet.
as a concept called job bookmarks, which enables Glue to keep track of which data
was previously processed, and then on subsequent runs only process new data. Glue does this
by having you identify a column (or multiple columns) in the table that will serve as a bookmark
key. The values in this bookmark key must always increase in value, although gaps are allowed

# ingesting streaming data
Increasingly common source of data for analytic projects is data that is continually generated
and needs to be ingested in near real time. Some common sources of this type of data are as follows:

Data from IoT devices (such as smartwatches, smart appliances, and so on)
Telemetry data from various types of vehicles (cars, airplanes, and so on)
Sensor data (from manufacturing machines, weather stations, and so on)
Live gameplay data from mobile games
Mentions of the company brand on various social media platforms
The two primary services for ingesting streaming data within AWS are Amazon Kinesis and Amazon
MSK.
Kinesis is provisioned or on demand