from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
import psycopg2
def create_table(conn, table_name):
    
    cur = conn.cursor()
    # cur.execute("SELECT version();")
    # results = cur.fetchall()
    sql = f""" 
    CREATE TABLE IF NOT EXISTS  {table_name} (
            station_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            capacity INTEGER
        );
    """
    cur.execute(sql)
    conn.commit()
    cur.close()

def register_source_table(table_env, table_name, topic_name):
    # register the metadata in Flink
    query = f"""
        CREATE TABLE {table_name} (
            station_id VARCHAR,
            name VARCHAR,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            capacity INTEGER,
            event_time TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
        ) WITH (
           'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = '{topic_name}',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    table_env.execute_sql(query)
    return 

def register_sink_table(table_env, table_name):
    query = f"""
        CREATE TABLE {table_name} (
            station_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            capacity INTEGER
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    table_env.execute_sql(query)
    return 

def main():
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="postgres",
            user="postgres",
            password="postgres",
            port=5432# Default is 5432
        )

        # api doc: https://nightlies.apache.org/flink/flink-docs-master/docs/dev/python/table/table_environment/#create-a-tableenvironment
        # watermark: https://medium.com/@ipolyzos_/understanding-watermarks-in-apache-flink-c8793a50fbb8
        # Set up the execution environment
        env = StreamExecutionEnvironment.get_execution_environment()
        env.enable_checkpointing(10 * 1000)
        # env.set_parallelism(1)

        # Set up the table environment
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)

        source_table = "src_station_info"
        sink_table = "station_info"
        register_source_table(table_env=t_env, table_name=source_table, topic_name="station_info")
        create_table(conn, sink_table)
        register_sink_table(table_env=t_env, table_name=sink_table)
        t_env.execute_sql(
            f"""
                INSERT INTO {sink_table}
                SELECT
                    station_id,
                    name,
                    longitude,
                    latitude,
                    capacity 
                FROM (
                    SELECT station_id, name, longitude, latitude, capacity,
                    ROW_NUMBER() OVER(PARTITION BY station_id ORDER BY event_time desc) as rnk
                    FROM {source_table}
                )
                WHERE rnk=1
                    """
        ).wait()
    except Exception as e: 
        print("Writing records from Kafka to JDBC failed:", str(e))
if __name__ == '__main__':
    main()
    