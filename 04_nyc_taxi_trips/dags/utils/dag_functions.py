import os, json
from datetime import datetime
from utils.aws_utils import check_s3_prefix_existence
def get_raw_parquet_links(taxi_color, s3_hook, bucket_name, temp_dir):
        """
            Retrieves Citi Bike parquet file links from the NYC taxi website.
        """
        now=datetime.now()
        cur_year = str(now.year)
        months = [m for m in range(1, now.month -1)]
        # s3_hook=S3Hook(aws_conn_id=AWS_CONN_ID)
        parquet_configs=[]

        for m in months:
            parquet_link=f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_color}_tripdata_{cur_year}-0{m}.parquet"
            temp_file = os.path.join(temp_dir, os.path.basename(parquet_link))
            s3_raw_prefix=f"raw/{taxi_color}_trips/year={cur_year}/month=0{m}/{os.path.basename(parquet_link)}"
            if not check_s3_prefix_existence(s3_hook, bucket_name, os.path.dirname(s3_raw_prefix)):
                parquet_configs.append((parquet_link, temp_file, s3_raw_prefix))

        return parquet_configs 

def is_table_exists(hook, schema_name, table_name):
    """get_first return the first row"""
    res = hook.get_first(f"""
            SELECT 1
            FROM SVV_EXTERNAL_TABLES
            WHERE schemaname = '{schema_name}'
                AND tablename = '{table_name}';
        """)
    if res is None:
        return False 
    return True

def add_partition(hook, schema_name, table_name, year, month, s3_path):
    try:
        add_partition_query = f"""
            ALTER TABLE {schema_name}.{table_name}
            ADD PARTITION (year={year}, month={month})
            LOCATION '{s3_path}';
        """
        hook.run(sql=add_partition_query, autocommit=True)
    except Exception as e:
        print(e)
        raise

def get_month_partions(hook, schema_name, table_name):
    res = hook.get_records(f"""
        SELECT 
            values AS partition_values
        FROM SVV_EXTERNAL_PARTITIONS
            WHERE schemaname = '{schema_name}'
            AND tablename = '{table_name}';
    """)

    parsed_months = []
    for partition in res:
        month = json.loads(partition[0])[1] # partion value ['["2025","1"]']
        if int(month) < 10:
            month="0"+month
        parsed_months.append(month)
    
    
    return parsed_months

def get_months_to_partition(s3_hook, bucket_name, taxi_color):
    now=datetime.now()
    cur_year = str(now.year)

    s3_paths=[]
    raw_prefixes = s3_hook.list_prefixes(bucket_name=bucket_name, prefix=f"raw/{taxi_color}_trips/year={cur_year}/", delimiter="/")
    months=[]
    for prefix in raw_prefixes:
        s3_path=f"s3://{bucket_name}/{prefix}"
        s3_paths.append(s3_path)
        months.append(prefix.split("/month=")[1].replace("/",""))
    
    return s3_paths, months