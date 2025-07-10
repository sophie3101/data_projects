import awswrangler as wr
from urllib.parse import unquote_plus

def lambda_handler(event, context):
    #get example log (for testing): https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html
    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key = unquote_plus(record['s3']['object']['key'])
        
    key_list = key.split("/")
    print(f'key_list: {key_list}')
    db_name = key_list[0]
    table_name = key_list[1].replace(".csv", "")

    print(f'Bucket: {bucket}')
    print(f'Key: {key}')
    print(f'DB Name: {db_name}')
    print(f'Table Name: {table_name}')
        
    input_path = f"s3://{bucket}/{key}"
    print(f'Input_Path: {input_path}')
    output_path = f"s3://sophie-datasets/us_baby_names_parquet/"
    print(f'Output_Path: {output_path}')

    current_databases = wr.catalog.databases()
    if db_name not in current_databases.values:
        print(f'- Database {db_name} does not exist ... creating')
        wr.catalog.create_database(db_name)
    else:
        print(f'- Database {db_name} already exists')

    input_df = wr.s3.read_csv([input_path])
    result = wr.s3.to_parquet(
        df=input_df, 
        path=output_path, 
        dataset=True,
        database=db_name,
        table=table_name,
        mode="append")
        
    print("RESULT: ")
    print(f'{result}')

    return result
