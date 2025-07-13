import awswrangler as wr
from urllib.parse import unquote_plus
import json
import os

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")

    if 'Records' not in event:
        print("No S3 records found in the event.")
        return {
            'statusCode': 400,
            'status': 'error',
            'message': 'No S3 records found in the event.'
        }

    processed_files = []
    failed_files = []

    for record in event['Records']:
        try:
            if 's3' not in record:
                print("Record missing S3 info. Skipping.")
                continue

            source_bucket = record['s3']['bucket']['name']
            source_key = unquote_plus(record['s3']['object']['key'])

            print(f"Processing: s3://{source_bucket}/{source_key}")

            # Generate output key
            base_key, _ = os.path.splitext(source_key)
            parquet_key = base_key.replace("raw", "processed") + ".parquet"

            # Read CSV into DataFrame
            df = wr.s3.read_csv(f"s3://{source_bucket}/{source_key}")
     
            # Write to Parquet
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{source_bucket}/{parquet_key}",
            )

            print(f"Saved Parquet: s3://{source_bucket}/{parquet_key}")
            processed_files.append(parquet_key)

        except Exception as e:
            print(f"Error processing {record['s3']['object']['key']}: {e}")
            failed_files.append(record['s3']['object']['key'])

    if failed_files:
        return {
            'statusCode': 207,  # Multi-status
            'status': 'partial_success',
            'processed': processed_files,
            'failed': failed_files
        }

    return {
        'statusCode': 200,
        'status': 'success',
        'processed': processed_files
    }
