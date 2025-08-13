import requests, os
def download_parquet_file(link, output_file):
    try:
        # if os.path.exists(output_file):
        #     return
        response = requests.get(link)
        with open(output_file, "wb") as fh:
           fh.write(response.content)
    except Exception as e:
        print(e)
        raise
if __name__=="__main__":    
    download_parquet_file("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet", "raw/test.parquet")