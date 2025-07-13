from diagrams import Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import GlueDataCatalog, Athena, GlueCrawlers

with Diagram("", show=True):
    s3_raw = S3("File Uploaded to S3 bucket")
    lambda_fn = Lambda("Lambda function")
    s3_processed = S3(".parquet files saved in S3")
    glue_crawler = GlueCrawlers("AWS Glue crawler")
    catalog = GlueDataCatalog("AWS Glue Data Catalog")
    athena = Athena("AWS Athena \n for SQL queries") #Athena just reads the data directly from S3, glue crawler only create table with schemas in glue data catalog
    s3_raw >> Edge(label="Trigger") >> lambda_fn >> Edge(label="CSV to Parquet") >> s3_processed >>\
        glue_crawler >> catalog>>athena
