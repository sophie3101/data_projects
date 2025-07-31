set s3 bucket name
set variable in airflow_settings.yaml

test glue job locally
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python

1. docker pull public.ecr.aws/glue/aws-glue-libs:5 
2. 
WORKSPACE_LOCATION=/Users/u249637/Documents/Personal/data_projects/03_nyc_citi_bike/
SCRIPT_FILE_NAME=dags/utils/glue_spark_test.py

3.
docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v "$WORKSPACE_LOCATION:/home/hadoop/workspace" \
    -v "$WORKSPACE_LOCATION/temp:/home/hadoop/temp" \
    -e AWS_PROFILE=son \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/"$SCRIPT_FILE_NAME"

docker run -it --rm \
    -v ~/.aws:/home/hadoop/.aws \
    -v "$WORKSPACE_LOCATION:/home/hadoop/workspace" \
    -v "$WORKSPACE_LOCATION/temp:/home/hadoop/temp" \
    -e AWS_PROFILE=son \
    --name glue5_spark_submit \
    public.ecr.aws/glue/aws-glue-libs:5 \
    spark-submit /home/hadoop/workspace/dags/utils/glue_spark.py --job_name my_test_glue_job --s3_input_path s3://nyc-citi-bikes-6dfe261a97b83ccc/raw_zones/year=2025/month=03/ --s3_output_path s3://nyc-citi-bikes-6dfe261a97b83ccc/clean_zones/year=2025/month=03/