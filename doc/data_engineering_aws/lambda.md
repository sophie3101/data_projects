Lambda layers allow your Lambda function to bring in additional code, packaged as a .zip file
By creating a Lambda layer for the AWS SDK for pandas library, we can use AWS SDK for pandas
in any of our Lambda functions just by ensuring this Lambda layer is attached to the function.

example of this is AWS SDK for pandas: https://github.com/aws/aws-sdk-pandas/releases/download/3.0.0/awswrangler-layer-3.0.0-py3.9.zip

copy this zip file to aws lambda


- Create IAM policy and role for Lambda function