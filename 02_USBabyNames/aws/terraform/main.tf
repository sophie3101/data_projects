# this block is optional
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
  }
  required_version = ">= 1.2"
}

#begin here
provider "aws" {
  region  = "us-east-1"
  profile = "son"
}

#policy
resource "aws_iam_policy" "managedPolicy" {
  name        = "lambdaS3nGluePolicy"
  path        = "/"
  description = "Policy to attach to IAM role"

  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Action" : [
          "logs:PutLogEvents",
          "logs:CreateLogGroup",
          "logs:CreateLogStream"
        ],
        "Resource" : "arn:aws:logs:*:*:*"
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "s3:*"
        ],
        "Resource" : [
          "arn:aws:s3:::*",
          "arn:aws:s3:::*/*"
        ]
      },
      {
        "Effect" : "Allow",
        "Action" : [
          "glue:*"
        ],
        "Resource" : "*"
      }
    ]
  })
}

#role
resource "aws_iam_role" "iamRole" {
  name = "lambdaS3Role"
  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

#attach policy to role
resource "aws_iam_role_policy_attachment" "attachedPolicy" {
  role       = aws_iam_role.iamRole.name
  policy_arn = aws_iam_policy.managedPolicy.arn
}

#lambda layer
resource "aws_lambda_layer_version" "lambdaLayer" {
  filename                 = "${path.module}/layer/awswrangler-layer-3.12.1-py3.9.zip"
  layer_name               = "awswrangler_pandas_3_12"
  description              = "awswranger SDK for pandas"
  compatible_runtimes      = ["python3.9"]
  compatible_architectures = ["x86_64", "arm64"]
}

#create lambda function with layer
data "archive_file" "lambdaFunctionZip" {
  type        = "zip"
  source_file = "${path.module}/lambda_function/lambda_function.py"
  output_path = "${path.module}/lambda_function/function.zip"
}

resource "aws_lambda_function" "lambdaFunction" {
  filename         = data.archive_file.lambdaFunctionZip.output_path
  function_name    = "csv_to_parquet"
  role             = aws_iam_role.iamRole.arn
  source_code_hash = data.archive_file.lambdaFunctionZip.output_base64sha256
  runtime          = "python3.9"
  handler          = "lambda_function.lambda_handler"
  layers           = [aws_lambda_layer_version.lambdaLayer.arn]
  memory_size      = 512
  timeout          = 60
}

# s3 resource for invoking lambda function, the bucket already exists so I reference with data block
data "aws_s3_bucket" "bucket" {
  bucket = "sophie-datasets"
}
# Add lambda permissions to allow S3 to invoke the Lambda
resource "aws_lambda_permission" "lambdaAllowS3" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambdaFunction.arn
  principal     = "s3.amazonaws.com"
  source_arn    = data.aws_s3_bucket.bucket.arn
}
# Configure the S3 bucket to send events to Lambda
resource "aws_s3_bucket_notification" "bucketNotification" {
  bucket = data.aws_s3_bucket.bucket.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.lambdaFunction.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "us_baby_names/"
    filter_suffix       = ".csv"
  }
  depends_on = [aws_lambda_permission.lambdaAllowS3]
}