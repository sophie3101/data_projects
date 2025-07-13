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

provider "aws" {
  region  = "us-east-1"
  profile = "son"
}

#policy
resource "aws_iam_policy" "managedPolicy" {
  name        = "lambdaS3nGluePolicy-${random_id.random_suffix.hex}"
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

resource "random_id" "random_suffix" {
  byte_length = 4
}

#IAM role
resource "aws_iam_role" "lambdaRole" {
  name = "lambdaS3Role-${random_id.random_suffix.hex}"
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
resource "aws_iam_role_policy_attachment" "lambdaAcessPolicy" {
  role       = aws_iam_role.lambdaRole.name
  policy_arn = aws_iam_policy.managedPolicy.arn
}

#lambda layer
# manually toggler, the sdk for pandas layer is already created

resource "aws_lambda_layer_version" "lambdaLayer" {
  filename                 = "${path.module}/layer/awswrangler-layer-3.12.1-py3.9.zip"
  layer_name               = "awswrangler_pandas_3_12_layer"
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
  function_name    = "csv_2_parquet"
  role             = aws_iam_role.lambdaRole.arn
  source_code_hash = data.archive_file.lambdaFunctionZip.output_base64sha256
  runtime          = "python3.9"
  handler          = "lambda_function.lambda_handler"
  layers           = [aws_lambda_layer_version.lambdaLayer.arn]
  memory_size      = 512
  timeout          = 60
}

# s3 bucket

resource "aws_s3_bucket" "s3_bucket" {
  bucket = "us-baby-names-${random_id.random_suffix.hex}"
  force_destroy = true
}

# crawler scans your S3 location, and infer the schema then create catalog table
resource "aws_iam_role" "glue_role" {
  name = "glueCrawlerRole"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "glue.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]

  })
}

resource "aws_iam_role_policy_attachment" "glueAccessPolicy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "glueAccessPolicyS3Read" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_glue_catalog_database" "catalogDatabase" {
  name = "baby_name_database"
}

resource "aws_glue_crawler" "glueCrawler" {
  name          = "babyNamesCrawler"
  database_name = aws_glue_catalog_database.catalogDatabase.name
  role          = aws_iam_role.glue_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.s3_bucket.bucket}/ssa_data_processed"
  }
}

resource "aws_s3_bucket" "AthenaQueryResultsBucket" {
  bucket = "${aws_s3_bucket.s3_bucket.bucket}/athena_query_results"
  force_destroy = true
}
output "bucket" {
  value = aws_s3_bucket.s3_bucket.bucket
}

output "lambda_func_name" {
  value = aws_lambda_function.lambdaFunction.function_name
}

#terraform output # to get bucket name and lambda function name