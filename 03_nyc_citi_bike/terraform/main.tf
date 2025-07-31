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

resource "random_id" "bucket_suffix" {
  byte_length = 8 # Generates 16 hex characters (8 bytes * 2 hex chars per byte)
}

resource "aws_s3_bucket" "s3_bucket" {
  bucket="nyc-citi-bikes-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_object" "glue_script" {
  bucket="nyc-citi-bikes-${random_id.bucket_suffix.hex}"
  key="scripts/my_glue_job.py"
  source = "${path.module}/my_glue_job.py" 
}


output "bucket_name" {
    value = aws_s3_bucket.s3_bucket.bucket 
    description = "Name of created bucket"
}
output "glue_script_s3_path" {
  value = aws_s3_object.glue_script.key
}

# create aws glue role
resource "aws_iam_role" "glue_service_role" {
  name = "createdAWSGlueRole-${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Principal = {
          Service = "glue.amazonaws.com"
        },
        Effect = "Allow",
        Sid    = ""
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_policy" {
  role= aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
resource "aws_iam_role_policy_attachment" "s3_glue_policy" {
  role= aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
output "glue_role_name" {
  value = aws_iam_role.glue_service_role.name
}