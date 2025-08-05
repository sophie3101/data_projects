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
#######################################
############S3 BUCKET##################
#######################################
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

#######################################
############GLUE ROLE##################
#######################################
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

#######################################
#######REDSHIFT SPECTRUM ROLE##########
#######################################

resource "aws_iam_role" "redshift_role" {
  name = "createdRedShiftRole-${random_id.bucket_suffix.hex}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole"
            ],
            "Principal": {
                "Service": [
                    "redshift.amazonaws.com"
                ]
            }
        }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_redshift_policy" {
  role = aws_iam_role.redshift_role.name 
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

resource "aws_iam_role_policy_attachment" "s3_redshift_policy" {
  role = aws_iam_role.redshift_role.name 
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

output "redshift_role_name" {
  value = aws_iam_role.redshift_role.arn
}

#######################################
###########REDSHIFT CLUSTER############
#######################################
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier = "citibike-redshift-cluster"
  node_type          = "ra3.large" 
  cluster_type       = "single-node" 
  master_username    = var.redshift_cluster_username
  master_password    = var.redshift_cluster_password
  encrypted          = true
  skip_final_snapshot = true 
  iam_roles = [aws_iam_role.redshift_role.arn]
}

#######################################
###########GLUE CATALOG############
#######################################

resource "aws_glue_catalog_database" "glue_database" {
  name = "citibike_database"
}

resource "aws_glue_crawler" "citibike_glue_crawler" {
  name          = "citibike_glue_crawler"
  database_name = aws_glue_catalog_database.glue_database.name
  role          = aws_iam_role.glue_service_role.name

  s3_target {
    path = "s3://${aws_s3_bucket.s3_bucket.bucket}/clean_zones"
  }
}

resource "aws_glue_crawler" "weather_glue_crawler" {
  name          = "weather_glue_crawler"
  database_name = aws_glue_catalog_database.glue_database.name
  role          = aws_iam_role.glue_service_role.name

  s3_target {
    path = "s3://${aws_s3_bucket.s3_bucket.bucket}/weather_data"
  }
}