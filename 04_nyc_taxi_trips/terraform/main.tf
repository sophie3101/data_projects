terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
    redshift = {
      source  = "brainly/redshift"
      version = ">= 1.0.0"
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
  bucket="nyc-taxi-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

output "bucket_name" {
    value = aws_s3_bucket.s3_bucket.bucket 
    description = "Name of created bucket"
}
# # resource "aws_s3_object" "glue_script" {
# #   bucket="nyc-taxi-${random_id.bucket_suffix.hex}"
# #   key="scripts/my_glue_job.py"
# #   source = "${path.module}/my_glue_job.py" 
# # }

# # output "glue_script_s3_path" {
# #   value = aws_s3_object.glue_script.key
# # }

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
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "athena_redshift_policy" {
  role = aws_iam_role.redshift_role.name 
  policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
}

resource "aws_iam_role_policy_attachment" "redshift_policy" {
  role = aws_iam_role.redshift_role.name 
  policy_arn = "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
}

output "redshift_role_name" {
  value = aws_iam_role.redshift_role.arn
}


#######################################
###########GLUE CATALOG############
#######################################

resource "aws_glue_catalog_database" "glue_database" {
  name = "nyc_taxi_database"
}

resource "aws_glue_crawler" "taxi_glue_crawler" {
  name          = "taxi_glue_crawler_raw"
  database_name = aws_glue_catalog_database.glue_database.name
  role          = aws_iam_role.glue_service_role.name

  s3_target {
    path = "s3://${aws_s3_bucket.s3_bucket.bucket}/raw"
  }
}

output "glue_database_name"{
  value = aws_glue_catalog_database.glue_database.name
}

resource "aws_glue_catalog_database" "dbt_athena_database" {
  name = "dbt_nyc_taxi_database"
}

output "dbt_database_name"{
  value = aws_glue_catalog_database.dbt_athena_database.name
}

######################################
##########REDSHIFT CLUSTER############
######################################
data "aws_vpc" "default" {
  default = true
}
data "http" "myip" {
  url = "https://ipv4.icanhazip.com"
}

resource "aws_security_group" "redshift_sg" {
  name        = "redshift-access"
  description = "Allow access to Redshift from my laptop"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["${chomp(data.http.myip.response_body)}/32"]  # e.g., "203.0.113.12/32"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# resource "aws_redshift_cluster" "redshift_cluster" {
#   cluster_identifier = "nyc-taxi-redshift-cluster"
#   node_type          = "ra3.large" 
#   cluster_type       = "single-node" 
#   master_username    = var.redshift_cluster_username
#   master_password    = var.redshift_cluster_password
#   encrypted          = true
#   skip_final_snapshot = true 
#   iam_roles = [aws_iam_role.redshift_role.arn]
#   publicly_accessible = true
#   vpc_security_group_ids = [aws_security_group.redshift_sg.id]
# }

# output "redshift_cluster_endpoint" {
#   value = aws_redshift_cluster.redshift_cluster.endpoint
# }

