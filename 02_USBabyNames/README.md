# US Baby Name Analysis

This project extracts U.S. baby names from the Social Security Administration (SSA) dataset, enriches the data with information on historical figures and name meanings, and converts the data from CSV to Parquet for efficient processing. It also features an AWS Lambda function, created using Terraform, that triggers automatically when a new CSV file is uploaded to a specified S3 path.

## Features
- Extract Baby Names and occurences by year
Parses official U.S. baby name data from the SSA (Social Security Administration) datasets.

- AWS Automation with Terraform for CSV to parquet transformation

    - Create an IAM policy and role, attach the policy to the role, and assign the role to a Lambda function.

    - Create an AWS Lambda function with a Lambda Layer that includes the Pandas SDK.

    - Add a trigger to the Lambda function: when a file is uploaded to an S3 bucket, the function transforms the file to Parquet format.

- Uses AWS Athena to perform SQL queries over the transformed Parquet files stored in S3.

- Name Enrichment

    - Uses NameAPI to fetch information about historical figures associated with a name.

    - Scrapes  https://www.behindthename.com/name/ website to retrieve the meaning of each name using BeautifulSoup

    - Leverages aiohttp and asyncio to send multiple HTTP requests concurrently, improving scraping performance and efficiency.

- Baby Names Analysis:
    Once the baby name data is transformed to Parquet and available in S3, use AWS Athena to run SQL queries for trend analysis and insights. The `doc/name_analysis.sql` file contains predefined queries, including examples for:

    - Top names of all time
    - Top names of the year
    - Popular names by gender
    - Popular names with longest streak

## Project Structure
```
project-root/
│
├── aws/                         # AWS-related infrastructure and code
│   ├── terraform/
│   │   ├── main.tf              # Main Terraform configuration
│   │   ├── layer/
│   │   │   └── awswrangler-layer-3.12.1-py3.9.zip
│   │   └── lambda_function/
│   │       ├── function.zip
│   │       └── lambda_function.py
│
├── data/                        # Raw and processed CSV files
├── doc/                        # Raw and processed CSV files
|   ├── name_analysis.sql        # SQL queries for the analysis
│
└── README.md                    # Project documentation
```

## Installation & Deployment
#### Set up Python environment

```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

####  Configure AWS CLI
```aws configure```

You’ll be prompted to input:
- AWS Access Key ID
- AWS Secret Access Key
- Default region (e.g., us-east-1)
#### Deploy Infrastructure with Terraform

```
cd aws/terraform
terraform init
terraform apply
```
Confirm the plan to provision:
- IAM role and policy
- S3 bucket
- Lambda function with layer
- S3 trigger for Lambda
## To Do
- Add unit tests
- Build API from the output datasets