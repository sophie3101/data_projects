from diagrams import Diagram, Edge, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import GlueDataCatalog, Athena, GlueCrawlers, Glue, Redshift
from diagrams.onprem.iac import Terraform
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Tableau, Dbt
with Diagram("", show=True):
    terraform = Terraform()
    airflow = Airflow("Airflow")
    s3=S3("AWS S3")
    glue=Glue("AWS Glue")
    crawler = GlueCrawlers("AWS Glue Crawler")
    redshift = Redshift("Redshfit Spectrum")
    tableau = Tableau("Tableau")
    athena = Athena("AWS Athena")
    dbt= Dbt("DBT")
    terraform - airflow - s3 - glue - crawler - dbt - redshift -  athena - tableau
   