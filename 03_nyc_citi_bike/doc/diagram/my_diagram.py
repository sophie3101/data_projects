from diagrams import Diagram, Edge, Cluster
from diagrams.aws.storage import S3
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import GlueDataCatalog, Athena, GlueCrawlers, Glue, Redshift
from diagrams.onprem.iac import Terraform
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Tableau, Dbt
from diagrams.custom import Custom
with Diagram("workflow", show=True,direction="TB"):
    airflow = Airflow() 

    with Cluster("EXTRACT"):
        dataset = Custom("NYC CitiBike", "./citibikelogo.png")
        s3_raw=S3("AWS S3 raw zones")
        extract_steps=dataset >> Edge(label="downloaded,\n extracted \n and uploaded") >> s3_raw

    with Cluster("LOAD"):
        glue = Glue()
        s3_clean=S3("AWS S3 clean zones")
        crawler = GlueCrawlers("Glue Crawler")
        load_steps = glue >> Edge(label="clean data") >> s3_clean >> Edge(label="trigger \n Glue crawlers") >> crawler

    with Cluster("TRANSFORM"):
        athena_src = Athena("AWS Athena")        
        dbt = Dbt()
        
        # athena = Athena("Analystics")
        transform_steps = athena_src >> dbt  
    
    with Cluster("DATA ANALYSTICS & VIZ"):
        redshift = Redshift("Analytics")
        tableau = Tableau("Data Visualization")
        analytics = redshift >> tableau

    airflow>>extract_steps >>load_steps >>transform_steps >>analytics
   