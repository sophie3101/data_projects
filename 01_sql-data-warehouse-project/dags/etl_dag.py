from airflow.decorators import dag,task
from pendulum import datetime
import json, sys, os, logging
# Add src to PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl import extract, transform, load
from src.utils.logger import get_logger
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    start_date=datetime(2025,7,8),
    schedule="@daily",
    catchup=False
)

def etl_dag():
    logger = get_logger(__name__, "dag.log")
    @task
    def load_config(config_file):
        try:
            with open(config_file, 'r') as json_fh:
                config = json.load(json_fh)
                logger.info("Config loaded successfully")
                return config
        except Exception as e:
            logger.error("ETL process failed")
            logger.error(e)

    @task
    def extract_data(config):
        logging.info("start to donwload raw files")
        extract.download_raw_files(config.get('raw_dataset'))
        logging.info("download finish")
    @task 
    def transform_data(config):
        transform.transform_datasets(**config)

    @task
    def load_to_db(postgres_conn_id_, config):
        try:
            hook = PostgresHook(postgres_conn_id=postgres_conn_id_)
            logger.info(f"Successfully instantiated PostgresHook for connection '{postgres_conn_id_}'")
            logger.info(f"Connecting to DB: {hook.get_uri()}")
            conn = hook.get_conn()
            cursor = conn.cursor()
            ddl_file = config.get("postgresql")['ddl']

            # create table
            with open(ddl_file, 'r') as fh:
                ddl_query = fh.read()
                cursor.execute(ddl_query)
                conn.commit()
            # load table content
            # make sure table in source_erp is processed first
            source_order = ["source_erp", "source_crm"]
            for source in source_order:
                # if source in config['processed_dataset']['child_items']:
                for child_key, child_values in config['processed_dataset']['child_items'].items():
                    if child_key==source:
                        logger.info(f"{child_key} {source}")
                        for file_name in child_values:
                            file_path = os.path.join(config['processed_dataset']['dest_folder'], child_key, file_name)
                            table_name = file_name.replace(".csv", "")
                            logger.info(f"{table_name}, {file_path}")
                            with open(file_path, 'r') as f:
                                cursor.copy_expert(f"COPY {table_name} \
                                                    FROM STDIN \
                                                    WITH CSV HEADER DELIMITER ',' ;", 
                                                f)
                            conn.commit()

        except Exception as e:
            print(f" {e}")
        finally:
            cursor.close()
            conn.close()

    config_ob = load_config("./config.json")
    extract_task = extract_data(config_ob)
    transform_task = transform_data(config_ob)
    load_task = load_to_db("my_local_pg", config_ob)

    # set dependency:
    extract_task >>transform_task>>load_task

etl_dag()