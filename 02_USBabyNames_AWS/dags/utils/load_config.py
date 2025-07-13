import configparser, os
from dags.utils.get_logger import get_logger

logger = get_logger(__name__)
def load_config(config_file_name):
    config = configparser.ConfigParser()
    # logger.info(os.path.abspath(config_file_name))
    if not os.path.exists(config_file_name):
        logger.error(f"Config file not found {config_file_name}")
        raise

    config.read(config_file_name)
    return config