import os
import logging

from pyspark.sql import SparkSession

from pyhocon import ConfigFactory, ConfigTree

log_level = logging.INFO
logging.basicConfig(level=log_level)


def get_spark_session(app_name: str = "great_expectations"):
    """
    Instantiate spark session.
    """

    return SparkSession.builder.appName(app_name).getOrCreate()


def get_main_dir():
    """
    Gets current working directory
    """
    current_working_dir = os.getcwd()
    os.environ["WORKING_DIR"] = current_working_dir

    print(f"Current working directory is: {current_working_dir}")
    return current_working_dir


def read_project_config() -> ConfigTree:
    """
    Parses the projects config file
    """
    conf_file = f"/Users/filipvanchevski/Desktop/ge_poc/great_expectations/config/config.conf"

    return ConfigFactory.parse_file(conf_file)
