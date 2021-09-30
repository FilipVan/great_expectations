from pyspark.sql import SparkSession, SQLContext, Row, DataFrame

from pyhocon import ConfigFactory
from typing import Dict


def read_input_files(spark_session: SparkSession, input_dir: str) -> DataFrame:
    """
    Read input files
    :param spark_session: SparkSession
    :param input_dir: Local directory which contains input files

    returns Dataframe
    """

    data = spark_session.read.csv(input_dir)

    return data
