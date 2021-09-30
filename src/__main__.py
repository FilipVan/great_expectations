import argparse
import logging
import dateutil

from utils import get_spark_session, read_project_config
from data_processing import read_input_files
from data_validation import create_data_context, run_expectations

log_level = logging.INFO
logging.basicConfig(level=log_level)


def main():
    """Initialises configuration properties, init spark, and set execution workflow"""

    # init spark
    spark = get_spark_session()
    config = read_project_config()
    input_dir = config.get("data.input_dir")

    # read input data
    raw_data = read_input_files(spark, input_dir)

    # validate input data
    # first we generate great_expectations context
    context = create_data_context()

    # run expectations and get test results
    results = run_expectations(context)
    results.list_validation_results()

    # TODO check what we can get from the results and fail the job if any of the tests failed
    return "Hello world"


if __name__ == "__main__":
    main()
