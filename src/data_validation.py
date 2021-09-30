import datetime
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations import DataContext
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import Batch
from great_expectations.data_asset import DataAsset
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

EXPECTATION_SUITE_NAME = "test_suite"
BATCH_KWARGS = {
        "data_asset_name": "data",
        "datasource": "taxi_test_data",
        "path": "/Users/filipvanchevski/Desktop/ge_tutorials/data/yellow_tripdata_sample_2019-01.csv",
    }


def run_expectations(context) -> CheckpointResult:
    # context = create_data_context()
    batch = create_batch()

    # generate the expectations for columns and tables
    tables_expectations(batch=batch)
    column_expectations(batch=batch)

    batch.save_expectation_suite(discard_failed_expectations=False)

    results = LegacyCheckpoint(
        name="_temp_checkpoint",
        data_context=context,
        batches=[
            {
                "batch_kwargs": BATCH_KWARGS,
                "expectation_suite_names": [EXPECTATION_SUITE_NAME]
            }
        ]
    ).run()
    return results

    # validation_result_identifier = results.list_validation_result_identifiers()[0]

    # context.build_data_docs()
    # context.open_data_docs(validation_result_identifier)


def create_data_context():
    """
    Create data context
    """
    context = ge.data_context.DataContext()

    return context


def create_batch(context: DataContext) -> DataAsset:
    """
    Creates data batch for validation
    """

    # Feel free to change the name of your suite here. Renaming this will not
    # remove the other one.
    suite = context.get_expectation_suite(EXPECTATION_SUITE_NAME)
    suite.expectations = []

    batch = context.get_batch(BATCH_KWARGS, suite)
    batch.head()

    return batch


def tables_expectations(batch):
    """
    Runs validation on table format and structure
    """

    batch.expect_table_row_count_to_be_between(max_value=11001, min_value=9000)

    batch.expect_table_column_count_to_equal(value=18)

    batch.expect_table_columns_to_match_ordered_list(
        column_list=[
            "_c0",
            "_c1",
            "_c2",
            "_c3",
            "_c4",
            "_c5",
            "_c6",
            "_c7",
            "_c8",
            "_c9",
            "_c10",
            "_c11",
            "_c12",
            "_c13",
            "_c14",
            "_c15",
            "_c16",
            "_c17",
        ]
    )


def column_expectations(batch):
    """
    Runs validation on columns format and structure
    """
    # check for null values
    batch.expect_column_values_to_not_be_null(column="_c0")

    # check for distinct values
    batch.expect_column_distinct_values_to_be_in_set(
        column="_c0", value_set=["1", "2", "4", "vendor_id"]
    )

    # confirm kl divergence less then specified threshold (0.6)
    # TODO make the threshold a param for more flexibility in the tests
    #  depends on use case and how the threshold is resolved
    batch.expect_column_kl_divergence_to_be_less_than(
        column="_c0",
        partition_object={
            "values": ["1", "2", "4", "vendor_id"],
            "weights": [
                0.37536246375362464,
                0.6149385061493851,
                0.009599040095990401,
                9.999000099990002e-05,
            ],
        },
        threshold=0.6,
    )

    # check nulls
    batch.expect_column_values_to_not_be_null(column="_c1")

    # confirm column length
    batch.expect_column_value_lengths_to_be_between(column="_c1", min_value=1)
