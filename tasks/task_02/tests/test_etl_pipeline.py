import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as fs


@pytest.fixture
def orders_df():
    spark = SparkSession.builder \
        .appName("test-ETL-pipeline") \
        .getOrCreate()

    orders_csv_path = "data/prepared/orders.csv"
    orders_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(orders_csv_path)

    yield orders_df

    orders_df.unpersist()
    spark.stop()


def test_orders_df_no_duplicates(orders_df):
    # Check for duplicates
    num_duplicates = orders_df.count() - orders_df.dropDuplicates().count()

    assert num_duplicates == 0, f"Found {num_duplicates} duplicates in orders_df"


def test_orders_df_no_null_values(orders_df):
    # Check for null values
    null_counts = {col: orders_df.filter(fs.col(col).isNull()).count() for col in orders_df.columns}

    for col, null_count in null_counts.items():
        assert null_count == 0, f"Column {col} has {null_count} null values in orders_df"
