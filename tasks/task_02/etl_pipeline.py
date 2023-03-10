from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as fs
from typing import Tuple
import pgeocode
import yaml
import os


def get_province_and_region(postal_code: str) -> Tuple[str, str]:
    """
    This function takes an Italian postal code as input and returns the corresponding province and region.

    :param postal_code: The Italian postal code (CAP)
    :type postal_code: str
    :return: A tuple containing the province and region corresponding to the input postal code
    :rtype: tuple[str, str]
    """
    # Create a Nominatim instance for Italy
    nomi = pgeocode.Nominatim('it')

    # Query the Nominatim database for information about the given postal code
    info = nomi.query_postal_code(postal_code).to_dict()

    # Extract the province and region information from the dictionary
    province = info['county_code']
    region = info['state_name']

    # Return the province and region as a tuple
    return province, region


def get_upper(word: str) -> str:
    """
    This function takes a string as input and returns the same string in uppercase letters.

    :param word: A string to be converted to uppercase
    :type word: str
    :return: The input string in uppercase letters
    :rtype: str
    """
    word = word.upper()
    return word


def save_dataframe_to_csv():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("ETL-pipeline") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Get the SparkContext and set the log level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Read the content of a YAML file
    metadata = yaml.safe_load(open('metadata.yaml'))

    # Read the JSON file into a DataFrame
    df = spark.read.json("data/raw/orders.jsonl")

    # Transform nested structure into a flat structure
    df = df.withColumn("StateOrRegion", fs.col("ShippingAddress.StateOrRegion")) \
        .withColumn("PostalCode", fs.col("ShippingAddress.PostalCode")) \
        .withColumn("City", fs.col("ShippingAddress.City")) \
        .withColumn("CountryCode", fs.col("ShippingAddress.CountryCode")) \
        .withColumn("Amount", fs.col("OrderTotal.Amount")) \
        .withColumn("IsBusinessOrder", fs.when(fs.col("IsBusinessOrder") == 'true', 1).otherwise(0)) \
        .withColumn("IsReplacementOrder", fs.when(fs.col("IsBusinessOrder") == 'true', 1).otherwise(0)) \
        .drop("OrderTotal", "ShippingAddress")

    # Cast columns
    cast_dictionary = metadata['colums_to_cast']

    for data_type, columns in cast_dictionary.items():
        for column in columns:
            df = df.withColumn(column, fs.col(column).cast(data_type))

    # Define the UDF
    get_province_and_region_udf = fs.udf(lambda x: get_province_and_region(x), StructType([
        StructField("province", StringType()),
        StructField("region", StringType())
    ]))

    # Define the UDF
    get_upper_udf = fs.udf(lambda x: get_upper(x), StringType())

    # Apply the UDF to the PostalCode column
    df = df.withColumn("province_region", get_province_and_region_udf("PostalCode"))

    # Split the province_region column into separate columns for province and region
    df = df.withColumn("Province", df["province_region"]["province"]) \
        .withColumn("Region", df["province_region"]["region"]) \
        .drop("province_region", "StateOrRegion")

    # Replace null values with an empty string
    df = df.withColumn("City", fs.when(fs.col("City").isNull(), "").otherwise(fs.col("City"))) \
        .withColumn("Region", fs.when(fs.col("Region").isNull(), "").otherwise(fs.col("Region")))

    # Apply the UDF to a column
    df = df.withColumn("City", get_upper_udf(fs.col("City"))) \
        .withColumn("Region", get_upper_udf(fs.col("Region")))

    # Extract year, month, and quarter from PurchaseDate
    df = df.withColumn("Year", fs.year("PurchaseDate")) \
        .withColumn("YearMonth", fs.date_format("PurchaseDate", "yyyyMM")) \
        .withColumn("YearQuarter", fs.concat(fs.year("PurchaseDate"), fs.lit("Q"), fs.quarter("PurchaseDate"))) \
        .withColumn("YearWeek", fs.concat(fs.year("PurchaseDate"), fs.lit("W"), fs.weekofyear("PurchaseDate"))) \
        .withColumn("Weekday", fs.date_format(fs.col("PurchaseDate"), "u").cast("integer"))

    # Reorder columns
    ordered_columns = metadata['ordered_columns']

    # Select ordered columns
    df = df.select(*ordered_columns)

    # Drop duplicates
    df = df.dropDuplicates()

    # Create folder if it not exists
    folder_path = 'data/prepared'
    os.makedirs(folder_path, exist_ok=True)

    # Define filepath
    filename = f"orders.csv"
    filepath = os.path.join(folder_path, filename)

    # Save DataFrame in CSV format
    df.toPandas().to_csv(filepath, index=False)


if __name__ == "__main__":
    save_dataframe_to_csv()
