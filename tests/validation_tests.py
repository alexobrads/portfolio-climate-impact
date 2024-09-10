import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col

from src.validation.raw_data_validation import validate_rows, validate_schema


def create_spark_session():
    return SparkSession.builder \
        .appName("TestValidationFunctions") \
        .master("local[*]") \
        .getOrCreate()

def test_validate_schema_correct(spark):
    schema = StructType([
        StructField("FundName", StringType(), True),
        StructField("FundValue", DoubleType(), True),
        StructField("HoldingIdentifier", StringType(), True)
    ])
    data = [
        ("Fund A", 1000.0, "ID1"),
        ("Fund B", 2000.0, "ID2"),
        ("Fund A", 1500.0, "ID3")
    ]
    df = spark.createDataFrame(data, schema)
    result = validate_schema(df, schema)
    assert result, "Schema validation failed when it should have passed."

def test_validate_schema_incorrect(spark):
    schema = StructType([
        StructField("FundName", StringType(), True),
        StructField("FundValue", DoubleType(), True),
        StructField("WrongColumn", StringType(), True)
    ])
    data = [
        ("Fund A", 1000.0, "ID1"),
        ("Fund B", 2000.0, "ID2"),
        ("Fund A", 1500.0, "ID3")
    ]
    df = spark.createDataFrame(data, schema)
    correct_schema = StructType([
        StructField("FundName", StringType(), True),
        StructField("FundValue", DoubleType(), True),
        StructField("HoldingIdentifier", StringType(), True)
    ])
    result = validate_schema(df, correct_schema)
    assert not result, "Schema validation passed when it should have failed."

def test_validate_rows_no_nulls_no_duplicates(spark):
    schema = StructType([
        StructField("FundName", StringType(), True),
        StructField("FundValue", DoubleType(), True),
        StructField("HoldingIdentifier", StringType(), True)
    ])
    data = [
        ("Fund A", 1000.0, "ID1"),
        ("Fund B", 2000.0, "ID2"),
        ("Fund A", 1500.0, "ID3")
    ]
    df = spark.createDataFrame(data, schema)
    result = validate_rows(df, ["FundName", "FundValue", "HoldingIdentifier"])
    assert result, "Row validation failed when there were no nulls or duplicates."

def test_validate_rows_with_duplicates(spark):
    schema = StructType([
        StructField("FundName", StringType(), True),
        StructField("FundValue", DoubleType(), True),
        StructField("HoldingIdentifier", StringType(), True)
    ])
    data = [
        ("Fund A", 1000.0, "ID1"),
        ("Fund A", 1000.0, "ID1"),
        ("Fund B", 2000.0, "ID2")
    ]
    df = spark.createDataFrame(data, schema)
    result = validate_rows(df, ["FundName", "FundValue", "HoldingIdentifier"])
    assert not result, "Row validation passed when there were duplicate rows."


if __name__ == "__main__":
    spark = create_spark_session()

    test_validate_schema_correct(spark)
    test_validate_schema_incorrect(spark)
    test_validate_rows_no_nulls_no_duplicates(spark)
    test_validate_rows_with_duplicates(spark)
    print("All tests passed.")

