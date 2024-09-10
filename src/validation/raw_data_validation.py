from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def validate_schema(df, expected_schema):
    return df.schema == expected_schema

def validate_rows(df: DataFrame, mandatory_columns: list) -> bool:
    # Check for nulls in mandatory columns
    null_checks = [df.filter(col(col_name).isNull()).count() == 0 for col_name in mandatory_columns]

    # Check if there are duplicates
    has_duplicates = df.count() == df.dropDuplicates().count()

    return all(null_checks) and has_duplicates

def raw_data_validation(df: DataFrame, expected_schema, mandatory_columns):
    return validate_schema(df, expected_schema) and validate_rows(df, mandatory_columns)