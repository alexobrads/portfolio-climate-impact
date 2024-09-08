from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def calculate_ownership(df: DataFrame) -> DataFrame:
    return  df.withColumn(
        "Ownership %",
        (col("Asset Weight") * col("FundValue")) / col("Value")
    )
