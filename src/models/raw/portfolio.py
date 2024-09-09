from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema
portfolio_schema = StructType([
    StructField("FundName", StringType(), nullable=True),
    StructField("FundValue", DoubleType(), nullable=True)
])
