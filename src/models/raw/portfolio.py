from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema
portfolio_schema = StructType([
    StructField("FundName", StringType(), nullable=True),  # 'object' maps to StringType
    StructField("FundValue", DoubleType(), nullable=True)   # 'object' maps to FloatType
])
