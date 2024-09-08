from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Define the schema
fixed_income_holdings_schema = StructType([
    StructField("Holding Identifier", StringType(), nullable=True),  # 'object' maps to StringType
    StructField("Fund", StringType(), nullable=True),                  # 'object' maps to StringType
    StructField("Asset Weight", FloatType(), nullable=True),           # 'float64' maps to FloatType
    StructField("Asset Class", StringType(), nullable=True)            # 'object' maps to StringType
])
