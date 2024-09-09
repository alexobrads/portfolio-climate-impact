from pyspark.sql.types import StructType, StructField, StringType, DoubleType

holdings_schema = StructType([
    StructField("Holding Identifier", StringType(), nullable=True),
    StructField("Fund", StringType(), nullable=True),
    StructField("Asset Weight", DoubleType(), nullable=True),
    StructField("Asset Class", StringType(), nullable=True)
])
