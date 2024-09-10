from pyspark.sql.types import StructType, StructField, StringType, DoubleType

holdings_schema = StructType([
        StructField("Holding Identifier", StringType(), nullable=True),
        StructField("Fund", StringType(), nullable=True),
        StructField("Asset Weight", DoubleType(), nullable=True),
        StructField("Asset Class", StringType(), nullable=True),
        StructField("Portfolio Name", StringType(), nullable=False)
    ])

holdings_mandatory_columns = ["Holding Identifier", "Fund", "Portfolio Name"]
