from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType

holdings_schema = StructType([
    StructField("Holding Identifier", StringType(), True),
    StructField("Fund", StringType(), True),
    StructField("Asset Weight", FloatType(), True),
    StructField("Asset Class", StringType(), True),
    StructField("Identifier", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Sector", StringType(), True),
    StructField("Value", DoubleType(), True),
    StructField("Scope1", DoubleType(), True),
    StructField("Scope2", DoubleType(), True),
    StructField("Scope3", DoubleType(), True),
    StructField("AssetClass", StringType(), True),
    StructField("BondType", StringType(), True)
])