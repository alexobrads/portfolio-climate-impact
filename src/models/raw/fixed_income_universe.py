from pyspark.sql.types import StructType, StructField, StringType, DoubleType

fixed_income_universe_schema = StructType([
    StructField("Identifier", StringType(), nullable=True),
    StructField("Name", StringType(), nullable=True),
    StructField("BondType", StringType(), nullable=True),
    StructField("Country", StringType(), nullable=True),
    StructField("Sector", StringType(), nullable=True),
    StructField("Value", DoubleType(), nullable=True),
    StructField("Scope1", DoubleType(), nullable=True),
    StructField("Scope2", DoubleType(), nullable=True),
    StructField("Scope3", DoubleType(), nullable=True),
    StructField("AssetClass", StringType(), nullable=True)
])
