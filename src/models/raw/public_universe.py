from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define a schema
public_universe_schema = StructType([
    StructField("Identifier", StringType(), nullable=True),  # 'object' maps to StringType
    StructField("Name", StringType(), nullable=True),         # 'object' maps to StringType
    StructField("Country", StringType(), nullable=True),      # 'object' maps to StringType
    StructField("Sector", StringType(), nullable=True),       # 'object' maps to StringType
    StructField("Value", DoubleType(), nullable=True),         # 'float64' maps to FloatType
    StructField("Scope1", DoubleType(), nullable=True),        # 'float64' maps to FloatType
    StructField("Scope2", DoubleType(), nullable=True),        # 'float64' maps to FloatType
    StructField("Scope3", DoubleType(), nullable=True),        # 'float64' maps to FloatType
    StructField("AssetClass", StringType(), nullable=True)     # 'object' maps to StringType
])