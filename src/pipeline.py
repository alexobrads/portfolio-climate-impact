from pyspark.sql import SparkSession

from src.models.raw.fixed_income_universe import fixed_income_universe_schema
from src.models.raw.public_universe import public_universe_schema

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PortfolioEmissionsPipeline") \
    .getOrCreate()

# Load reference data
df_public_universe = spark.read.csv("path/to/PublicEquityUniverse.csv", schema=public_universe_schema, header=True)
df_fixed_income_universe = spark.read.csv("path/to/FixedIncomeUniverse.csv", schema=fixed_income_universe_schema, header=True)

# Cache the reference data
df_public_universe.cache()
df_fixed_income_universe.cache()

