from pyspark.sql.types import StructType, StructField, StringType, DoubleType

portfolio_schema = StructType([
    StructField("FundName", StringType(), nullable=True),
    StructField("FundValue", DoubleType(), nullable=True),
    StructField("Portfolio Name", StringType(), nullable=False)
])

portfolio_mandatory_columns = ["FundName", "Portfolio Name"]