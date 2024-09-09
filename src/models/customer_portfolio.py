from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

customer_portfolio_schema = StructType([
    StructField("FundName", StringType(), True),
    StructField("FundValue", DoubleType(), True),
    StructField("Holdings", ArrayType(
        StructType([
            StructField("HoldingIdentifier", StringType(), True),
            StructField("AssetWeight", DoubleType(), True),
            StructField("AssetClass", StringType(), True)
        ])
    ), False)
])