from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

# Updated schema with aggregated financed emissions fields
customer_portfolio_With_emissions_schema = StructType([
    StructField("Portfolio Name", StringType(), True),
    StructField("FundName", StringType(), True),
    StructField("FundValue", DoubleType(), True),
    StructField("Holdings", ArrayType(
        StructType([
            StructField("HoldingIdentifier", StringType(), True),
            StructField("AssetWeight", DoubleType(), True),
            StructField("AssetClass", StringType(), True),
            StructField("OwnershipPercent", DoubleType(), True),
            StructField("FinancedScope1", DoubleType(), True),
            StructField("FinancedScope2", DoubleType(), True),
            StructField("FinancedScope3", DoubleType(), True)

        ])
    ), False)
])