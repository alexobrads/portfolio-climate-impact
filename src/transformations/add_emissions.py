from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_emissions(customer_portfolio_df: DataFrame, public_markets_df: DataFrame) -> DataFrame:


    exploded_customer_holdings_df = customer_portfolio_df.withColumn("Holding", F.explode("Holdings"))
    exploded_df = exploded_customer_holdings_df.select(
        "Portfolio Name",
        "FundName",
        "FundValue",
        F.col("Holding.HoldingIdentifier").alias("HoldingIdentifier"),
        F.col("Holding.AssetWeight").alias("AssetWeight")
    )

    flat_portfolio_with_public_markets = exploded_df.join(public_markets_df, "HoldingIdentifier", "left")

    joined_df = flat_portfolio_with_public_markets \
        .withColumn("OwnershipPercent", (F.col("AssetWeight") * F.col("FundValue")) / F.col("Value")) \
        .withColumn("FinancedScope1", F.col("OwnershipPercent") * F.col("Scope1")) \
        .withColumn("FinancedScope2", F.col("OwnershipPercent") * F.col("Scope2")) \
        .withColumn("FinancedScope3", F.col("OwnershipPercent") * F.col("Scope3"))

    portfolio_with_emission = joined_df.groupBy("Portfolio Name", "FundName", "FundValue").agg(
        F.collect_list(
            F.struct(
                F.col("HoldingIdentifier"),
                F.col("AssetWeight"),
                F.col("AssetClass"),
                F.col("OwnershipPercent"),
                F.col("FinancedScope1"),
                F.col("FinancedScope2"),
                F.col("FinancedScope3")
            )
        ).alias("Holdings")
    )

    return portfolio_with_emission