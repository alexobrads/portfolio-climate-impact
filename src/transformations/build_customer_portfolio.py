from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_flat_portfolio(
        portfolio_df: DataFrame,
        public_equity_df: DataFrame,
        fixed_income_df: DataFrame) -> DataFrame:

    holdings = public_equity_df.union(fixed_income_df)

    flat_portfolio_df = portfolio_df.join(
        holdings,
        portfolio_df["FundName"] == holdings["Fund"],
        how="left"
    ).drop("Fund")

    return flat_portfolio_df


def build_customer_portfolio(
        portfolio_df: DataFrame,
        public_equity_df: DataFrame,
        fixed_income_df: DataFrame) -> DataFrame:

    ##TODO CAN I ADD SCHEMA CHECK TO THE INPUTS
    """

    Transforms dataframes Portfolio, PublicEquityHoldings, and FixedIncomeHoldings into
    a customer portfolio view.

    Args:
        portfolio_df (pyspark.sql.DataFrame): The first integer.
        public_equity_df (pyspark.sql.DataFrame): The second integer.
        fixed_income_df (pyspark.sql.DataFrame): The second integer.


    Returns:
        pyspark.sql.DataFrame: A customer portfolio view with schema ../models/customer_portfolio.py

    """

    flat_portfolio_df = build_flat_portfolio(portfolio_df, public_equity_df, fixed_income_df)


    customer_portfolio_df = flat_portfolio_df.groupBy("FundName", "FundValue").agg(
        F.collect_list(
            F.struct(
                F.col("Holding Identifier").alias("HoldingIdentifier"),
                F.col("Asset Weight").alias("AssetWeight"),
                F.col("Asset Class").alias("AssetClass")
            )
        ).alias("Holdings")
    )

    return customer_portfolio_df