from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def build_flat_portfolio(
        portfolio_df: DataFrame,
        public_equity_df: DataFrame,
        fixed_income_df: DataFrame) -> DataFrame:

    holdings = public_equity_df.union(fixed_income_df).withColumnRenamed("Portfolio Name", "PN")

    flat_portfolio_df = portfolio_df.join(
        holdings,
        (portfolio_df["FundName"] == holdings["Fund"]) & (portfolio_df["Portfolio Name"] == holdings["PN"]),
        how="left"
    ).drop("Fund")

    return flat_portfolio_df


def build_customer_portfolio(
        portfolio_df: DataFrame,
        public_equity_df: DataFrame,
        fixed_income_df: DataFrame) -> DataFrame:

    """

    Transforms dataframes Portfolio, PublicEquityHoldings, and FixedIncomeHoldings into
    a customer portfolio view.

    Args:
        portfolio_df (pyspark.sql.DataFrame): df containing portfolio data.
        public_equity_df (pyspark.sql.DataFrame): df containing equity holdings.
        fixed_income_df (pyspark.sql.DataFrame): df containing bond holdings.


    Returns:
        pyspark.sql.DataFrame: A customer portfolio view with schema ../models/customer_portfolio.py

    """

    flat_portfolio_df = build_flat_portfolio(portfolio_df, public_equity_df, fixed_income_df)

    customer_portfolio_df = flat_portfolio_df.groupBy("Portfolio Name", "FundName", "FundValue").agg(
        F.collect_list(
            F.struct(
                F.col("Holding Identifier").alias("HoldingIdentifier"),
                F.col("Asset Weight").alias("AssetWeight"),
                F.col("Asset Class").alias("AssetClass")
            )
        ).alias("Holdings")
    )

    return customer_portfolio_df