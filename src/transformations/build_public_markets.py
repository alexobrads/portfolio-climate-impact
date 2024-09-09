from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def build_public_markets(
        public_universe_df: DataFrame,
        fixed_income_universe_df: DataFrame) -> DataFrame:

    """

    Unions dataframes public_universe and fixed_income_universe into a single view of public markets

    Args:
        public_universe_df (pyspark.sql.DataFrame): The first integer.
        fixed_income_universe_df (pyspark.sql.DataFrame): The second integer.


    Returns:
        pyspark.sql.DataFrame: A full view of public markets with schema ../models/public_markets.py

    """

    public_universe_df = public_universe_df \
        .withColumn("BondType", lit(None).cast("string")) \
        .select(fixed_income_universe_df.columns)

    public_markets = public_universe_df.union(fixed_income_universe_df).withColumnRenamed("Identifier", "HoldingIdentifier")

    return public_markets