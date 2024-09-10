from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def build_public_markets(
        public_universe_df: DataFrame,
        fixed_income_universe_df: DataFrame) -> DataFrame:

    public_universe_df = public_universe_df \
        .withColumn("BondType", lit(None).cast("string")) \
        .select(fixed_income_universe_df.columns)

    public_markets = public_universe_df.union(fixed_income_universe_df).withColumnRenamed("Identifier", "HoldingIdentifier")

    return public_markets