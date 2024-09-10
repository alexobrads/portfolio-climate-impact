from typing import List

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import functions as F


def calculate_aggregations(df: DataFrame) -> List[DataFrame]:

    exploded_df = df.withColumn("Holding", F.explode("Holdings"))

    # Aggregate by FundName
    fund_agg_df = exploded_df.groupBy("Portfolio Name", "FundName").agg(
        (spark_sum("Holding.FinancedScope1") + spark_sum("Holding.FinancedScope2") + spark_sum("Holding.FinancedScope3")) \
            .alias("TotalFinancedEmissions")
    )

    # Aggregate by AssetClass
    asset_class_agg_df = exploded_df.groupBy("Portfolio Name", "Holding.AssetClass").agg(
        (spark_sum("Holding.FinancedScope1") + spark_sum("Holding.FinancedScope2") + spark_sum("Holding.FinancedScope3")) \
            .alias("TotalFinancedEmissions")
    )

    # Aggregate for total portfolio
    total_agg_df = exploded_df.groupBy("Portfolio Name").agg(
        (spark_sum("Holding.FinancedScope1") + spark_sum("Holding.FinancedScope2") + spark_sum("Holding.FinancedScope3")) \
            .alias("TotalFinancedEmissions")
    )


    fund_agg_pd = fund_agg_df.toPandas()
    asset_class_agg_pd = asset_class_agg_df.toPandas()
    total_agg_pd = total_agg_df.toPandas()

    return [fund_agg_pd, asset_class_agg_pd, total_agg_pd]


def save_to_excel_with_aggregations(df: DataFrame, output_file: str):

    aggregated_dfs = calculate_aggregations(df)

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        aggregated_dfs[0].to_excel(writer, sheet_name='ByFund', index=False)
        aggregated_dfs[1].to_excel(writer, sheet_name='ByAssetClass', index=False)
        aggregated_dfs[2].to_excel(writer, sheet_name='TotalPortfolio', index=False)

