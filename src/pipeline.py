from pyspark.sql import SparkSession
from typing import List

from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType

from models.raw.fixed_income_universe import fixed_income_universe_schema
from models.raw.public_universe import public_universe_schema
from models.raw.portfolio import portfolio_mandatory_columns, portfolio_schema
from models.raw.holdings import holdings_schema, holdings_mandatory_columns
from utils import save_to_excel_with_aggregations
from transformations.build_public_markets import build_public_markets
from transformations.add_emissions import add_emissions
from validation.raw_data_validation import raw_data_validation
from transformations.build_customer_portfolio import build_customer_portfolio
import argparse


def main(base_directory: str, portfolios: List[str]):

    spark = SparkSession.builder \
        .appName("PortfolioEmissionsPipeline") \
        .getOrCreate()

    # Load reference data
    df_public_universe = spark.read.csv("./data/public_universe.csv", schema=public_universe_schema, header=True)
    df_fixed_income_universe = spark.read.csv("./data/fixed_income_universe.csv", schema=fixed_income_universe_schema, header=True)
    public_markets = build_public_markets(df_public_universe, df_fixed_income_universe)
    # Cache the reference data
    public_markets.cache()

    # Initialize empty DataFrames
    combined_portfolio_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), portfolio_schema)
    combined_public_equity_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), holdings_schema)
    combined_fixed_income_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), holdings_schema)

    for portfolio in portfolios:

        try:
            portfolio_df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f"{base_directory}/{portfolio}/Portfolio.xlsx") \
                .withColumn("Portfolio Name", lit(f"{portfolio}").cast("string"))

            if not raw_data_validation(portfolio_df, portfolio_schema, portfolio_mandatory_columns):
                raise ValueError(f"Schema mismatch for Portfolio in {portfolio}")

            public_equity_df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f"{base_directory}/{portfolio}/PublicEquityHoldings.xlsx") \
                .withColumn("Portfolio Name", lit(f"{portfolio}").cast("string"))

            if not raw_data_validation(public_equity_df, holdings_schema, holdings_mandatory_columns):
                raise ValueError(f"Schema mismatch for PublicEquityHoldings in {portfolio}")

            fixed_income_df = spark.read.format("com.crealytics.spark.excel") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(f"{base_directory}/{portfolio}/FixedIncomeHoldings.xlsx") \
                .withColumn("Portfolio Name", lit(f"{portfolio}").cast("string"))

            if not raw_data_validation(fixed_income_df, holdings_schema, holdings_mandatory_columns):
                raise ValueError(f"Schema mismatch for FixedIncomeHoldings in {portfolio}")

            combined_portfolio_df = combined_portfolio_df.union(portfolio_df)
            combined_public_equity_df = combined_public_equity_df.union(public_equity_df)
            combined_fixed_income_df = combined_fixed_income_df.union(fixed_income_df)

        except ValueError as e:
            print(f"************************************************")
            print(f"Skipping portfolio {portfolio} due to error: {e}")
            print(f"************************************************")
            spark.createDataFrame([(f"{portfolio}",)], StructType([StructField("portfolio_name", StringType(), True)])) \
                .write.json("./output/failed_portfolios.json")
            continue

    customer_portfolio = build_customer_portfolio(combined_portfolio_df, combined_public_equity_df, combined_fixed_income_df)
    customer_portfolio.write.mode("overwrite").json("./output/customer_portfolio.json")
    customer_profile_with_emissions = add_emissions(customer_portfolio, public_markets)
    customer_profile_with_emissions.write.mode("overwrite").json("./output/customer_profile_with_emissions.json")

    save_to_excel_with_aggregations(customer_profile_with_emissions, "./output/aggregated_results.xlsx")
    spark.stop()

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("base_directory", type=str, help="Base directory path")
    parser.add_argument("portfolios", type=str, nargs='+', help="List of portfolio names")

    args = parser.parse_args()
    main(args.base_directory, args.portfolios)