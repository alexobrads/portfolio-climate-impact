from pyspark.sql import SparkSession

from models.raw.fixed_income_universe import fixed_income_universe_schema
from models.raw.public_universe import public_universe_schema
import pandas as pd

from transformations.build_customer_portfolio import build_customer_portfolio


def main():

    spark = SparkSession.builder \
        .appName("PortfolioEmissionsPipeline") \
        .getOrCreate()

    # Load reference data
    df_public_universe = spark.read.csv("./data/public_universe.csv", schema=public_universe_schema, header=True)
    df_fixed_income_universe = spark.read.csv("./data/fixed_income_universe.csv", schema=fixed_income_universe_schema, header=True)

    # Cache the reference data
    df_public_universe.cache()
    df_fixed_income_universe.cache()

    portfolio_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./data/SamplePortfolio1/Portfolio.xlsx")

    public_equity_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./data/SamplePortfolio1/PublicEquityHoldings.xlsx")

    fixed_income_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("./data/SamplePortfolio1/FixedIncomeHoldings.xlsx")

    customer_portfolio = build_customer_portfolio(portfolio_df, public_equity_df, fixed_income_df)
    customer_portfolio.write.json("./output/customer_portfolio.json")

    spark.stop()

if __name__ == "__main__":
    main()