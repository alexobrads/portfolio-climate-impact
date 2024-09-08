from pyspark.sql import DataFrame

def add_equity_emissions(public_equity_df: DataFrame, public_universe_df: DataFrame) -> DataFrame:

    equity_with_emissions_df = public_equity_df.join(
        public_universe_df,
        public_equity_df["Holding Identifier"] == public_universe_df["Identifier"],
        how="left"
    )

    return equity_with_emissions_df

def add_bond_emissions(fixed_income_df: DataFrame, fixed_income_universe_df: DataFrame) -> DataFrame:

    bonds_with_emissions_df = fixed_income_df.join(
        fixed_income_universe_df,
        fixed_income_df["Holding Identifier"] == fixed_income_universe_df["Identifier"],
        how="left"
    )

    return bonds_with_emissions_df


def build_portfolio(
        portfolio_df: DataFrame,
        public_equity_df: DataFrame,
        fixed_income_df: DataFrame,
        public_universe_df: DataFrame,
        fixed_income_universe_df: DataFrame) -> DataFrame:


    equity_with_emissions_df = add_equity_emissions(public_equity_df, public_universe_df)
    bonds_with_emissions_df = add_bond_emissions(fixed_income_df, fixed_income_universe_df)

    portfolio_with_equity_df = portfolio_df.join(
        equity_with_emissions_df,
        portfolio_df["FundName"] == equity_with_emissions_df["Fund"],
        how="left"
    )

    portfolio_with_equity_and_bonds_df = portfolio_with_equity_df.join(
        bonds_with_emissions_df,
        portfolio_with_equity_df["FundName"] == bonds_with_emissions_df["Fund"],
        how="left"
    )


    return portfolio_with_equity_and_bonds_df
