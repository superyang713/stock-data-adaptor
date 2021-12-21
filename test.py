import os

import numpy as np
import pandas as pd

from iex.stocks import Stock
from scipy.stats import percentileofscore as ps


os.environ["IEX_OUTPUT_FORMAT"] = "pandas"
os.environ["IEX_TOKEN"] = "Tpk_c4b4d59b011048feb01f7063e39502b7"


def main():
    ticks = get_sp_stock_ticks()
    quotes = get_quote(ticks)
    stats = get_stats(ticks)

    columns = [
        "marketCap",
        "latestPrice",
        "year1ChangePercent",
        "month6ChangePercent",
        "month3ChangePercent",
        "month1ChangePercent",
        "priceToBook",
        "priceToSales",
        "enterpriseValue",
        "EBITDA",
        "grossProfit",
    ]
    df = pd.concat([quotes, stats], axis=1, join="inner")[columns]
    df = df.apply(lambda x: pd.to_numeric(x), axis=1)
    df.dropna(inplace=True)

    hqm = get_hqm_score(df, top_n=50)
    rv = get_rv_score(df, top_n=50)

    portfolio_size = 5000000
    df = pd.concat([hqm, rv, df["latestPrice"]], axis=1, join="inner")
    df = get_equal_weight_fund_index(portfolio_size, df)
    print(df)


def get_rv_score(df, top_n=50):
    """
    The RV Score stands for robust value. It is a composite basket of valuation
    metrics, typically used to build robust quantitative value strategies.
    In this function, the RV score is the arithmetic mean of the 4 following
    percentile scores:
        1. Price-to-earnings ratio
        2. Price-to-book ratio
        3. Price-to-sales ratio
        4. Enterprise Value divided by Earnings Before Interest, Taxes,
           Depreciation, and Amortization (EV/EBITDA)
        5. Enterprise Value divided by Gross Profit (EV/GP)

    Parameters
    ----------
    df: pd.DataFrame
        Must have to the following feature, which can be obtained from IEX
        advanced stats API and quote API:
            "priceToBook"
            "priceToSales"
            "enterpriseValue"
            "EBITDA":
            "grossProfit":
    columns: list(str)
        A list of names of the six columns required
    top_n: int
        The number of top stocks with the highest HQM score.

    Return:
    ----------
    df: pd.DataFrame
        A new column "rv_score" is added to the input df.
        Other columns involved in the calculation are added to the input df.
    """
    df["ev_to_ebitda"] = df["enterpriseValue"] / df["EBITDA"]
    df["ev_to_gross_profit"] = df["enterpriseValue"] / df["grossProfit"]

    columns = [
        "priceToBook",
        "priceToSales",
        "ev_to_ebitda",
        "ev_to_gross_profit",
    ]

    result = calculate_percentile_score(df, columns, top_n)
    result = result.rename("rv_score")
    return result


def get_hqm_score(df, top_n=50):
    """
    The HQM Score is the arithmetic mean of the 4 momentum percentile
    scores of 1 month, 3 months, 6 months, and 1 year price returns.

    Parameters
    ----------
    df: pd.DataFrame
        Must have to the following features, which can be obtianed from IEX key
        stats API:
            "year1ChangePercent": 1 year price return percentage.
            "month6ChangePercent": 6 month price return percentage.
            "month3ChangePercent": 3 month price return percentage.
            "month1ChangePercent": 1 month price return percentage.
    columns: list(str)
        A list of names of the four columns requried
    top_n: int
        The number of top stocks with the highest HQM score.

    Return:
    ----------
    df: pd.DataFrame
        A new column "hqm" is added to the input df.
        Other columns involved in the calculation are added to the input df.
    """
    columns = [
        "year1ChangePercent",
        "month6ChangePercent",
        "month3ChangePercent",
        "month1ChangePercent",
    ]
    result = calculate_percentile_score(df, columns, top_n)
    result = result.rename("hqm_score")
    return result


def calculate_percentile_score(df, columns, top_n):
    percentile_columns = [f"{col}_percentile" for col in columns]
    for column, percentile_column in zip(columns, percentile_columns):
        df[percentile_column] = df[column].apply(
            lambda x: ps(df[column], x) / 100)

    result = df[percentile_columns].apply(np.mean, axis=1)
    result = result.sort_values(ascending=False)[:top_n]
    return result


def get_equal_weight_fund_index(portfolio_size, df):
    """
    Parameters
    ----------
    df: pd.DataFrame
        Must have to the following feature:
        "latestPrice": the latest market stcok price
    portfolio_size: float or int
        the total amount of investment money

    Return:
    ----------
    df: pd.DataFrame
        A new column "shares_to_buy" is added to the input df. The number of
        shares to buy for each stock so that all investment money is equalized
        on each stock.
    """
    position_size = portfolio_size / len(df.index)
    df["shares_to_buy"] = position_size / df["latestPrice"]
    df["shares_to_buy"] = df["shares_to_buy"].apply(int)
    return df


def get_quote(ticks):
    symbol_groups = list(chunks(ticks, 100))

    df = pd.DataFrame()
    for symbol_group in symbol_groups:
        stock = Stock(symbol_group)
        result = stock.get_quote()
        df = pd.concat([df, result])

    return df


def get_stats(ticks):
    symbol_groups = list(chunks(ticks, 100))

    df = pd.DataFrame()
    for symbol_group in symbol_groups:
        stock = Stock(symbol_group)
        result = stock.get_advanced_stats()
        df = pd.concat([df, result])

    return df


def get_sp_stock_ticks():
    """
    Return:
    ----------
    ticks: list
        A list of ticks (symbols) of fortune 500 companies.
    """
    ticks = pd.read_csv("./sp_500_stocks.csv")["Ticker"]
    ticks = list(ticks)
    return ticks


def chunks(lst, n):
    """
    Yield successive n-sized chunks from lst.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


if (__name__ == "__main__"):
    main()
