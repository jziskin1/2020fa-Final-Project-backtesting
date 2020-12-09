""" Webscraping tasks and functions """

from luigi import Task, Parameter, build
import dask.dataframe as dd
import yfinance as yf
from bs4 import BeautifulSoup
import requests
import pandas as pd

from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from csci_utils.luigi.dask.target import ParquetTarget
from utils import get_range


def scrape_summary_data(symbol):
    """Uses BeautifulSoup and Requests to webscrape summary data of stock from Yahoo Finance

    Parameters:
    symbol: The stock symbol (str)

    Returns:
    Two Pandas Dataframe in a Tuple with up to date summary statistics of stock
    """

    # Request data from yahoo finance
    r = requests.get(f"https://finance.yahoo.com/quote/{symbol}?p={symbol}")
    soup = BeautifulSoup(r.text, "lxml")

    # Create summary stats dictionary and add current price
    summary = {
        "Price": soup.find_all("div", {"class": "My(6px) Pos(r) smartphone_Mt(6px)"})[0]
        .find("span")
        .text
    }

    # Use html class strings from inspecting yahoo finance website
    class1_str = (
        "D(ib) W(1/2) Bxz(bb) Pend(12px) Va(t) ie-7_D(i) smartphone_D(b) smartphone_W(100%)"
        + " smartphone_Pend(0px) smartphone_BdY smartphone_Bdc($seperatorColor)"
    )

    # Locate summary statistics and add to summary dictionary
    tablesoup = soup.find_all("div", {"class": class1_str})[0].find_all("td")
    for i in range(7):
        summary[tablesoup[2 * i].text] = tablesoup[2 * i + 1].text

    # Return Pandas dataframe of summary stats
    return pd.DataFrame(summary, index=[0])


class GetHistoricalData(Task):
    """Uses the yfinance package to webscrape stock data from yahoo finance
    read in as a dask dataframe, and writes to parquet.

        Parameters:
        symbol: The stock symbol (str)
        interval: The unit of time per row (str)
    """

    # Task Parameters
    symbol = Parameter(default="AAPL")
    interval = Parameter(default="1d")

    # Target Output as descriptor
    output = TargetOutput(
        file_pattern="data/{symbol}/{interval}/rawdata/", target_class=ParquetTarget
    )

    def run(self):
        stock = yf.Ticker(self.symbol)
        df = stock.history(
            interval=self.interval, period=get_range(self.interval)
        ).iloc[:, :5]
        df = df.dropna()
        ddf = dd.from_pandas(df, chunksize=500)
        self.output().write_dask(ddf, compression="gzip")

if __name__ == "__main__":
    build([GetHistoricalData(symbol="AAPL", interval="1wk")], local_scheduler=True)
    print(scrape_summary_data("AAPL"))

