""" Luigi tasks and functions used to webscrape stock data to use in
backtesting and final report

GetHistoricalData: Luigi Task that downloads


"""

from luigi import Task, Parameter, build
import dask.dataframe as dd
import yfinance as yf

from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from csci_utils.luigi.dask.target import ParquetTarget
from utils import get_range


class GetHistoricalData(Task):
    # TODO: Add parameter based on strategy to only download needed columns.
    # TODO: Add date function to update
    # TODO: Salted

    symbol = Parameter(default="AAPL")
    interval = Parameter(default="1d")
    output = TargetOutput(
        file_pattern="data/{symbol}/{interval}/rawdata/", target_class=ParquetTarget
    )

    def run(self):
        stock = yf.Ticker(self.symbol)
        df = stock.history(
            interval=self.interval, period=get_range(self.interval)
        ).iloc[:, :5]
        ddf = dd.from_pandas(df, npartitions=1)
        self.output().write_dask(ddf, compression="gzip")


if __name__ == "__main__":
    build([GetHistoricalData(symbol="BA", interval="1d")], local_scheduler=True)
