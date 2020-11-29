from scrape import GetHistoricalData
from luigi import IntParameter, Parameter, BoolParameter, Task, LocalTarget, build
import indicators as ind
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from utils import combine_series, evaluate_crossover, evaluate_profit


class Backtest(Task):
    symbol = Parameter(default="AAPL")
    interval = Parameter(default="1d")
    short = BoolParameter(default=False)
    output = TargetOutput(
        file_pattern="data/{symbol}/{interval}/{task.__class__.__name__}/trading_stats.csv",
        target_class=LocalTarget,
    )
    # TODO: Figure how to add shorting to name


class SMA_Divergence(Backtest):
    # Luigi Task Requirements as Descriptors
    requires = Requires()
    history = Requirement(GetHistoricalData)

    # Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    short = BoolParameter(default=False)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Create the slow and fast SMAs
        slow_sma = ind.calculate_sma(df, self.slow)
        fast_sma = ind.calculate_sma(df, self.fast)

        # Combine the Close and SMA series into a dataframe
        df = combine_series(df, Slow=slow_sma, Fast=fast_sma)

        # Create Action column that uses strategy to determine when to Buy, Sell, or Wait
        evaluate_crossover(df)

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


class EMA_Divergence(Backtest):
    # Luigi Task Requirements as Descriptors
    requires = Requires()
    history = Requirement(GetHistoricalData)

    # Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    short = BoolParameter(default=False)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Create the slow and fast EMAs
        slow_sma = ind.calculate_ema(df, self.slow)
        fast_sma = ind.calculate_ema(df, self.fast)

        # Combine the Close and EMA series into a dataframe
        df = combine_series(df, Slow=slow_sma, Fast=fast_sma)

        # Create Action column that uses strategy to determine when to Buy, Sell, or Wait
        evaluate_crossover(df)

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


class MACD_Signal_Divergence(Backtest):
    # Luigi Task Requirements as Descriptors
    requires = Requires()
    history = Requirement(GetHistoricalData)

    # Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    signal = IntParameter(default=9)
    short = BoolParameter(default=False)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Create MACD and Signal lines
        macd = ind.calculate_macd(df, self.fast, self.slow)
        signal = ind.calculate_ema(macd, self.signal)

        # Combine the Close and SMA series into a dataframe
        df = combine_series(df, MACD=macd, Signal=signal)

        # Create Action column that uses strategy to determine when to Buy, Sell, or Wait
        evaluate_crossover(df, "MACD", "Signal")

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


if __name__ == "__main__":
    build([MACD_Signal_Divergence(symbol="BA", interval="1h")], local_scheduler=True)
    # build([SMAcross(symbol='MSFT', interval="1d", short=True)], local_scheduler=True)
