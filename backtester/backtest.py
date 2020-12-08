# Python Libraries
from scrape import GetHistoricalData
from luigi import IntParameter, Parameter, BoolParameter, Task, LocalTarget, build
from csci_utils.luigi.task import TargetOutput, Requirement, Requires

# Local Imports
import indicators as ind
from utils import combine_series, evaluate_crossover, evaluate_profit


class Backtest(Task):
    """Abstract Backtest Task containing parameters, requires, and output common to all tasks

    Parameters:
        symbol: The stock symbol (str)
        interval: The unit of time per row (str)
        short: Defaults to False. True if you want to observe a shorting strategy.
    """

    requires = Requires()
    history = Requirement(GetHistoricalData)
    symbol = Parameter(default="AAPL")
    interval = Parameter(default="1d")
    short = BoolParameter(default=False)
    output = TargetOutput(
        file_pattern="data/{symbol}/{interval}/{task.__class__.__name__}/trading_stats.csv",
        target_class=LocalTarget,
    )
    # TODO: Figure how to add shorting to name


class MA_Divergence(Backtest):
    """This strategy involves buying (or covering) when the fast MA passes the slow MA
    and selling (or shorting) when the fast MA falls below the slow MA

    Parameters:
        slow: Number of time units the slow SMA is calculated over (int)
        fast: Number of time units the slow SMA is calculated over (int)
        short: Defaults to False. True if you want to observe a shorting strategy.
        use_simple_ma: Defaults to False. This strategy will calculate the exponential
        moving average unless this is set to True, in which case it will calculate the
        simple moving average

    output:
        Dataframe showing every trade made using this strategy, % profit per trade,
        cumulative % profit and profit/loss ratio.
    """

    # Task Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    short = BoolParameter(default=False)
    use_simple_ma = BoolParameter(default=False)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Use either SMA or EMA depending on parameter
        if self.use_simple_ma:
            ma_func = ind.calculate_sma
        else:
            ma_func = ind.calculate_ema

        # Create the slow and fast MAs
        slow_ma = ma_func(df, self.slow)
        fast_ma = ma_func(df, self.fast)

        # Combine the Close and MA series into a dataframe
        df = combine_series(df, Slow=slow_ma, Fast=fast_ma)

        # Create Action column that uses strategy to determine when to Buy, Sell, or Wait
        evaluate_crossover(df)

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


class MACD_Signal_Divergence(Backtest):
    """This strategy involves buying (or covering) when the fast MACD line passes the
    signal line and selling (or shorting) when the MACD line falls below the signal line.

    Parameters:
        slow: Number of time units the slow EMA for MACD is calculated over (int)
        fast: Number of time units the slow EMA for MACD is calculated over (int)
        short: Defaults to False. True if you want to observe a shorting strategy.

    output:
        Dataframe showing every trade made using this strategy, % profit per trade,
        cumulative % profit and profit/loss ratio.
    """

    # Task Parameters
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


class RSI_Failure_Swings(Backtest):
    # Task Parameters
    period = IntParameter(default=14)
    short = BoolParameter(default=False)


if __name__ == "__main__":
    # build([MACD_Signal_Divergence(symbol="BA", interval="1h")], local_scheduler=True)
    build([MA_Divergence(symbol="BA", interval="1d")], local_scheduler=True)
