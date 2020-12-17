#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Luigi Tasks for Backtesting Different Stock Trading Strategies"""

# Python Libraries
from luigi import IntParameter, Parameter, BoolParameter, Task, LocalTarget, build
from csci_utils.luigi.task import TargetOutput, Requirement, Requires

# Local Imports
from .scrape import GetHistoricalData
from .indicators import calculate_ema, calculate_sma, calculate_macd, calculate_rsi, calculate_sto_osc
from .utils import combine_series, evaluate_crossover, evaluate_profit, evaluate_osc


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
        cumulative % profit and win/loss ratio.
    """

    # Task Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    short = BoolParameter(default=False)
    use_simple_ma = BoolParameter(default=False)

    def output(self):
        # Change filepattern depending on whether MA is simple or exponential
        file_pattern = f"data/{self.symbol}/{self.interval}/short_{self.short}/E{self.__class__.__name__}({self.slow},{self.fast})/trading_stats.csv"
        if self.use_simple_ma:
            file_pattern = f"data/{self.symbol}/{self.interval}/short_{self.short}/S{self.__class__.__name__}({self.slow},{self.fast})/trading_stats.csv"
        return LocalTarget(file_pattern)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Use either SMA or EMA depending on parameter
        if self.use_simple_ma:
            ma_func = calculate_sma
        else:
            ma_func = calculate_ema

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
        cumulative % profit and win/loss ratio.
    """

    # Task Parameters
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    signal = IntParameter(default=9)
    short = BoolParameter(default=False)

    # Target Output as descriptor
    output = TargetOutput(file_pattern="data/{symbol}/{interval}/short_{short}/{task.__class__.__name__}({slow},{fast},{signal})/trading_stats.csv",
                          target_class=LocalTarget)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns="Close")

        # Create MACD and Signal lines
        macd = calculate_macd(df, self.fast, self.slow)
        signal = calculate_ema(macd, self.signal)

        # Combine the Close and SMA series into a dataframe
        df = combine_series(df, MACD=macd, Signal=signal)

        # Create Action column that uses strategy to determine when to Buy, Sell, or Wait
        evaluate_crossover(df, "MACD", "Signal")

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


class RSI_OverSold(Backtest):
    """This strategy involves buying (or covering) when the RSI passes the lower bound
    and selling (or shorting) when the RSI falls below the upper bound.

    Parameters:
        period: Number of time units the RSI is calculated over (int)
        lower: The lower bound of the RSI. Signals a buy when the RSI passes above this value (int)
        upper: The upper bound of the RSI. Signals a sell when the RSI passes below this value (int)
        short: Defaults to False. True if you want to observe a shorting strategy.

    output:
        Dataframe showing every trade made using this strategy, % profit per trade,
        cumulative % profit and win/loss ratio.
    """
    # Task Parameters
    period = IntParameter(default=14)
    lower = IntParameter(default=30)
    upper = IntParameter(default=70)
    short = BoolParameter(default=False)

    # Target Output as descriptor
    output = TargetOutput(file_pattern="data/{symbol}/{interval}/short_{short}/{task.__class__.__name__}({period},{lower},{upper})/trading_stats.csv",
                          target_class=LocalTarget)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns=["Open","Close"])

        # Calculate RSI Line
        rsi = calculate_rsi(df, self.period)

        # Combine the Open, Close, and RSI series into a dataframe
        df = combine_series(df, RSI=rsi)

        # Evaluate RSI
        evaluate_osc(df, self.lower, self.upper, short=False)

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)


class Stochastic_Crossover(Backtest):
    """This strategy involves buying (or covering) when the Stocastic reaches a higher low
     and then passes the lower bound and selling (or shorting) when the Stocastic reaches a
     lower high and falls below the upper bound.

    Parameters:
        period: Number of time units the Stocastic is calculated over (int)
        lower: The lower bound of the Stocastic. Signals a buy when the RSI passes above this value (int)
        upper: The upper bound of the Stocastic. Signals a sell when the RSI passes below this value (int)
        short: Defaults to False. True if you want to observe a shorting strategy.

    output:
        Dataframe showing every trade made using this strategy, % profit per trade,
        cumulative % profit and win/loss ratio.
    """
    # Task Parameters
    period = IntParameter(default=14)
    lower = IntParameter(default=20)
    upper = IntParameter(default=80)
    short = BoolParameter(default=False)

    # Target Output as descriptor
    output = TargetOutput(file_pattern="data/{symbol}/{interval}/short_{short}/{task.__class__.__name__}({period},{lower},{upper})/trading_stats.csv",
                          target_class=LocalTarget)

    def run(self):
        # Use dask to read in just the "Close" column
        df = self.input()["history"].read_dask(columns=["High", "Low", "Close"])

        # Calculate Stochastic Line
        stoc = calculate_sto_osc(df, self.period)

        # Combine the Open, Close, and RSI series into a dataframe
        df = combine_series(df, Stochastic=stoc)

        # Evaluate Stochastic
        evaluate_osc(df, self.lower/100, self.upper/100, short=False)

        # Create Condensed dataframe of Profit on trade, Cumulative profit, and win/loss ratio
        stats = evaluate_profit(df, short=self.short)
        with self.output().open("w") as file_path:
            stats.to_csv(file_path)
