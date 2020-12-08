""" Technical indicators used in technical analysis
"""

import pandas as pd
import numpy as np


def calculate_sma(prices, period):
    """Calculate Simple Moving Average as an array

    ::param Series, list or Array of prices
    ::param int period: Number of time periods to take sma over

    ::returns Simple Moving Average
    ::rtype: Pandas Series (1D)"""
    prices = pd.array(prices)
    return pd.Series(
        [np.mean(prices[idx : period + idx]) for idx in range(len(prices) - period + 1)]
    )


def calculate_ema(prices, period, smoother=2):
    """Calculate Exponential Moving Average as an array

    ::param Series, list or Array of prices
    ::param int period: Number of time periods to take ema over
    ::param int smoother: Value of smoothing factor

    ::returns Exponential Moving Average
    ::rtype: Pandas Series (1D)"""
    prices = pd.array(prices)

    sma = sum(prices[:period]) / period
    multiplier = smoother / (1 + period)
    result = [sma]

    for price in prices[period:]:
        last = result[-1]
        ema = price * multiplier + last * (1 - multiplier)
        result.append(ema)

    return pd.Series(result)


def calculate_macd(prices, fast_period=12, slow_period=26, smoother=2):
    """Calculate Moving Average Convergence Divergence

    ::param Series, list or Array of prices
    ::param int period1: Number of time periods to take first ema over
    ::param int period2: Number of time periods to take second ema over
    ::param int smoother: Value of smoothing factor

    ::returns Moving Average Convergence Divergence
    ::rtype: Pandas Series (1D)"""
    if fast_period >= slow_period:
        raise ValueError("period2 must be greater than period1")

    slow_ema = pd.array(calculate_ema(prices, slow_period, smoother=smoother))
    fast_ema = pd.array(calculate_ema(prices, fast_period, smoother=smoother))[
        slow_period - fast_period :
    ]

    return pd.Series(fast_ema - slow_ema)


def calculate_sto_osc(dataframe, period):
    """Calculate Stochastic Oscillator

    ::param Dataframe of Closing, High, and Low prices
    ::param int period: Number of time periods to calculate over

    ::returns Calculate Stochastic Oscillator
    ::rtype: Pandas Series (1D)"""

    close = dataframe["Close"]
    high = dataframe["High"]
    low = dataframe["Low"]

    def Stochastic(idx):
        Max = max(high[idx: period + idx])
        Min = min(low[idx: period + idx])
        return (close[period + idx - 1] - Min) / (Max - Min)

    return pd.Series([Stochastic(i) for i in range(len(close) - period)])


def calculate_rsi(dataframe, period=14):
    """Calculate Relative Strength Index as an array

    ::param Dataframe or Series of Closing prices
    ::param int period: Number of time periods to take calculate over

    ::returns Relative Strength Index
    ::rtype: ndarray (1D)"""

    # Read in open and closing prices
    open = dataframe["Open"]
    close = dataframe["Close"]

    # calculate change in prices.
    # If change is positive add value to up_change add 0 to down_change
    # If change is negative add value to down_change add 0 to up_change
    change = [(close[i] - open[i]) for i in range(len(open))]
    up_change = [val if val > 0 else 0 for val in change]
    down_change = [abs(val) if val < 0 else 0 for val in change]

    # Calculate simple moving average of up_chance and down_change for first period
    up_smma = [np.mean(up_change[:period])]
    down_smma = [np.mean(down_change[:period])]

    # Calculate simple modified moving average for up_change and down_change
    for i in range(period, len(open)):
        if change[i] > 0:
            up_smma.append((change[i] + up_smma[-1] * (period - 1)) / period)
            down_smma.append((down_smma[-1] * (period - 1)) / period)

        else:
            up_smma.append((up_smma[-1] * (period - 1)) / period)
            down_smma.append((abs(change[i]) + down_smma[-1] * (period - 1)) / period)

    # Calculate RS and RSI
    RS = pd.array(up_smma) / pd.array(down_smma)
    RSI = 100 - 100 / (1 + RS)
    return pd.Series(RSI)
