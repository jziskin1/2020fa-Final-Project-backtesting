import pandas as pd


def get_range(interval):
    """ Takes interval as a string, returns range as string """
    interval_to_range = {
        "1m": "7d",
        "2m": "60d",
        "5m": "60d",
        "15m": "60d",
        "1h": "2y",
        "1d": "5y",
        "5d": "10y",
        "1wk": "10y",
        "1mo": "10y",
        "3mo": "10y",
    }
    try:
        return interval_to_range[interval]
    except KeyError:
        raise KeyError(
            "Interval must be one of '1m', '2m', '5m', '15m', '1h', '1d', '5d', '1wk', '1mo', '3mo'"
        )


def combine_series(main, **kwargs):
    """Combine series of different lengths by trimming from the top
    Move to utilities"""
    length = min(len(x) for x in list(kwargs.values()))
    main = main.compute()[-length:]
    colnames = [main.name]
    cols = [main]
    for name, vector in kwargs.items():
        new_vec = vector[-length:]
        colnames.append(name)
        cols.append(new_vec)
        new_vec.index = main.index

    df_dict = {colname: col for colname, col in zip(colnames, cols)}
    df = pd.DataFrame(df_dict)
    return df


def evaluate_crossover(df, buy_col="Fast", sell_col="Slow", diff=0):
    buy_vec = pd.array(df[buy_col])
    sell_vec = pd.array(df[sell_col])
    difference = buy_vec - sell_vec
    action = ["Wait"]
    for idx in range(len(buy_vec) - 1):
        if difference[idx + 1] * difference[idx] > 0:
            action.append("Wait")
        else:
            action += ["SELL" if difference[idx + 1] < 0 else "BUY"]

    df["Action"] = action
    return difference, action


def evaluate_profit(df, short=False):
    # Create Dataframe of just buying and selling
    adjusted_df = df.loc[
        (df["Action"] == "BUY") | (df["Action"] == "SELL"), ["Close", "Action"]
    ]

    # Read in Close and Action columns as arrays
    close = pd.array(adjusted_df["Close"])
    action = pd.array(adjusted_df["Action"])

    # Initialize lists and variables used to collect stats
    percent_profit = []
    cumulative_profit_lst = []
    win_loss_ratio = []
    cumulative_profit = 0
    wins = 0

    # Disregard action in first row if it does not coincide with strategy
    if (short == False and action[0] == "SELL") or (short and action[0] == "BUY"):
        close = close[1:]
        action = action[1:]
        for lst in (percent_profit, cumulative_profit_lst, win_loss_ratio):
            lst.append("-")

    # Use s to adjust formula below for shorting
    s = 0
    if short:
        s = 1

    # Iterate through rows and collect trading results
    for idx in range(len(close) // 2):
        percent_increase = (close[2 * idx + 1 - s] - close[2 * idx + s]) / close[
            2 * idx + s
        ]
        percent_profit += ["-", percent_increase]
        cumulative_profit += percent_profit[-1]
        cumulative_profit_lst += ["-", cumulative_profit]
        if percent_increase > 0:
            wins += 1
        win_loss_ratio += ["-", wins / (idx + 1)]

    # Disregard final action if it puts trader mid trade
    if len(close) % 2 == 1:
        for lst in (percent_profit, cumulative_profit_lst, win_loss_ratio):
            lst.append("-")

    # Add columns
    adjusted_df["% Profit on Trade"] = percent_profit
    adjusted_df["Cumulative % Profit"] = cumulative_profit_lst
    adjusted_df["Win/Loss Ratio"] = win_loss_ratio

    return adjusted_df
