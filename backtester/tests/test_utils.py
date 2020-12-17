#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for utils.py"""

import os
from tempfile import TemporaryDirectory
from unittest import TestCase, main
import pandas as pd
import dask.dataframe as dd

from backtester.utils import get_range, get_strategy_text, combine_series, evaluate_profit
from backtester.utils import evaluate_crossover, evaluate_osc, recommendation


class TestUtils(TestCase):
    def test_get_range(self):
        # Ensure all possible interval values return correct ranges
        self.assertTrue(get_range("1m"), "7d")
        self.assertTrue(get_range("2m"), "60d")
        self.assertTrue(get_range("5m"), "60d")
        self.assertTrue(get_range("15m"), "60d")
        self.assertTrue(get_range("1h"), "2y")
        self.assertTrue(get_range("1d"), "5y")
        self.assertTrue(get_range("5d"), "10y")
        self.assertTrue(get_range("1wk"), "10y")
        self.assertTrue(get_range("1mo"), "10y")
        self.assertTrue(get_range("3mo"), "10y")

        # Ensure proper errors are raised if input is not valid
        with self.assertRaises(TypeError):
            get_range(3)
        with self.assertRaises(KeyError):
            get_range("1 Week")

    def test_get_strategy_text(self):
        # Assert strings of text are returned when proper strategy is inputed
        self.assertTrue(isinstance(get_strategy_text("MACD_Signal_Divergence"), str))
        self.assertTrue(isinstance(get_strategy_text("MA_Divergence"), str))
        self.assertTrue(isinstance(get_strategy_text("Stochastic_Crossover"), str))
        self.assertTrue(isinstance(get_strategy_text("RSI_OverSold"), str))

        # Ensure proper errors are raised if input is not valid
        with self.assertRaises(TypeError):
            get_strategy_text(3)
        with self.assertRaises(KeyError):
            get_strategy_text("Not a real strategy")

    def test_combine_series(self):
        # Ensure combination of dask index results in proper index, lengths, and vectors
        test_index = ["a","b","c","d","e","f","g"]
        main_series = pd.Series(data=[1, 2, 3, 4, 5, 6, 7], index=test_index, name="Test Series")
        dask_series = dd.from_pandas(main_series, npartitions=1)
        join_series_1 = pd.Series(data=[1,2,3,4,5])
        join_series_2 = pd.array([1,2,3,4])
        joined_df = combine_series(dask_series, t2=join_series_1, t3=join_series_2)

        self.assertEqual(list(joined_df.index), ["d","e","f","g"])
        self.assertEqual(list(joined_df["Test Series"]), [4 ,5, 6, 7])
        self.assertEqual(list(joined_df["t2"]), [2, 3, 4, 5])
        self.assertEqual(list(joined_df["t3"]), [1, 2, 3, 4])
        self.assertEqual(len(joined_df), 4)

    def test_evaluate_crossover(self):
        # Ensure function correctly creates buy and sell signals and adds it to dataframe
        test_data = {"test_Fast":[30,40,50,60,60,40,30], "test_Slow":[42,44,46,48,48,44,42]}
        test_df = pd.DataFrame(test_data)
        evaluate_crossover(test_df, buy_col="test_Fast", sell_col="test_Slow")

        self.assertEqual(list(test_df["Action"]), ["Wait", "Wait", "BUY", "Wait", "Wait", "SELL", "Wait"])

    def test_evaluate_osc(self):
        # Ensure function correctly creates buy and sell signals and adds it to dataframe for RSI
        test_data = {"RSI": [20,35,50,80,65,40,20,35]}
        test_df = pd.DataFrame(test_data)
        evaluate_osc(test_df, 30, 70, short=False)

        self.assertEqual(list(test_df["Action"]), ["Wait", "BUY", "Wait", "Wait", "SELL", "Wait", "Wait", "BUY"])

        # Ensure function correctly creates buy and sell signals and adds it to dataframe for RSI shorting
        test_df2 = pd.DataFrame(test_data)
        evaluate_osc(test_df2, 30, 70, short=True)

        self.assertEqual(list(test_df2["Action"]), ["Wait", "Wait", "Wait", "Wait", "SELL", "Wait", "Wait", "BUY"])

        # Ensure function correctly creates buy and sell signals and adds it to dataframe for Stochastic
        test_data = {"Stochastic": [20,35,50,80,65,40,20,35]}
        test_df3 = pd.DataFrame(test_data)
        evaluate_osc(test_df3, 30, 70, short=False)

        self.assertEqual(list(test_df3["Action"]), ["Wait", "BUY", "Wait", "Wait", "SELL", "Wait", "Wait", "BUY"])

        # Ensure function correctly creates buy and sell signals and adds it to dataframe for Stochastic shorting
        test_df4 = pd.DataFrame(test_data)
        evaluate_osc(test_df4, 30, 70, short=True)

        self.assertEqual(list(test_df4["Action"]), ["Wait", "Wait", "Wait", "Wait", "SELL", "Wait", "Wait", "BUY"])


    def test_evaulate_profit(self):
        # Ensure function correctly creates captures profit and returns new dataframe
        test_data = {"Close": [20, 50, 100, 80, 60, 40, 10, 30, 35],
                     "Action": ["Wait", "BUY", "Wait", "SELL", "Wait", "BUY", "Wait", "SELL", "BUY"]}
        test_df = pd.DataFrame(test_data)
        stats_df = evaluate_profit(test_df, short=False)

        self.assertEqual(list(stats_df["Close"]), [50,80,40,30])
        self.assertEqual(list(stats_df["% Profit on Trade"]), ["-",60.0,"-",-25.0])
        self.assertEqual(list(stats_df["Cumulative % Profit"]), ["-",60.0,"-",35.0])
        self.assertEqual(list(stats_df["Win/Loss Ratio"]), ["-",1.0,"-",0.5])

        # Ensure function work for shorting
        stats_df2 = evaluate_profit(test_df, short=True)

        self.assertEqual(list(stats_df2["Close"]), [80,40,30,35])
        self.assertEqual(list(stats_df2["% Profit on Trade"]), ["-",50.0,"-",-16.67])
        self.assertEqual(list(stats_df2["Cumulative % Profit"]), ["-",50.0,"-",33.33])
        self.assertEqual(list(stats_df2["Win/Loss Ratio"]), ["-",1.0,"-",0.5])

    def test_recommendation(self):
        # Ensure create recommendations are created based on performance
        test_data = {"Close": [50,80,40,30],
                    "Action": ["BUY", "SELL", "BUY", "SELL"],
                    "% Profit on Trade": ["-",50.0,"-",-16.67],
                    "Cumulative % Profit": ["-",50.0,"-",33.33],
                    "Win/Loss Ratio": ["-",1.0,"-",0.5]}
        test_df = pd.DataFrame(test_data)
        with TemporaryDirectory() as tmpdir:
            fp = os.path.join(tmpdir, "test.csv")
            test_df.to_csv(fp)
            self.assertEqual(recommendation(fp), "After 2 trades, this strategy resulted in an overall profit of 33.33% with a win/loss ratio of 50.0%. Over the same period of time, the price of this stock rose -40.0%. Since this strategy outperformed buying and holding long term, this strategy receives our recommendation.")


        test_data = {"Close": [50,40,80,100],
                    "Action": ["BUY", "SELL", "BUY", "SELL"],
                    "% Profit on Trade": ["-",50.0,"-",-16.67],
                    "Cumulative % Profit": ["-",50.0,"-",33.33],
                    "Win/Loss Ratio": ["-",1.0,"-",0.5]}
        test_df = pd.DataFrame(test_data)
        with TemporaryDirectory() as tmpdir:
            fp = os.path.join(tmpdir, "test.csv")
            test_df.to_csv(fp)
            self.assertEqual(recommendation(fp), "After 2 trades, this strategy resulted in an overall profit of 33.33% with a win/loss ratio of 50.0%. Over the same period of time, the price of this stock rose 100.0%. Because the profit from this strategy is significantly lower than just buying and holding, this strategy does not receive our recommendation.")



        test_data = {"Close": [50,40,80,100],
                    "Action": ["BUY", "SELL", "BUY", "SELL"],
                    "% Profit on Trade": ["-",50.0,"-",-16.67],
                    "Cumulative % Profit": ["-",50.0,"-",33.33],
                    "Win/Loss Ratio": ["-",1.0,"-",0.2]}
        test_df = pd.DataFrame(test_data)
        with TemporaryDirectory() as tmpdir:
            fp = os.path.join(tmpdir, "test.csv")
            test_df.to_csv(fp)
            self.assertEqual(recommendation(fp), "After 2 trades, this strategy resulted in an overall profit of 33.33% with a win/loss ratio of 20.0%. Over the same period of time, the price of this stock rose 100.0%. Since this strategy does not produce a consistent winning ratio, it does not receive our recommendation.")


        test_data = {"Close": [50,40,80,80],
                    "Action": ["BUY", "SELL", "BUY", "SELL"],
                    "% Profit on Trade": ["-",50.0,"-",-16.67],
                    "Cumulative % Profit": ["-",50.0,"-",40.33],
                    "Win/Loss Ratio": ["-",1.0,"-",0.5]}
        test_df = pd.DataFrame(test_data)
        with TemporaryDirectory() as tmpdir:
            fp = os.path.join(tmpdir, "test.csv")
            test_df.to_csv(fp)
            self.assertEqual(recommendation(fp), "After 2 trades, this strategy resulted in an overall profit of 40.33% with a win/loss ratio of 50.0%. Over the same period of time, the price of this stock rose 60.0%. Though this strategy did not create as much profit as buying and holding in this instance, it does seem to be profitable and therefore receives our recommendation.")


if __name__ == "__main__":
    main()