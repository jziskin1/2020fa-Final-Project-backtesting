#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for indicators.py"""

from unittest import TestCase, main
import pandas as pd

from backtester.indicators import calculate_sma, calculate_ema, calculate_macd, calculate_sto_osc, calculate_rsi

class TestIndicators(TestCase):
    def test_calculate_sma(self):
        # Ensure sma is calculated correctly
        sma = calculate_sma(pd.array([1,4,7,3,6,8,10,12]),4)
        self.assertEqual(list(sma),[3.75,5.00,6.00,6.75,9.0])

        # Ensure type error raised
        with self.assertRaises(TypeError):
            calculate_sma(pd.array([1,4,7,3,6,8,10,12]), "Not a Number")

    def test_calculate_ema(self):
        # Ensure ema is calculated correctly
        ema = calculate_ema(pd.array([1,4,7,3,6,8,10,12]),4)
        self.assertEqual(list(round(ema,4)),[3.7500,4.6500,5.9900,7.5940,9.3564])

        # Ensure type error raised
        with self.assertRaises(TypeError):
            calculate_ema(pd.array([1,4,7,3,6,8,10,12]), "Not a Number")

    def test_calculate_macd(self):
        # Ensure ema is calculated correctly

        test_df = pd.DataFrame(
            {"Close": [10, 12, 13, 15, 16, 18, 20, 21, 16, 18, 15, 17, 18, 24, 25, 27, 35, 33, 36, 29, 26]})
        macd = calculate_macd(test_df["Close"], 12, 14)
        self.assertEqual(list(round(macd, 6)), [0.788602, 0.838708, 0.899273, 1.089343, 1.165357, 1.258734, 1.157801, 0.998495])

        # Ensure type error raised
        with self.assertRaises(TypeError):
            calculate_macd(test_df, "Not a Number")

    def test_calculate_sto_osc(self):
        # Ensure ema is calculated correctly
        test_df = pd.DataFrame({"Close": [2, 4, 4, 6, 4, 4, 6, 7, 7, 11], "High": [3, 5, 5, 8, 5, 6, 7, 10, 8, 12],
                           "Low": [1, 2, 3, 4, 2, 1, 4, 6, 6, 10]})
        stoc = calculate_sto_osc(test_df, 4)
        self.assertEqual(list(round(stoc, 6)), [0.714286, 0.333333, 0.428571, 0.714286, 0.666667, 0.666667])

        # Ensure type error raised
        with self.assertRaises(TypeError):
            calculate_sto_osc(test_df, "Not a Number")

    def test_calculate_rsi(self):
        # Ensure ema is calculated correctly
        test_df = pd.DataFrame({"Open": [95, 97, 92, 101, 98, 98, 94, 112], "Close": [100, 95, 100, 105, 100, 95, 100, 105]})
        rsi = calculate_rsi(test_df, 4)
        self.assertEqual(list(round(rsi, 6)),[89.473684, 90.769231, 72.839506, 82.210243, 53.498343])

        # Ensure type error raised
        with self.assertRaises(TypeError):
            calculate_rsi(test_df, "Not a Number")


if __name__ == "__main__":
    main()
