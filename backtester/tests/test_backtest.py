#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for backtest.py"""

import os
from luigi import Task, build, LocalTarget
from tempfile import TemporaryDirectory
from unittest import TestCase, main
import pandas as pd
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from csci_utils.luigi.dask.target import ParquetTarget

from backtester.scrape import GetHistoricalData
from backtester.backtest import MA_Divergence, MACD_Signal_Divergence, RSI_OverSold, Stochastic_Crossover


class TestTasks(TestCase):
    def test_BacktestTasks(self):
        with TemporaryDirectory() as tmp:
            class MockHistoricalData(GetHistoricalData):
                """Mock of GetHistoricalData with Temporary Directory path as target"""
                # Write to temporary directory
                output = TargetOutput(file_pattern=tmp+'/', target_class=ParquetTarget)

            class Mock_MA_Divergence(MA_Divergence):
                """Mock of MA_Divergence with Temporary Directory path as target"""
                # Use MockHistoricalData as Requirement and write to temporary directory
                requires = Requires()
                history = Requirement(MockHistoricalData)
                output = TargetOutput(file_pattern=tmp+"/MA_test.csv",target_class=LocalTarget)

            class Mock_MACD_Signal_Divergence(MACD_Signal_Divergence):
                """Mock of MACD_Signal_Divergence with Temporary Directory path as target"""
                # Use MockHistoricalData as Requirement and write to temporary directory
                requires = Requires()
                history = Requirement(MockHistoricalData)
                output = TargetOutput(file_pattern=tmp+"/MACD_test.csv",target_class=LocalTarget)

            class Mock_RSI_OverSold(RSI_OverSold):
                """Mock of RSI_OverSold with Temporary Directory path as target"""
                # Use MockHistoricalData as Requirement and write to temporary directory
                requires = Requires()
                history = Requirement(MockHistoricalData)
                output = TargetOutput(file_pattern=tmp + "/RSI_test.csv", target_class=LocalTarget)

            class Mock_Stochastic_Crossover(Stochastic_Crossover):
                """Mock of Stochastic_Crossover with Temporary Directory path as target"""
                # Use MockHistoricalData as Requirement and write to temporary directory
                requires = Requires()
                history = Requirement(MockHistoricalData)
                output = TargetOutput(file_pattern=tmp + "/Stoc_test.csv", target_class=LocalTarget)


            # Run MockBackTests
            build([Mock_MA_Divergence()], local_scheduler=True)
            build([Mock_MACD_Signal_Divergence()], local_scheduler=True)
            build([Mock_RSI_OverSold()], local_scheduler=True)
            build([Mock_Stochastic_Crossover()], local_scheduler=True)

            # Write all expected output paths created by tasks
            written_paths = [
                tmp +"/_common_metadata",
                tmp + "/_metadata",
                tmp + "/_SUCCESS",
                tmp + "/part.0.parquet",
                tmp+"/MA_test.csv",
                tmp+"/MACD_test.csv",
                tmp + "/RSI_test.csv",
                tmp + "/Stoc_test.csv",
            ]

            # Assert paths exist and are dataframes with the correct row
            for path in written_paths:
                self.assertTrue(os.path.exists(os.path.join(tmp, path)))

                if path[-4:] == ".csv":
                    Expected_cols = ['Date', 'Close', 'Action', '% Profit on Trade', 'Cumulative % Profit', 'Win/Loss Ratio']
                    self.assertEqual(list(pd.read_csv(path).columns), Expected_cols)


if __name__ == "__main__":
    main()