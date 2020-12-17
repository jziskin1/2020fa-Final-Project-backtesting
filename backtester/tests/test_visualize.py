#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for visualize.py"""

import os
from luigi import Task, build, LocalTarget
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from csci_utils.luigi.task import Requirement, Requires, TargetOutput
from csci_utils.luigi.dask.target import ParquetTarget

from backtester.scrape import GetHistoricalData
from backtester.backtest import MA_Divergence
from backtester.visualize import Visualize


class TestTasks(TestCase):
    def test_BacktestTasks(self):
        with TemporaryDirectory() as tmp:
            class MockHistoricalData(GetHistoricalData):
                """Mock of GetHistoricalData with Temporary Directory path as target"""
                # Write to temporary directory
                output = TargetOutput(file_pattern=tmp+'/rawdata/', target_class=ParquetTarget)

            class Mock_MA_Divergence(MA_Divergence):
                """Mock of MA_Divergence with Temporary Directory path as target"""
                # Use MockHistoricalData as Requirement and write to temporary directory
                requires = Requires()
                history = Requirement(MockHistoricalData)
                output = TargetOutput(file_pattern=tmp+"/Repo1/Repo2/MA_test.csv",target_class=LocalTarget)

            class Mock_Visualize(Visualize):
                def requires(self):
                    requirements = {"MA_Divergence": self.clone(Mock_MA_Divergence)}
                    return requirements[self.strategy]
                output = TargetOutput(file_pattern=tmp+"/Repo1/Repo2/Visualize_test.pdf",target_class=LocalTarget)

            # Run MockVisualize
            build([Mock_Visualize()], local_scheduler=True)

            # Write all expected output paths created by tasks
            written_paths = [
                tmp + "/rawdata/_common_metadata",
                tmp + "/rawdata/_metadata",
                tmp + "/rawdata/_SUCCESS",
                tmp + "/rawdata/part.0.parquet",
                tmp + "/Repo1/Repo2/MA_test.csv",
                tmp + "/Repo1/Repo2/Profit_Plot.png",
                tmp + "/Repo1/Repo2/Ratio_Plot.png",
                tmp + "/Repo1/Repo2/Stock_Chart.png",
                tmp + "/Repo1/Repo2/Visualize_test.pdf"
            ]

            # Assert paths exist and are dataframes with the correct row
            for path in written_paths:
                self.assertTrue(os.path.exists(os.path.join(tmp, path)))


if __name__ == "__main__":
    main()