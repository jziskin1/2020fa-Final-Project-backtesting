#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for scrape.py"""

from luigi import Task, build
from tempfile import TemporaryDirectory
from unittest import TestCase, main
import pandas as pd
import dask.dataframe as dd
from csci_utils.luigi.task import TargetOutput
from csci_utils.luigi.dask.target import ParquetTarget

from backtester.scrape import scrape_summary_data, GetHistoricalData



class TestScrape(TestCase):
    def test_scrape_summary_data(self):
        # Ensure data is scraped from yahoo and turned into pandas dataframe
        self.assertTrue(isinstance(scrape_summary_data("AAPL"), pd.DataFrame))

        # Ensure that proper errors are raised if input is incorrect
        with self.assertRaises(TypeError):
            scrape_summary_data(3)
        with self.assertRaises(ValueError):
            scrape_summary_data("ABCDEFGH")

    def test_GetHistoricalData(self):
        # Ensure data is scraped from yahoo and written to parquet
        with TemporaryDirectory() as tmp:
            fp = tmp+'/'
            class MockHistoricalData(GetHistoricalData):
                output = TargetOutput(
                    file_pattern=fp, target_class=ParquetTarget
                    )

            build([MockHistoricalData(symbol="AAPL", )], local_scheduler=True)
            data = dd.read_parquet(fp)

            self.assertTrue(isinstance(data, dd.DataFrame))
            self.assertTrue(len(data)>0)


if __name__ == "__main__":
    main()