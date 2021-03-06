#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Visualization Task takes a backtesting strategy
and produces bokeh plots and pdf report
"""
# Base Libraries
import os
import warnings

# Python Libraries
import pandas as pd
from bokeh.models import Range1d, ColumnDataSource
from bokeh.plotting import figure, output_file, show
from bokeh.io import export_png, export_svgs
from bokeh.models.widgets import DataTable, TableColumn
from fpdf import FPDF
from luigi import Parameter, Task, build, LocalTarget
from luigi.util import inherits

# Local Imports
from .backtest import Backtest, MA_Divergence, MACD_Signal_Divergence, RSI_OverSold, Stochastic_Crossover
from .scrape import scrape_summary_data
from .utils import get_strategy_text, recommendation


@inherits(Backtest, MA_Divergence, MACD_Signal_Divergence, RSI_OverSold, Stochastic_Crossover)
class Visualize(Task):
    """The Visualize Task creates a report and plots based on the given strategy and parameters
    """
    # Strategy Parameter
    strategy = Parameter(default="MA_Divergence")

    def requires(self):
        requirements = {
            "MA_Divergence": self.clone(MA_Divergence),
            "MACD_Signal_Divergence": self.clone(MACD_Signal_Divergence),
            "RSI_OverSold": self.clone(RSI_OverSold),
            "Stochastic_Crossover": self.clone(Stochastic_Crossover)
        }
        return requirements[self.strategy]

    def output(self):
        return LocalTarget(os.path.split(self.input().path)[0]+"/Report.pdf")

    def create_plots(self, export=True):
        """Creates Percent Profit Plot and Win/Loss Ratio Plot for report"""
        # Read in data from path, fill values
        warnings.simplefilter(action="ignore", category=FutureWarning)
        try:
            time_col="Date"
            df = pd.read_csv(self.input().path, parse_dates=[time_col])
        except:
            time_col="Datetime"
            df = pd.read_csv(self.input().path, parse_dates=[time_col])
        df = df[df["Win/Loss Ratio"]!="-"]
        df.iloc[:, -3:] = df.iloc[:, -3:].astype("float64")

        # Create Percent Profit Plot
        plot1 = figure(plot_width=700, plot_height=500, x_axis_type="datetime")
        strat = os.path.split(os.path.split(self.input().path)[0])[1].replace("_"," ")
        plot1.title.text = f"Percent Profit trading {self.symbol} on {self.interval} interval using {strat} strategy"
        plot1.title.text_font_size = '12pt'

        for col, color in zip(["% Profit on Trade", "Cumulative % Profit"], ["blue", "green"]):
            plot1.line(df[time_col], df[col], line_width=2, color=color, alpha=0.8, legend_label=col)

        plot1.legend.location = "top_left"

        # Create Win/Loss Ratio Plot
        plot2 = figure(plot_width=700, plot_height=500, x_axis_type="datetime")
        plot2.title.text = f"Win/Loss ratio trading {self.symbol} on {self.interval} interval using {strat} strategy"
        plot2.title.text_font_size = '12pt'
        plot2.line(df[time_col], df["Win/Loss Ratio"], line_width=3, color="purple", alpha=0.8, legend_label="Win/Loss Ratio",
        )
        plot2.legend.location = "top_left"
        plot2.y_range = Range1d(0, 1)

        # Export Plots (Dependent on a bool for testing purposes)
        if export:
            out_dir = os.path.split(self.input().path)[0]
            export_png(plot1, filename=out_dir + "/Profit_Plot.png")
            export_png(plot2, filename=out_dir + "/Ratio_Plot.png")

    def create_stock_chart(self, export=True):
        """Creates Price Chart for report"""
        # Read Parquet Files
        path = os.path.split(os.path.split(os.path.split(self.input().path)[0])[0])[0] + "/rawdata/"
        df = pd.read_parquet(path)

        # Split data based on whether price increases or decreases
        inc = df.Close > df.Open
        dec = df.Open > df.Close

        # Initiate Plot
        p = figure(x_axis_type="datetime", plot_width=1400, plot_height=500, title=f"{self.interval} chart of {self.symbol}")
        p.title.text_font_size = '12pt'

        # Create candles
        p.segment(df.index, df.High, df.index, df.Low, color="black")
        p.vbar(df.index[inc], 43200000, df.Open[inc], df.Close[inc], fill_color="lawngreen", line_color="red")
        p.vbar(df.index[dec], 43200000, df.Open[dec], df.Close[dec], fill_color="tomato", line_color="lime")

        # Export Plots (Dependent on a bool for testing purposes)
        if export:
            export_png(p, filename=os.path.split(self.input().path)[0] + "/Stock_Chart.png")

    def create_table(self, path=None, dataframe=None, outpath="test.png", export=True):
        """Create image of data table from a path or a pandas dataframe"""
        if path:
            df = pd.read_csv(path)
        else:
            df = dataframe

        table_height = int(800 / 31 * (len(df) + 1))
        columns = [TableColumn(field=col, title=col) for col in df.columns]
        data_table = DataTable(
            columns=columns, source=ColumnDataSource(df), width=740, height=table_height
        )
        # Export Plots (Dependent on a bool for testing purposes)
        if export:
            export_png(data_table, filename=outpath)

    def create_report(self, include_images=True):
        """Takes scraped data and plots to create report"""
        # Create PDF
        pdf = FPDF()

        # First Page
        pdf.add_page()

        # Write Title
        action = ["Shorting" if self.short else "Trading"][0]
        pdf.set_font("Times", "B", 16)
        pdf.cell(2,10,f"{action} {self.symbol} on {self.interval} Interval using the {self.strategy.replace('_',' ')} Strategy")

        # Write Strategy and Recommendation
        pdf.set_font("Times", "B", 10)
        pdf.write(5,"\n\n\nStrategy: "+get_strategy_text(self.strategy)+"\n\nRecommendation: " +recommendation(self.input().path))

        # Plot Plot and Chart Images
        if include_images:
            root = os.path.split(self.input().path)[0]
            WIDTH = 210
            pdf.image(root + "/Profit_Plot.png", 2, 80, WIDTH / 2 - 5)
            pdf.image(root + "/Ratio_Plot.png", WIDTH / 2 + 2, 80, WIDTH / 2 - 5)
            pdf.image(root + "/Stock_Chart.png", 2, 155, WIDTH - 5)
            pdf.image("tmp2.png", 2, 235, WIDTH-5)

            # Second Page
            pdf.add_page()

            # Add title and Plot Table
            pdf.set_font("Times", "B", 16)
            pdf.cell(2, 10, f"Backtest Trading Log",)
            pdf.image("tmp1.png", 2, 20, WIDTH - 5)

        # Write PDF to output path
        pdf.output(self.output().path)

    def run(self):
        # Create plots and Charts
        self.create_plots()
        self.create_stock_chart()
        self.create_table(path=self.input().path, outpath="tmp1.png")
        self.create_table(dataframe=scrape_summary_data(self.symbol), outpath="tmp2.png")

        # Create Report
        self.create_report()

        # Remove Unnecessary Plots
        os.remove("tmp1.png")
        os.remove("tmp2.png")


if __name__ == "__main__":
    build([Visualize(symbol="AAPL", interval="1d", strategy="RSI_OverSold")], local_scheduler=True)
    build([Visualize(symbol="AAPL", interval="1d", strategy="RSI_OverSold", short=True)], local_scheduler=True)
