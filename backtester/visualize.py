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
from luigi import IntParameter, Parameter, BoolParameter, Task, LocalTarget, build
from csci_utils.luigi.task import TargetOutput, Requirement, Requires

# conda install -c conda-forge firefox geckodriver (Add to Readme)

# Local Imports
from backtest import MA_Divergence, MACD_Signal_Divergence, RSI_Failure_Swings
from scrape import scrape_summary_data


class Visualize(Task):
    symbol = Parameter(default="AAPL")
    interval = Parameter(default="1d")
    short = BoolParameter(default=False)
    strategy = Parameter(default="MA_Divergence")
    slow = IntParameter(default=26)
    fast = IntParameter(default=12)
    use_simple_ma = BoolParameter(default=False)
    signal = IntParameter(default=9)

    def requires(self):
        requirements = {
            "MA_Divergence": self.clone(MA_Divergence),
            "MACD_Signal_Divergence": self.clone(MACD_Signal_Divergence),
            # "RSI_Failure_Swings": self.clone(RSI_Failure_Swings),
        }
        return requirements[self.strategy]

    def create_plots(self):
        """Creates Percent Profit Plot and Profit/Loss Ratio Plot for report"""
        # Read in data from path, fill values
        warnings.simplefilter(action="ignore", category=FutureWarning)
        df = pd.read_csv(self.input().path, parse_dates=["Date"])
        df = df.replace("-", method="bfill")
        df.iloc[:, -3:] = df.iloc[:, -3:].astype("float64")

        # Create Percent Profit Plot
        plot1 = figure(plot_width=700, plot_height=500, x_axis_type="datetime")
        plot1.title.text = f"Percent Profit trading {self.symbol} on {self.interval} interval using {self.strategy.replace('_',' ')} strategy"
        for col, color in zip(
            ["% Profit on Trade", "Cumulative % Profit"], ["blue", "green"]
        ):
            plot1.line(
                df["Date"],
                df[col],
                line_width=2,
                color=color,
                alpha=0.8,
                legend_label=col,
            )
        plot1.legend.location = "top_left"

        # Create Profit/Loss Ratio Plot
        plot2 = figure(plot_width=700, plot_height=500, x_axis_type="datetime")
        plot2.title.text = f"Profit/Loss ratio trading {self.symbol} on {self.interval} interval using {self.strategy.replace('_',' ')} strategy"
        plot2.line(
            df["Date"],
            df["Profit/Loss Ratio"],
            line_width=3,
            color="purple",
            alpha=0.8,
            legend_label="Profit/Loss Ratio",
        )
        plot2.legend.location = "top_left"
        plot2.y_range = Range1d(0, 1)

        # Export Plots
        out_dir = os.path.split(self.input().path)[0]
        export_png(plot1, filename=out_dir + "/Profit_Plot.png")
        export_png(plot2, filename=out_dir + "/Ratio_Plot.png")

    def create_stock_chart(self):
        """Creates Price Chart for report"""
        path = os.path.split(os.path.split(self.input().path)[0])[0] + "/rawdata/"
        df = pd.read_parquet(path)
        inc = df.Close > df.Open
        dec = df.Open > df.Close
        p = figure(
            x_axis_type="datetime",
            plot_width=1400,
            plot_height=500,
            title=f"{self.interval} chart of {self.symbol}",
        )
        p.segment(df.index, df.High, df.index, df.Low, color="black")
        p.vbar(
            df.index[inc],
            43200000,
            df.Open[inc],
            df.Close[inc],
            fill_color="lawngreen",
            line_color="red",
        )
        p.vbar(
            df.index[dec],
            43200000,
            df.Open[dec],
            df.Close[dec],
            fill_color="tomato",
            line_color="lime",
        )
        export_png(p, filename=os.path.split(self.input().path)[0] + "/Stock_Chart.png")
        # show(p)

    def create_table(self, path=None, dataframe=None, outpath="test.png"):
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
        export_png(data_table, filename=outpath)
        # show(data_table)

    def create_report(self):
        """Takes scraped data and plots to create report"""
        root = os.path.split(self.input().path)[0]
        WIDTH = 210
        HEIGHT = 297
        pdf = FPDF()
        pdf.add_page()
        pdf.set_font("Helvetica", "B", 16)
        pdf.cell(
            2,
            10,
            f"Trading {self.symbol} on {self.interval} Interval using the {self.strategy.replace('_',' ')} Strategy",
        )
        pdf.image(root + "/Profit_Plot.png", 2, 30, WIDTH / 2 - 5)
        pdf.image(root + "/Ratio_Plot.png", WIDTH / 2 + 2, 30, WIDTH / 2 - 5)
        pdf.image(root + "/Stock_Chart.png", 2, 105, WIDTH - 5)
        pdf.output(root + "/Report.pdf")

    def run(self):
        self.create_plots()
        self.create_stock_chart()
        self.create_table(path=self.input().path, outpath="tmp1.png")
        self.create_table(
            dataframe=scrape_summary_data(self.symbol), outpath="tmp2.png"
        )
        self.create_report()
        os.remove("tmp1.png")
        os.remove("tmp2.png")


if __name__ == "__main__":
    build(
        [Visualize(symbol="NFLX", interval="1h", strategy="MACD_Signal_Divergence")],
        local_scheduler=True,
    )


### TODO:
# 1. Finish Visualization functionality (Need to add other part to reports)
# 2. Create CLI and Main.py
# 3. Write README
# 4. Figure out how to incorporate metaclasses etc.
