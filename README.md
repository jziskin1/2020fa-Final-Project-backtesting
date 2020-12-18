# BackTester
[![Build Status](https://travis-ci.com/jziskin1/2020fa-Final-Project-backtesting.svg?branch=main)](https://travis-ci.com/jziskin1/2020fa-Final-Project-backtesting) 
<a href="https://codeclimate.com/github/jziskin1/2020fa-Final-Project-backtesting/maintainability"><img src="https://api.codeclimate.com/v1/badges/b452a3cadb8790e53a66/maintainability" /></a>
<a href="https://codeclimate.com/github/jziskin1/2020fa-Final-Project-backtesting/test_coverage"><img src="https://api.codeclimate.com/v1/badges/b452a3cadb8790e53a66/test_coverage" /></a> 
## Objective

Backtesting is the general method for seeing how well a strategy or model would 
have done by discovering how it would play out using historical data. This project 
aims to create an open source python project that pulls stock data from internet, 
backtests a trading strategy chosen by a user, and produces meaningful output that 
will either recommend or reject the given strategy.

## Strategy

1. Web Scrape: Use BeautifulSoup, requests, and yfinance packages to pull live stock 
data from yahoo finance into my local python environment.

2. Backtest: Hardcode technical analysis indicators such as an exponential moving 
average, MACD and relative strength index in indicators.py. Use said indicators to 
backtest different strategies and determine % profit, win/loss ratio.

3. Visualize: Automate pdf reports that contain plots, stock charts, dataframes and 
recommendations.

## Parameterized Luigi Workflow

One of the main staples of this package is just how many ways there are to customize 
each task to fit you analysis needs. Below are just some of the ways you parameters
you can choose

 * Symbol: Choose from any stock symbol publicly listed on the NYSE and foreign markets.

 * Interval: Want to day trade? Let's analyze a 1 minute chart. Looking for more long term
 investments? Set that interval to 1 day or 1 week. 
 
 * Short: Interested in shorting strategies? Set short equal to true and run it all from
 there
 
 * Strategy: Want to track the MACD and signal line? Or maybe you want to trade the RSI.
 This package comes with 5 strategies based on the most widely used stock indicators.
 
 * Many more: Do you want you moving average to be calculated over 14 days or every 12 days?
 You have the tools to do that. Each stock indicator comes with its own parameters that 
 you the user get to set.


## Tools Leveraged
Are you interested in learning about stocks or python? Either way, see below for all the tools 
used in the creation of this project.

* Luigi

* Dask

* Decorators

* Descriptors

* Context Managers

* CookieCutter

* Git Flow

* Virtual Environments

* Class Inheritance

* Abstract Classes

* CS/CI with Travis

* Unittesting

* Technical Analysis

* Web Scraping

* Bokeh

* FPDF

## How to Run

In order to run this project simply use ```python -m backtester```. You can parameterize 
using the following arguments:

Choose a stock with ```-sy <symbol>``` or ```--symbol <symbol>```. Defaults to 'AAPL'

Choose a time interval with ```-i <interval>``` or ```--interval <interval>```. Intervals can be one of '1m', '2m', '5m', '15m', '1h', '1d', '5d', '1wk', '1mo', '3mo'. Defaults to '1d.'

Choose a strategy with ```-S <strategy>``` or ```--strat <strategy>```.
The current available strategies are "MA_Divergence", "MACD_Signal_Divergence", "RSI_OverSold", "Stochastic_Crossover." Defaults to MA_Divergence.

Choose to short with ```-sh``` or  ```--short```. Defaults to False (will not short unless told to.)

Set the number of time periods to take the fast average over with ```-f <int>``` or ```--fast <int>```. (Only applicable to MA_Divergence and MACD_Signal_Divergence). Defaults to 12.

Set the number of time periods to take the slow average over with ```-s <int>``` or ```--slow <int>```. (Only applicable to MA_Divergence and MACD_Signal_Divergence). Defaults to 26.

Use a simple average for MA_Divergence strategy with ```-sma``` or ```--simple```. (Only applicable to MA_Divergence). Defaults to False (will use exponential moving average by default.)

Set the number of time periods signal line should calculate over with ```-g <int>``` or ```--signal <int>```. (Only applicable to MACD_Signal_Divergence). Defaults to 9.

Set the lower bound signal for buying of oscillator with ```-l <int>``` or ```--lower <int>```. (Only applicable to RSI_OverSold and Stochastic_Crossover). Defaults to 30.

Set the upper bound signal for selling of oscillator with ```-u <int>``` or ```--upper <int>```. (Only applicable to RSI_OverSold and Stochastic_Crossover). Defaults to 70.

Set the time period to calculate oscillator over with ```-p <int>``` or ```--period <int>```. (Only applicable to RSI_OverSold and Stochastic_Crossover). Defaults to 14.


## Acknowledgements
A special thanks to Dr. Scott Gorlin and all the students and TA's of Harvard 
Extension School's CSCI-29 Fall 2020 class.

For more information on this project, reach out to Jordan Ziskin at jordan.ziskin@yahoo.com.

