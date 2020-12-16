from argparse import ArgumentParser, RawDescriptionHelpFormatter
from luigi import build
from .visualize import Visualize


def get_args(args):
    parser = ArgumentParser(formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument("-sy", "--symbol", type=str, nargs="?", default="AAPL")
    parser.add_argument("-i", "--interval", type=str, nargs="?", default="1d")
    parser.add_argument("-S", "--strat", type=str, nargs="?", default="MA_Divergence")
    parser.add_argument("-sh", "--short", type=bool, nargs="?", const=True, default=False)
    parser.add_argument("-f", "--fast", type=int, nargs="?", default=12)
    parser.add_argument("-s", "--slow", type=int, nargs="?", default=26)
    parser.add_argument("-u", "--sma", type=bool, nargs="?", const=True, default=False)
    parser.add_argument("-g", "--signal", type=int, nargs="?", default=9)
    args = parser.parse_args()
    return args


def main(args):
    args = get_args(args)
    build(
        [
            Visualize(
                symbol=args.symbol,
                interval=args.interval,
                strategy=args.strat,
                fast=args.fast,
                short=args.short,
                signal=args.signal,
                slow=args.slow,
                use_simple_ma=args.sma,
            )
        ],
        local_scheduler=True,
    )
