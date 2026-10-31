import re
import pandas as pd
from load_data.trading_view_data import TradingViewData, Interval


class CryptoDataLoader:
    """
    Downloads OHLCV data from TradingView and get monthly CSV files.

    Code execution example:
    
    loader = CryptoDataLoader(
        symbol="ADAUSDT",
        exchange="BINANCE",
        interval=Interval.daily,
        output_dir="monthly_data",
    )

    loader.run(n_bars=1483)

    """

    def __init__(
        self,
        symbol: str,
        exchange: str,
        interval: Interval = Interval.daily,
    ):
        self.symbol = symbol
        self.exchange = exchange
        self.interval = interval
        self.client = TradingViewData()

    def download_history(self, n_bars: int = 10000, verbose=False) -> pd.DataFrame:
        """
        Download historical price data from TradingView.
        """
        if verbose:
            print(f"Downloading {self.symbol} from {self.exchange}...")

        df = self.client.get_hist(
            symbol=self.symbol,
            exchange=self.exchange,
            interval=self.interval,
            n_bars=n_bars,
        )

        if df.empty:
            raise ValueError("No data returned from TradingView.")

        df = df.reset_index()

        return df

    def _extract_year_month(self, dt: str) -> str:
        """
        Extract YYYY_MM from datetime string using regex.
        Example: 2025-01-22 01:00:00 -> 2025_01
        """

        pattern = r"(\d{4})-(\d{2})-\d{2}"
        match = re.search(pattern, dt)

        if not match:
            raise ValueError(f"Invalid datetime format: {dt}")

        year, month = match.groups()
        return f"{year}_{month}"

    def _split_and_save_by_month(self, df: pd.DataFrame, verbose=False):
        """
        Split dataframe by month and return dict of monthly DataFrames.
        """
        if verbose:
            print("Splitting data by month...")

        df["datetime"] = pd.to_datetime(df["datetime"])

        df["year_month"] = df["datetime"].dt.strftime("%Y_%m")
        df["day"] = df["datetime"].dt.day

        years_dict = {}

        for ym, monthly_df in df.groupby("year_month"):
            monthly_df = monthly_df.drop(columns=["datetime", "year_month"])
            years_dict[ym] = monthly_df.reset_index(drop=True)

        return years_dict


    
    def run(self, n_bars: int = 1483, verbose = False):
        """
        Full pipeline: download + split + save.
        """
        df = self.download_history(n_bars=n_bars, verbose=verbose)
        dict_csvs = self._split_and_save_by_month(df, verbose=verbose)

        return dict_csvs

