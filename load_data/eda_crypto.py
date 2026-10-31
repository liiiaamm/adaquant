import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from .crypto_data_loader import CryptoDataLoader
from .trading_view_data import Interval


OUTPUT_DIR = "eda_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def concatenate_monthly_dict(years_dict: dict) -> pd.DataFrame:
    """
    Concatenate all monthly DataFrames into a single sorted dataset.
    """

    df = pd.concat(years_dict.values(), ignore_index=True)

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")

    return df


############## EDA FUNCTIONS ################

def basic_overview(df: pd.DataFrame):

    print("\n========== BASIC OVERVIEW ==========\n")
    print(df.info())
    print("\nDescribe:\n")
    print(df.describe())

    print("\nMissing values:\n")
    print(df.isna().sum())

    print("\nDuplicated rows:", df.duplicated().sum())


def plot_price_series(df):

    plt.figure()
    plt.plot(df["datetime"], df["close"])
    plt.title("ADAUSDT Close Price - Full History")
    plt.xlabel("Date")
    plt.ylabel("Price")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/price_series.png")
    plt.close()


def plot_volume(df):

    plt.figure()
    plt.plot(df["datetime"], df["volume"])
    plt.title("Trading Volume Over Time")
    plt.xlabel("Date")
    plt.ylabel("Volume")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/volume_series.png")
    plt.close()


def plot_histograms(df):

    cols = ["open", "high", "low", "close", "volume"]

    for col in cols:
        plt.figure()
        plt.hist(df[col], bins=100)
        plt.title(f"Distribution of {col}")
        plt.xlabel(col)
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/hist_{col}.png")
        plt.close()


def plot_boxplots(df):

    cols = ["open", "high", "low", "close", "volume"]

    for col in cols:
        plt.figure()
        plt.boxplot(df[col], vert=False)
        plt.title(f"Boxplot of {col}")
        plt.tight_layout()
        plt.savefig(f"{OUTPUT_DIR}/box_{col}.png")
        plt.close()


def compute_returns(df):

    df["log_return"] = np.log(df["close"] / df["close"].shift(1))
    df["pct_return"] = df["close"].pct_change()

    return df


def plot_returns(df):

    plt.figure()
    plt.plot(df["datetime"], df["pct_return"])
    plt.title("Daily Percentage Returns")
    plt.xlabel("Date")
    plt.ylabel("Return")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/returns_series.png")
    plt.close()


def plot_rolling_volatility(df, window=30):

    df["rolling_vol"] = df["pct_return"].rolling(window).std()

    plt.figure()
    plt.plot(df["datetime"], df["rolling_vol"])
    plt.title(f"{window}-Day Rolling Volatility")
    plt.xlabel("Date")
    plt.ylabel("Volatility")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/rolling_volatility.png")
    plt.close()


def plot_price_vs_volume(df):

    plt.figure()
    plt.scatter(df["volume"], df["close"], alpha=0.3)
    plt.xlabel("Volume")
    plt.ylabel("Close Price")
    plt.title("Volume vs Price")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/volume_vs_price.png")
    plt.close()


def correlation_matrix(df):

    cols = ["open", "high", "low", "close", "volume"]

    corr = df[cols].corr()

    plt.figure()
    plt.imshow(corr)
    plt.colorbar()
    plt.xticks(range(len(cols)), cols, rotation=45)
    plt.yticks(range(len(cols)), cols)
    plt.title("Correlation Matrix (OHLCV)")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/correlation_matrix.png")
    plt.close()

    print("\nCorrelation matrix:\n")
    print(corr)


def detect_outliers_iqr(df, column):

    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    outliers = df[(df[column] < lower) | (df[column] > upper)]

    print(
        f"\nOutliers detected in {column}: {len(outliers)} "
        f"({len(outliers)/len(df)*100:.2f}%)"
    )

    return outliers


############# MAIN RUN ###############

def main():

    loader = CryptoDataLoader(
        symbol="ADAUSDT",
        exchange="BINANCE",
        interval=Interval.daily,
        output_dir="monthly_data",
    )

    print("\nDownloading and splitting data...\n")
    years_dict = loader.run(n_bars=5000)

    df = concatenate_monthly_dict(years_dict)

    print(f"\nFinal dataset shape: {df.shape}")

    #BASIC
    basic_overview(df)

    #SERIES 
    plot_price_series(df)
    plot_volume(df)

    #DISTRIBUTIONS
    plot_histograms(df)
    plot_boxplots(df)

    #RETURNS & VOLATILITY
    df = compute_returns(df)
    plot_returns(df)
    plot_rolling_volatility(df)

    #RELATIONSHIPS
    plot_price_vs_volume(df)
    correlation_matrix(df)

    #OUTLIERS
    for col in ["open", "high", "low", "close", "volume"]:
        detect_outliers_iqr(df, col)

    print("\nEDA completed. Plots saved in 'eda_outputs/' folder.")


if __name__ == "__main__":
    main()
