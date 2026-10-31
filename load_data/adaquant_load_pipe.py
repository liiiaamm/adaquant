from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List

import pandas as pd

from load_data.crypto_data_loader import CryptoDataLoader
from load_data.create_buckets import S3YearlyBucketManager
from load_data.rawdata_bucket_uploader import S3DataUploader


# =========================
# Pipeline domain exceptions
# =========================
class AdaquantPipelineError(Exception):
    """Base exception for all ADAQuant pipeline errors."""


class DataLoadError(AdaquantPipelineError):
    """Raised when the data loading step fails."""


class BucketCreationError(AdaquantPipelineError):
    """Raised when S3 bucket creation or initialization fails."""


class UploadError(AdaquantPipelineError):
    """Raised when uploading datasets to S3 fails."""


# =========================
# Configuration
# =========================
@dataclass
class PipelineConfig:
    """
    Configuration container for the ingestion pipeline.

    Attributes
    ----------
    symbol : str
        Trading symbol to download from the data source (e.g., "ADAUSDT").
    exchange : str
        Exchange identifier used by the data source (e.g., "BINANCE").
    region : str
        AWS region where S3 resources will be managed.
    bucket_name : str
        Name of the S3 bucket where the data lake is stored.
    crypto_name : str
        Cryptocurrency name used in the S3 path structure (e.g., "ada").
    tiers : List[str]
        Tiers used in the data lake structure (e.g., ["bronze", "silver", "gold"]).
    """
    symbol: str = "ADAUSDT"
    exchange: str = "BINANCE"
    region: str = "eu-south-2"
    bucket_name: str = "adaquant-data-bucket"
    crypto_name: str = "ada"
    tiers: List[str] = None

    def __post_init__(self) -> None:
        if self.tiers is None:
            self.tiers = ["bronze", "silver", "gold"]


# =========================
# Pipeline orchestrator
# =========================
class AdaquantLoadPipe:
    """
    Ingestion pipeline orchestrator for the ADAQuant project.

    The pipeline is split into three main stages:
      1) Load and partition historical data (loader)
      2) Create bucket + tier/crypto/year/month structure in S3 (manager)
      3) Upload monthly datasets to S3 (uploader)

    Each stage can be executed independently with its own parameters:
      - load(...)
      - ensure_buckets(...)
      - upload(...)

    The full end-to-end process can be run via `run_pipeline(...)`.

    Notes
    -----
    - `self.datasets` stores the most recently loaded/partitioned datasets.
    - `S3YearlyBucketManager` creates its own boto3 client internally.
    - `S3DataUploader` can optionally reuse an externally provided `s3_client`.
    """

    def __init__(
        self,
        config: PipelineConfig = PipelineConfig(),
        loader: CryptoDataLoader | None = None,
        manager: S3YearlyBucketManager | None = None,
        uploader: S3DataUploader | None = None,
        s3_client=None,
    ):
        """
        Initialize the pipeline and its internal components.

        Parameters
        ----------
        config : PipelineConfig, optional
            Default configuration for symbol, exchange, AWS region, bucket name, and lake structure.
        loader : CryptoDataLoader, optional
            Custom loader instance (dependency injection). If None, a default one is created.
        manager : S3YearlyBucketManager, optional
            Custom S3 bucket manager instance. If None, a default one is created.
        uploader : S3DataUploader, optional
            Custom uploader instance. If None, a default one is created.
        s3_client : optional
            Optional boto3 S3 client to be reused by the uploader.
        """
        self.config = config

        self.loader = loader if loader is not None else CryptoDataLoader(
            symbol=config.symbol,
            exchange=config.exchange,
        )

        # NEW MANAGER SIGNATURE: (region, bucket_name)
        self.manager = manager if manager is not None else S3YearlyBucketManager(
            region=config.region,
            bucket_name=config.bucket_name,
        )

        # NEW UPLOADER: bucket_prefix should be the actual bucket name here
        self.uploader = uploader if uploader is not None else S3DataUploader(
            region=config.region,
            bucket_prefix=config.bucket_name,
            start_year=None,
            end_year=None,
            s3_client=s3_client,
        )

        # Ensure uploader path matches config (uploader defaults are hardcoded in your class)
        self.uploader.coin = config.crypto_name
        # Keep uploader default tier unless you want to set it here:
        # self.uploader.tier = config.tiers[0]

        # Internal state: last loaded/partitioned datasets
        self.datasets: Optional[dict[str, pd.DataFrame]] = None

    ######### Load ##########
    def load(self, n_bars: int = 1483, verbose: bool = False) -> dict[str, pd.DataFrame]:
        """
        Download historical data and partition it into monthly datasets.

        The returned object is also stored in `self.datasets`.

        Returns
        -------
        dict[str, pandas.DataFrame]
            A dictionary mapping month keys (YYYY_MM) to DataFrames.

        Raises
        ------
        DataLoadError
            If the loader fails for any reason.
        """
        try:
            self.datasets = self.loader.run(n_bars=n_bars, verbose=verbose)
            return self.datasets
        except Exception as e:
            raise DataLoadError(
                f"Data loading failed (n_bars={n_bars}, symbol={self.config.symbol}, exchange={self.config.exchange})"
            ) from e

    ############  Buckets  ###########
    def ensure_buckets(
        self,
        start_year: int,
        end_year: int,
        tiers: Optional[List[str]] = None,
        crypto_name: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        """
        Ensure the S3 bucket exists and contains the tier/crypto/year/month prefix structure.

        Parameters
        ----------
        start_year : int
            First year to create (inclusive).
        end_year : int
            Last year to create (inclusive).
        tiers : list[str], optional
            Tiers to create (defaults to config.tiers).
        crypto_name : str, optional
            Crypto name to use in the structure (defaults to config.crypto_name).
        verbose : bool, optional
            Whether to print progress information from the manager.

        Raises
        ------
        BucketCreationError
            If bucket creation or initialization fails.
        """
        try:
            self.manager.create_crypto_structure(
                crypto_name=crypto_name or self.config.crypto_name,
                tiers=tiers or self.config.tiers,
                start_year=start_year,
                end_year=end_year,
                verbose=verbose,
            )
        except Exception as e:
            raise BucketCreationError(
                f"S3 structure creation failed (start_year={start_year}, end_year={end_year}, "
                f"region={self.config.region}, bucket_name={self.config.bucket_name}, "
                f"crypto_name={crypto_name or self.config.crypto_name}, tiers={tiers or self.config.tiers})"
            ) from e

    ############## Stage 3: Upload ###############
    def upload(
        self,
        datasets: Optional[dict[str, pd.DataFrame]] = None,
        start_year: Optional[int] = None,
        end_year: Optional[int] = None,
        tier: Optional[str] = None,
        coin: Optional[str] = None,
        verbose: bool = False,
    ) -> None:
        """
        Upload monthly datasets to S3.

        If `datasets` is not provided, the method uses `self.datasets` from the last
        successful `load()` call.

        Parameters
        ----------
        datasets : dict[str, pandas.DataFrame], optional
            Monthly datasets to upload. If None, uses `self.datasets`.
        start_year : int, optional
            Filter uploads to this start year (inclusive).
        end_year : int, optional
            Filter uploads to this end year (inclusive).
        tier : str, optional
            Tier for upload path (defaults to uploader.tier).
        coin : str, optional
            Coin/crypto name for upload path (defaults to config.crypto_name).
        verbose : bool, optional
            Whether to print progress information from the uploader.

        Raises
        ------
        UploadError
            If no datasets are available or if the upload fails.
        """
        data = datasets if datasets is not None else self.datasets
        if data is None:
            raise UploadError("No datasets available. Run load() first or pass datasets explicitly.")

        ###Update uploader filters + path
        self.uploader.start_year = start_year
        self.uploader.end_year = end_year

        if tier is not None:
            self.uploader.tier = tier
        if coin is not None:
            self.uploader.coin = coin
        else:
            self.uploader.coin = self.config.crypto_name

        try:
            if verbose:
                print(
                    f"Uploading datasets to s3://{self.config.bucket_name}/"
                    f"{self.uploader.tier}/{self.uploader.coin}/year=YYYY/month=MM/"
                )
            self.uploader.upload_monthly_datasets(data)
        except Exception as e:
            raise UploadError(
                f"Upload failed (start_year={start_year}, end_year={end_year}, "
                f"region={self.config.region}, bucket_name={self.config.bucket_name}, "
                f"tier={self.uploader.tier}, coin={self.uploader.coin})"
            ) from e

    ######### Full pipeline #########
    def run_pipeline(
        self,
        *,
        n_bars: int = 1483,
        buckets_start_year: int = 2022,
        buckets_end_year: int = 2025,
        upload_start_year: int = 2022,
        upload_end_year: int = 2025,
        tiers: Optional[List[str]] = None,
        crypto_name: Optional[str] = None,
        upload_tier: str = "bronze",
        upload_coin: Optional[str] = None,
        verbose_loader: bool = False,
        verbose_manager: bool = False,
        verbose_uploader: bool = False,
        verbose_pipe: bool = False,
    ) -> None:
        """
        Run the full end-to-end pipeline with configurable parameters.

        Parameters
        ----------
        n_bars : int, optional
            Number of bars to download in the load stage.
        buckets_start_year : int, optional
            First year for S3 structure creation (inclusive).
        buckets_end_year : int, optional
            Last year for S3 structure creation (inclusive).
        upload_start_year : int, optional
            First year to upload (inclusive).
        upload_end_year : int, optional
            Last year to upload (inclusive).
        tiers : list[str], optional
            Tiers for structure creation (defaults to config.tiers).
        crypto_name : str, optional
            Crypto name for structure creation (defaults to config.crypto_name).
        upload_tier : str, optional
            Tier used for upload path.
        upload_coin : str, optional
            Coin used for upload path (defaults to config.crypto_name).
        verbose_loader : bool, optional
            Enable verbose output for the load stage.
        verbose_manager : bool, optional
            Enable verbose output for the bucket creation stage.
        verbose_uploader : bool, optional
            Enable verbose output for the upload stage.
        verbose_pipe : bool, optional
            Enable high-level pipeline progress messages.

        Raises
        ------
        AdaquantPipelineError
            If any stage fails (load, bucket creation, or upload).
        """
        if verbose_pipe:
            print("Starting pipeline...")

        self.load(n_bars=n_bars, verbose=verbose_loader)

        self.ensure_buckets(
            start_year=buckets_start_year,
            end_year=buckets_end_year,
            tiers=tiers,
            crypto_name=crypto_name,
            verbose=verbose_manager,
        )

        self.upload(
            start_year=upload_start_year,
            end_year=upload_end_year,
            tier=upload_tier,
            coin=upload_coin or (crypto_name or self.config.crypto_name),
            verbose=verbose_uploader,
        )

        if verbose_pipe:
            print("Pipeline finished without errors.")
