#from load_data.adaquant_load_pipe import AdaquantLoadPipe
from load_data.create_buckets import S3YearlyBucketManager

from load_data.crypto_data_loader import CryptoDataLoader
from load_data.rawdata_bucket_uploader import S3DataUploader


def main() -> None:

    loader = CryptoDataLoader(
        symbol="ADAUSDT",
        exchange="BINANCE",
    )
    dtsets = loader.run(n_bars=1483)

    manager = S3YearlyBucketManager(region="eu-south-2")
    manager.create_crypto_structure(
        crypto_name="ada",
        tiers=["bronze", "silver", "gold"],
        start_year=2022,
        end_year=2025,
        verbose=True,
    )

    data_uploader = S3DataUploader(region="eu-south-2", bucket_prefix="adaquant-data-bucket", start_year=2022, end_year=2025)
    data_uploader.upload_monthly_datasets(dtsets)

if __name__ == "__main__":
    main()