import boto3
from botocore.exceptions import ClientError
from typing import List


class S3YearlyBucketManager:
    """
    Manager class responsible for creating a single S3 bucket and
    initializing a crypto-based data lake structure.

    Bucket:
        <bucket_name>

    Structure:
        tier/<crypto_name>/year=YYYY/month=MM/

    Example:
        s3://adaquant-data-bucket/bitcoin/gold/year=2024/month=01/

    The bucket is created with secure defaults:
        - Public access blocked
        - Versioning enabled
        - Server-side encryption (SSE-S3)

    Public API
    ----------
    create_crypto_structure(
        crypto_name: str,
        tiers: List[str]
        start_year: int,
        end_year: int
    )
    """

    def __init__(
        self,
        region: str,
        bucket_name: str = "adaquant-data-bucket",
    ):
        """
        Initialize the S3CryptoDataLakeManager.

        Parameters
        ----------
        region : str
            AWS region where the bucket will be created.
        bucket_name : str, optional
            Name of the S3 bucket.
        """
        self.region = region
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3", region_name=region)

    def _ensure_bucket(self, verbose: bool = False) -> None:
        """Ensure that the S3 bucket exists with secure defaults."""
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            if verbose:
                print(f"Bucket already exists: {self.bucket_name}")
            return
        except ClientError:
            pass

        if verbose:
            print(f"Creating bucket: {self.bucket_name}")

        params = {"Bucket": self.bucket_name}
        if self.region != "us-east-1":
            params["CreateBucketConfiguration"] = {
                "LocationConstraint": self.region
            }

        self.s3.create_bucket(**params)

        self.s3.put_public_access_block(
            Bucket=self.bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )

        self.s3.put_bucket_versioning(
            Bucket=self.bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )

        self.s3.put_bucket_encryption(
            Bucket=self.bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            },
        )

    @staticmethod
    def _normalize_name(name: str) -> str:
        """
        Normalize names to be S3-friendly:
        - lowercase
        - no leading/trailing slashes
        """
        return name.lower().strip().strip("/")

    def _create_crypto_year_month_structure(
        self,
        crypto_name: str,
        tiers: List[str],
        start_year: int,
        end_year: int,
        verbose: bool = False,
    ) -> None:
        """Create crypto/year/month prefixes inside the bucket."""
        if start_year > end_year:
            raise ValueError("start_year must be <= end_year")

        crypto_name = self._normalize_name(crypto_name)
        for year in range(start_year, end_year + 1):
            for tier in tiers:
                for month in range(1, 13):
                    key = (
                        f"{tier}/"
                        f"{crypto_name}/"
                        f"year={year}/"
                        f"month={month:02d}/.keep"
                    )
                    self.s3.put_object(
                        Bucket=self.bucket_name,
                        Key=key,
                    )

        if verbose:
            print(
                f"Structure created: s3://{self.bucket_name}/{crypto_name}/tier/"
                f" (years {start_year}-{end_year})"
            )

    def create_crypto_structure(
        self,
        crypto_name: str,
        tiers: List[str],
        start_year: int,
        end_year: int,
        verbose: bool = False,
    ) -> None:
        """
        Public entry point.

        Parameters
        ----------
        crypto_name : str
            Cryptocurrency name (e.g. 'bitcoin', 'ethereum').
        tiers: List[str]
            Tiers to split bucket: bronce, silver, gold
        start_year : int
            First year to create (inclusive).
        end_year : int
            Last year to create (inclusive).
        """
        self._ensure_bucket(verbose=verbose)
        self._create_crypto_year_month_structure(
            crypto_name=crypto_name,
            tiers=tiers,
            start_year=start_year,
            end_year=end_year,
            verbose=verbose,
        )
