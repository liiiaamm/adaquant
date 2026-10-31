import io
import boto3
import pandas as pd


class S3DataUploader:
    """
    Class responsible for managing CSV uploads to AWS S3.

    This class allows monthly datasets to be uploaded to a specific S3 bucket,
    following a structured hierarchy organized by year and month.

    Attributes:
    -----------
    region : str
        AWS region where the S3 bucket is located.
    bucket_prefix : str
        Name of the S3 bucket. Default is 'adaquant'.
    start_year : Optional[int]
        Start year filter for uploaded data. If None, no lower bound is applied.
    end_year : Optional[int]
        End year filter for uploaded data. If None, no upper bound is applied.
    s3 : boto3.client
        S3 client used to interact with AWS. Defaults to a standard boto3 client
        if no custom client is provided.
    """

    def __init__(
        self,
        region: str,
        bucket_prefix: str = "adaquant",
        start_year: int | None = None,
        end_year: int | None = None,
        s3_client=None,
    ) -> None:

        self.region = region
        self.bucket_prefix = bucket_prefix
        self.safe_prefix = bucket_prefix.lower().replace("_", "-").strip("-")
        self.s3 = s3_client or boto3.client("s3", region_name=region)
        self.start_year = start_year
        self.end_year = end_year
        self.tier = "bronze"
        self.coin = "ada"

    def _parse_year_month(self, year_month: str) -> tuple[str, str]:
        """
        Splits a 'YYYY_MM' formatted string into year and month values.

        Args:
        year_month : str
            String in the format 'YYYY_MM', where YYYY represents the year
            and MM represents the month.

        Returns:
        tuple[str, str]
            A tuple containing year and month as strings.
        """
        year, month = year_month.split("_")
        return year, month

    def _build_s3_key(self, year: str, month: str, filename: str) -> str:
        """
        Builds the S3 object key where the file will be stored.

        Args:
        year : str
            Year component of the storage path.
        month : str
            Month component of the storage path.
        filename : str
            Name of the file to upload.

        Returns:
        str
            Full S3 key in the format:
            'bronze/ada/year=YYYY/month=MM/filename'.
        """
        return f"{self.tier}/{self.coin}/year={year}/month={month}/{filename}"

    def _upload_dataframe(
        self,
        df: pd.DataFrame,
        bucket_name: str,
        s3_key: str,
    ) -> None:
        """
        Converts a pandas DataFrame to CSV in memory and uploads it to S3.

        Args:
        df : pd.DataFrame
            DataFrame containing the data to upload.
        bucket_name : str
            Name of the target S3 bucket.
        s3_key : str
            Full key (path) where the file will be stored inside the bucket.

        Returns:
        None
            This method uploads the file to S3 and does not return a value.
        """
        csv_buffer = io.StringIO()

        df.to_csv(csv_buffer, index=False)

        self.s3.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv",
        )

    def upload_monthly_datasets(
        self,
        datasets: dict[str, pd.DataFrame],
    ) -> None:
        """
        Uploads monthly datasets to S3.

        This method iterates over a dictionary of DataFrames, where the key
        represents the year and month ('YYYY_MM') and the value is the
        corresponding DataFrame. For each dataset, it builds the S3 key
        and uploads the file to the bucket.

        Args:
        datasets : dict[str, pd.DataFrame]
            Dictionary where keys follow the 'YYYY_MM' format and values
            are pandas DataFrames containing monthly data.

        Returns:
        None
            Uploads all provided datasets to S3.
        """
        for year_month, df in datasets.items():

            year, month = self._parse_year_month(year_month)
            year_int = int(year)

            if self.start_year is not None and year_int < self.start_year:
                continue

            if self.end_year is not None and year_int > self.end_year:
                continue

            bucket_name = self.bucket_prefix
            filename = f"adausdt_daily_{year}_{month}.csv"

            s3_key = self._build_s3_key(year, month, filename)

            self._upload_dataframe(
                df=df,
                bucket_name=bucket_name,
                s3_key=s3_key,
            )

            print(f"Uploaded {filename} to s3://{bucket_name}/{s3_key}")