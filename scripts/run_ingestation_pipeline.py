import argparse

from load_data.adaquant_load_pipe import AdaquantLoadPipe, PipelineConfig


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run ADAQuant historical data ingestion (ETL) pipeline"
    )

    ######### global args ########
    parser.add_argument("--region", default="eu-south-2", help="AWS region (e.g. eu-south-2)")
    parser.add_argument("--bucket-name", default="adaquant-data-bucket", help="S3 bucket name")
    parser.add_argument("--symbol", default="ADAUSDT", help="TradingView symbol (e.g. ADAUSDT)")
    parser.add_argument("--exchange", default="BINANCE", help="TradingView exchange (e.g. BINANCE)")
    parser.add_argument(
        "--crypto-name",
        default="ada",
        help="Crypto name used in S3 path (e.g. ada, bitcoin, ethereum)",
    )
    parser.add_argument(
        "--tiers",
        default="bronze,silver,gold",
        help="Comma-separated tiers to create in S3 structure (e.g. bronze,silver,gold)",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True,
        help="Pipeline stage to execute",
    )

    ######### load ########
    load_parser = subparsers.add_parser("load", help="Run only the data loading stage")
    load_parser.add_argument("--n-bars", type=int, default=1483)
    load_parser.add_argument("--verbose", action="store_true")

    ######## buckets ########
    buckets_parser = subparsers.add_parser(
        "buckets",
        help="Run only the S3 bucket + structure creation stage",
    )
    buckets_parser.add_argument("--start-year", type=int, default=2022)
    buckets_parser.add_argument("--end-year", type=int, default=2025)
    buckets_parser.add_argument("--verbose", action="store_true")

    ######## upload ########
    # NOTE: upload needs datasets, so this command does load+upload
    upload_parser = subparsers.add_parser("upload", help="Run load + upload stage")
    upload_parser.add_argument("--n-bars", type=int, default=1483)
    upload_parser.add_argument("--start-year", type=int, default=2022)
    upload_parser.add_argument("--end-year", type=int, default=2025)
    upload_parser.add_argument("--tier", default="bronze")
    upload_parser.add_argument("--coin", default="ada")
    upload_parser.add_argument("--verbose", action="store_true")

    ######## full pipeline ########
    full_parser = subparsers.add_parser("run", help="Run the full ingestion pipeline")
    full_parser.add_argument("--n-bars", type=int, default=1483)

    full_parser.add_argument("--buckets-start-year", type=int, default=2022)
    full_parser.add_argument("--buckets-end-year", type=int, default=2025)

    full_parser.add_argument("--upload-start-year", type=int, default=2022)
    full_parser.add_argument("--upload-end-year", type=int, default=2025)
    full_parser.add_argument("--upload-tier", default="bronze")
    full_parser.add_argument("--upload-coin", default="ada")

    full_parser.add_argument("--verbose-loader", action="store_true")
    full_parser.add_argument("--verbose-manager", action="store_true")
    full_parser.add_argument("--verbose-uploader", action="store_true")
    full_parser.add_argument("--verbose-pipeline", action="store_true")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    tiers_list = [t.strip() for t in args.tiers.split(",") if t.strip()]

    cfg = PipelineConfig(
        symbol=args.symbol,
        exchange=args.exchange,
        region=args.region,
        bucket_name=args.bucket_name,
        crypto_name=args.crypto_name,
        tiers=tiers_list,
    )

    pipe = AdaquantLoadPipe(cfg)

    if args.command == "load":
        _ = pipe.load(n_bars=args.n_bars, verbose=args.verbose)

    elif args.command == "buckets":
        pipe.ensure_buckets(
            start_year=args.start_year,
            end_year=args.end_year,
            tiers=tiers_list,
            crypto_name=args.crypto_name,
            verbose=args.verbose,
        )

    elif args.command == "upload":
        datasets = pipe.load(n_bars=args.n_bars, verbose=args.verbose)
        pipe.upload(
            datasets=datasets,
            start_year=args.start_year,
            end_year=args.end_year,
            tier=args.tier,
            coin=args.coin,
            verbose=args.verbose,
        )

    elif args.command == "run":
        pipe.run_pipeline(
            n_bars=args.n_bars,
            buckets_start_year=args.buckets_start_year,
            buckets_end_year=args.buckets_end_year,
            upload_start_year=args.upload_start_year,
            upload_end_year=args.upload_end_year,
            tiers=tiers_list,
            crypto_name=args.crypto_name,
            upload_tier=args.upload_tier,
            upload_coin=args.upload_coin,
            verbose_loader=args.verbose_loader,
            verbose_manager=args.verbose_manager,
            verbose_uploader=args.verbose_uploader,
            verbose_pipe=args.verbose_pipeline,
        )


if __name__ == "__main__":
    main()
