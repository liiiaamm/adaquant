#!/bin/bash
set -e

# Run the ingestion ETL pipeline as a module
python -m scripts.run_ingestation_pipeline "$@"
