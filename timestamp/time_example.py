#!/usr/bin/env python3
"""
Prueba minima de escritura en Amazon Timestream.
Escribe un registro de prueba en las tablas de Timestream:
- btc_quotes_raw -> En esta tabla vamos a meter el parámetro de la cotización o el valor "close"
- btc_vwap_5m -> n esta tabla vamos a meter el kpi "vwap"
"""

import json
import time
from datetime import datetime, timezone

import boto3

# Parametros de prueba (edita estos valores para poner los correspondientes a vuestro grupo)
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
QUOTES_TABLE = "btc_quotes_raw"
VWAP_TABLE = "btc_vwap_5m"
SYMBOL = "BTCUSDT"
TEST_CLOSE = 74240.12
TEST_VWAP = 74229.99
WINDOW_START = "2026-03-25T15:10:00.000Z"
WINDOW_END = "2026-03-25T15:15:00.000Z"


def now_epoch_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def iso_to_epoch_ms(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return str(int(dt.timestamp() * 1000))


def main() -> None:
    ts = boto3.client("timestream-write", region_name=REGION)

    # Registro de prueba para cotizacion (btc_quotes_raw)
    quote_record = {
        "Dimensions": [
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "source_topic", "Value": "imat3a-BTC"},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "event_ts", "Value": WINDOW_END},
        ],
        "MeasureName": "close",
        "MeasureValue": str(float(TEST_CLOSE)),
        "MeasureValueType": "DOUBLE",
        "Time": iso_to_epoch_ms(WINDOW_END),
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }

    # Registro de prueba para VWAP (btc_vwap_5m)
    vwap_record = {
        "Dimensions": [
            {"Name": "symbol", "Value": SYMBOL},
            {"Name": "window_start", "Value": WINDOW_START},
            {"Name": "window_end", "Value": WINDOW_END},
            {"Name": "source_topic", "Value": "imat3a-BTC-VWAP-test"},
        ],
        "MeasureName": "vwap",
        "MeasureValue": str(float(TEST_VWAP)),
        "MeasureValueType": "DOUBLE",
        "Time": iso_to_epoch_ms(WINDOW_END),
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000) + 1,
    }

    quote_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=QUOTES_TABLE,
        Records=[quote_record],
    )
    vwap_resp = ts.write_records(
        DatabaseName=DATABASE,
        TableName=VWAP_TABLE,
        Records=[vwap_record],
    )

    print("Write OK (2 tablas)")
    print(f"Database={DATABASE} Region={REGION}")
    print(f"Tabla {QUOTES_TABLE} record:")
    print(json.dumps(quote_record, indent=2))
    print("Response:")
    print(json.dumps(quote_resp, default=str, indent=2))
    print(f"Tabla {VWAP_TABLE} record:")
    print(json.dumps(vwap_record, indent=2))
    print("Response:")
    print(json.dumps(vwap_resp, default=str, indent=2))


if __name__ == "__main__":
    main()
