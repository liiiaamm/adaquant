# -*- coding: utf-8 -*-
import os
import json
import time
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer

# ── Kafka ──────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC = "imat3a-ADA"
GROUP_ID = "imat3a_group1"

# ── Timestream ─────────────────────────────────────────────────────────────────
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
QUOTES_TABLE = "ADATable_quotes_raw"


def now_epoch_ms() -> str:
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def iso_to_epoch_ms(value: str) -> str:
    if not value:
        return now_epoch_ms()

    clean_value = str(value).strip().lstrip("@").replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_value)
    return str(int(dt.timestamp() * 1000))


def build_quote_record(record_value: dict, topic: str) -> dict:
    close_value = record_value.get("close")
    if close_value is None:
        raise ValueError("El mensaje no contiene el campo 'close'.")

    event_ts = record_value.get("@timestamp")
    if event_ts:
        event_ts_clean = str(event_ts).strip().lstrip("@")
        time_value = iso_to_epoch_ms(event_ts)
    else:
        event_ts_clean = datetime.now(timezone.utc).isoformat()
        time_value = now_epoch_ms()

    symbol = record_value.get("symbol", "ADAUSDT")

    return {
        "Dimensions": [
            {"Name": "symbol", "Value": symbol},
            {"Name": "source_topic", "Value": topic},
            {"Name": "event_ts", "Value": event_ts_clean},
        ],
        "MeasureName": "close",
        "MeasureValue": str(float(close_value)),
        "MeasureValueType": "DOUBLE",
        "Time": time_value,
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }


def main() -> None:
    ts = boto3.client("timestream-write", region_name=REGION)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"Escuchando topic '{TOPIC}' en {BOOTSTRAP_SERVERS}...")
    print(f"Escribiendo en Timestream: {DATABASE}.{QUOTES_TABLE}")
    print("Ctrl+C para salir\n")

    try:
        for record in consumer:
            try:
                data = record.value

                print(
                    f"[{data.get('@timestamp')}] "
                    f"key={record.key} | "
                    f"close={data.get('close')} | "
                    f"volume={data.get('volume')} | "
                    f"offset={record.offset}"
                )

                quote_record = build_quote_record(data, TOPIC)

                resp = ts.write_records(
                    DatabaseName=DATABASE,
                    TableName=QUOTES_TABLE,
                    Records=[quote_record],
                )

                print("Write OK en Timestream")
                print(json.dumps(quote_record, indent=2))
                print(json.dumps(resp, default=str, indent=2))
                print("-" * 80)

            except Exception as e:
                print(f"Error procesando/escribiendo mensaje offset={record.offset}: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Conexión cerrada.")


if __name__ == "__main__":
    main()
