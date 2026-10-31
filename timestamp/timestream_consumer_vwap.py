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
TOPIC = "imat3a-ADA-VWAP"
GROUP_ID = "imat3a_group1"

# ── Timestream ─────────────────────────────────────────────────────────────────
REGION = "eu-west-1"
DATABASE = "imat3a_crypto_rt"
VWAP_TABLE = "ADATable_vwap"


def now_epoch_ms():
    return str(int(datetime.now(timezone.utc).timestamp() * 1000))


def iso_to_epoch_ms(value):
    if not value:
        return now_epoch_ms()

    clean_value = str(value).strip().lstrip("@").replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean_value)
    return str(int(dt.timestamp() * 1000))


def build_vwap_record(record_value, topic, record_key):
    vwap_value = record_value.get("vwap")
    if vwap_value is None:
        raise Exception("No hay campo 'vwap'")

    window_start = record_value.get("window_start")
    window_end = record_value.get("window_end")

    if not window_end:
        raise Exception("No hay 'window_end'")

    time_value = iso_to_epoch_ms(window_end)

    # Symbol fallback
    symbol = record_value.get("symbol")
    if not symbol:
        if record_key:
            symbol = str(record_key) + "USDT"
        else:
            symbol = "ADAUSDT"

    window_start_clean = str(window_start).strip().lstrip("@") if window_start else ""
    window_end_clean = str(window_end).strip().lstrip("@")

    return {
        "Dimensions": [
            {"Name": "symbol", "Value": symbol},
            {"Name": "source_topic", "Value": topic},
            {"Name": "window_start", "Value": window_start_clean},
            {"Name": "window_end", "Value": window_end_clean},
        ],
        "MeasureName": "vwap",
        "MeasureValue": str(float(vwap_value)),
        "MeasureValueType": "DOUBLE",
        "Time": time_value,
        "TimeUnit": "MILLISECONDS",
        "Version": int(time.time() * 1000),
    }


def main():
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

    print("Escuchando topic '{}' en {}...".format(TOPIC, BOOTSTRAP_SERVERS))
    print("Escribiendo en Timestream: {}.{}".format(DATABASE, VWAP_TABLE))
    print("Ctrl+C para salir\n")

    try:
        for record in consumer:
            try:
                data = record.value

                print(
                    "[{} -> {}] key={} | symbol={} | vwap={} | offset={}".format(
                        data.get("window_start"),
                        data.get("window_end"),
                        record.key,
                        data.get("symbol"),
                        data.get("vwap"),
                        record.offset,
                    )
                )

                vwap_record = build_vwap_record(data, TOPIC, record.key)

                resp = ts.write_records(
                    DatabaseName=DATABASE,
                    TableName=VWAP_TABLE,
                    Records=[vwap_record],
                )

                print("Write OK en Timestream")
                print(json.dumps(vwap_record, indent=2))
                print(json.dumps(resp, default=str, indent=2))
                print("-" * 80)

            except Exception as e:
                print("Error en offset {}: {}".format(record.offset, e))

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Conexión cerrada.")


if __name__ == "__main__":
    main()
