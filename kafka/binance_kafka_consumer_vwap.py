# -*- coding: utf-8 -*-
import os
import json
from kafka import KafkaConsumer

# ── Configuración ──────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
TOPIC = "imat3a-ADA-VWAP"
GROUP_ID = "imat3a_group1"
# ──────────────────────────────────────────────────────────────────────────────


def main() -> None:
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
    print("Ctrl+C para salir\n")

    try:
        for record in consumer:
            print(
                f"[{record.value.get('window_start')} -> {record.value.get('window_end')}] "
                f"key={record.key} | "
                f"symbol={record.value.get('symbol')} | "
                f"vwap={record.value.get('vwap')} | "
                f"offset={record.offset}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        print("Conexión cerrada.")


if __name__ == "__main__":
    main()