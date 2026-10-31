# -*- coding: utf-8 -*-
import os
import json
from datetime import datetime
from binance import Client, ThreadedWebsocketManager
from kafka import KafkaProducer

# ── Configuración ──────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
USERNAME = os.environ.get("KAFKA_USERNAME", "")
PASSWORD = os.environ.get("KAFKA_PASSWORD", "")
SYMBOL = "ADAUSDT"          # Símbolo Binance completo
TOPIC = "imat3a-ADA"        # Topic Kafka correspondiente
INTERVAL = Client.KLINE_INTERVAL_1MINUTE
# ──────────────────────────────────────────────────────────────────────────────


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=PASSWORD,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
    )


def handle_kline(msg):
    k = msg.get("k")
    if k is None:
        return

    # Solo procesar velas cerradas
    if not k["x"]:
        return

    close_time_ms = k["T"]
    timestamp_str = datetime.utcfromtimestamp(close_time_ms / 1000).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    symbol = k["s"]
    key = symbol.replace("USDT", "").replace("USD", "")

    message = {
        "symbol": symbol,
        "@timestamp": "@"+timestamp_str,
        "close": float(k["c"]),
        "volume": float(k["v"]),
    }

    print(
        f"[VELA CERRADA] {symbol} | {timestamp_str} | "
        f"close={message['close']} | vol={message['volume']}"
    )

    producer.send(
        topic=TOPIC,
        key=key,
        value=message,
        timestamp_ms=close_time_ms,
    )
    producer.flush()

    print(
        f"[KAFKA] Publicado en topic {TOPIC} | key={key} | timestamp_ms={close_time_ms}"
    )


if __name__ == "__main__":
    producer = build_producer()

    twm = ThreadedWebsocketManager()
    twm.start()

    twm.start_kline_socket(
        symbol=SYMBOL,
        interval=INTERVAL,
        callback=handle_kline,
    )

    try:
        input("Pulsa ENTER para salir\n")
    except KeyboardInterrupt:
        pass
    finally:
        twm.stop()
        producer.flush()
        producer.close()
        print("Conexión cerrada.")
