# -*- coding: utf-8 -*-
"""
sns_alerts.py
ADAQuant — Automatic SNS Alerts for ADA technical indicators (real-time).

Lee los últimos precios de ADA directamente desde Amazon Timestream,
calcula los indicadores técnicos en local y envía alertas SNS cuando se
dispara alguna de las siguientes condiciones:
  - RSI < 30  (sobrevendido)
  - RSI > 70  (sobrecomprado)
  - Death Cross: EMA50 cruza por debajo de SMA200
  - Golden Cross: EMA50 cruza por encima de SMA200

Flujo:
    Binance → Kafka → Timestream → este script → SNS → email

Run standalone:
    python kafka/sns_alerts.py
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import numpy as np
import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================
# SNS
SNS_REGION     = "eu-south-2"
SNS_TOPIC_NAME = "adaquant-ada-alerts"

# S3 — alert log
S3_BUCKET = "adaquant-data-bucket"

# Timestream (región donde está la base de datos de streaming)
TIMESTREAM_REGION   = "eu-west-1"
TIMESTREAM_DATABASE = "imat3a_crypto_rt"
TIMESTREAM_TABLE    = "ADATable_quotes_raw"

# Número de registros a leer (necesitamos 200+ para SMA200)
TIMESTREAM_LIMIT = 250

# Intervalo entre evaluaciones (segundos)
CHECK_INTERVAL = 60

SYMBOL = "ADAUSDT"


# =============================================================================
# STEP 1 — Verificar credenciales AWS
# =============================================================================
def check_aws_credentials() -> boto3.Session:
    print("[1/5] Checking AWS credentials...")
    try:
        session = boto3.Session()
        sts = session.client("sts", region_name=SNS_REGION)
        identity = sts.get_caller_identity()
        print(f"      Connected as: {identity['Arn']}")
        return session
    except (NoCredentialsError, PartialCredentialsError):
        print("\n  ERROR: AWS credentials not found or incomplete.")
        print("  Run: aws configure")
        print("  Region: eu-south-2  |  Output: json")
        sys.exit(1)
    except ClientError as exc:
        print(f"\n  ERROR connecting to AWS: {exc}")
        sys.exit(1)


# =============================================================================
# STEP 2 — Crear o recuperar topic SNS
# =============================================================================
def get_or_create_topic(session: boto3.Session) -> str:
    print(f"[2/5] Creating / retrieving SNS topic '{SNS_TOPIC_NAME}'...")
    sns = session.client("sns", region_name=SNS_REGION)
    response = sns.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = response["TopicArn"]
    print(f"      Topic ARN: {topic_arn}")
    return topic_arn


# =============================================================================
# STEP 3 — Suscribir email
# =============================================================================
def subscribe_email(session: boto3.Session, topic_arn: str) -> None:
    print("[3/5] Subscribing email to the SNS topic...")
    email = input("      Enter your email address (Enter to skip): ").strip()
    if not email:
        print("      Skipped.")
        return

    sns = session.client("sns", region_name=SNS_REGION)
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"      Subscription request sent to: {email}")
    print("      >>> Check your inbox and click 'Confirm subscription' <<<")


# =============================================================================
# STEP 4 — Leer precios desde Timestream y calcular indicadores
# =============================================================================
def read_from_timestream(session: boto3.Session) -> pd.DataFrame:
    print(f"[4/5] Reading latest {TIMESTREAM_LIMIT} prices from Timestream...")
    print(f"      Database : {TIMESTREAM_DATABASE}")
    print(f"      Table    : {TIMESTREAM_TABLE}")
    print(f"      Region   : {TIMESTREAM_REGION}")

    client = session.client("timestream-query", region_name=TIMESTREAM_REGION)

    query = f"""
        SELECT time, measure_value::double AS close
        FROM "{TIMESTREAM_DATABASE}"."{TIMESTREAM_TABLE}"
        WHERE measure_name = 'close'
        ORDER BY time DESC
        LIMIT {TIMESTREAM_LIMIT}
    """

    rows = []
    try:
        paginator = client.get_paginator("query")
        for page in paginator.paginate(QueryString=query):
            for row in page["Rows"]:
                time_val  = row["Data"][0]["ScalarValue"]
                close_val = float(row["Data"][1]["ScalarValue"])
                rows.append({"time": time_val, "close": close_val})
    except ClientError as exc:
        print(f"\n  ERROR querying Timestream: {exc}")
        print("  Make sure the Kafka consumer (timestamp/timestream_consumer.py)")
        print("  is running and writing data to Timestream.")
        sys.exit(1)

    if not rows:
        print("\n  ERROR: No data found in Timestream.")
        print("  Start the Kafka producer and consumer first:")
        print("    1. python kafka/binance_kafka_producer.py")
        print("    2. python timestamp/timestream_consumer.py")
        sys.exit(1)

    df = pd.DataFrame(rows)
    df["time"]  = pd.to_datetime(df["time"])
    df["close"] = pd.to_numeric(df["close"])
    df = df.sort_values("time").reset_index(drop=True)
    print(f"      Rows loaded : {len(df)}")
    print(f"      From        : {df['time'].iloc[0]}")
    print(f"      To          : {df['time'].iloc[-1]}")

    df = _compute_indicators(df)
    return df


def _compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula RSI14, EMA50 y SMA200 sobre la columna 'close'."""
    close = df["close"].values
    n     = len(close)

    # SMA 200
    df["sma_200"] = df["close"].rolling(window=200, min_periods=200).mean()

    # EMA 50
    k50    = 2.0 / (50 + 1)
    ema_50 = np.full(n, np.nan)
    if n >= 50:
        ema_50[49] = np.mean(close[:50])
        for i in range(50, n):
            ema_50[i] = close[i] * k50 + ema_50[i - 1] * (1 - k50)
    df["ema_50"] = ema_50

    # RSI 14 — Wilder
    period = 14
    rsi    = np.full(n, np.nan)
    if n > period:
        deltas   = np.diff(close)
        gains    = np.where(deltas > 0,  deltas, 0.0)
        losses   = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        rsi[period] = (
            100.0 if avg_loss == 0
            else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
        )
        for i in range(period, n - 1):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            rsi[i + 1] = (
                100.0 if avg_loss == 0
                else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
            )
    df["rsi"] = rsi

    return df


# =============================================================================
# STEP 5 — Helpers: S3 alert log upload
# =============================================================================
def _upload_alert_to_s3(session: boto3.Session, record: dict) -> None:
    now_utc = datetime.now(timezone.utc)
    ts_str  = now_utc.strftime("%Y%m%dT%H%M%SZ")
    key     = f"alerts-log/{now_utc.year:04d}/{now_utc.month:02d}/alert_{ts_str}.json"
    s3      = session.client("s3", region_name=SNS_REGION)
    s3.put_object(
        Bucket      = S3_BUCKET,
        Key         = key,
        Body        = json.dumps(record, ensure_ascii=False),
        ContentType = "application/json",
    )
    print(f"      Alert log uploaded: s3://{S3_BUCKET}/{key}")


# =============================================================================
# STEP 5 — Evaluar condiciones y publicar en SNS
# =============================================================================
def evaluate_and_alert(session: boto3.Session, df: pd.DataFrame, topic_arn: str) -> None:
    print("[5/5] Evaluating alert conditions...")

    if len(df) < 2:
        print("      Not enough rows to evaluate (need at least 2).")
        return

    current  = df.iloc[-1]
    previous = df.iloc[-2]

    current_time   = str(current["time"])
    current_price  = current["close"]
    current_rsi    = current["rsi"]
    current_ema50  = current["ema_50"]
    current_sma200 = current["sma_200"]
    prev_ema50     = previous["ema_50"]
    prev_sma200    = previous["sma_200"]

    print(f"      Latest data point : {current_time}")
    print(f"      Close             : {current_price:.6f}")
    print(f"      RSI(14)           : {current_rsi:.2f}" if pd.notna(current_rsi) else "      RSI(14)           : N/A (not enough data)")
    print(f"      EMA50             : {current_ema50:.6f}" if pd.notna(current_ema50) else "      EMA50             : N/A (not enough data)")
    print(f"      SMA200            : {current_sma200:.6f}" if pd.notna(current_sma200) else "      SMA200            : N/A (not enough data)")

    alerts = []

    if pd.notna(current_rsi) and current_rsi < 30:
        alerts.append({
            "subject":         "[ADAQuant] OVERSOLD SIGNAL: ADA RSI below 30",
            "body": (
                f"OVERSOLD SIGNAL: ADA RSI has dropped below 30 "
                f"(current value: {current_rsi:.2f})\n\n"
                f"Symbol : {SYMBOL}\n"
                f"Time   : {current_time}\n"
                f"Price  : {current_price:.6f}\n"
                f"RSI    : {current_rsi:.2f}"
            ),
            "alert_type":      "OVERSOLD",
            "indicator_value": float(current_rsi),
        })

    if pd.notna(current_rsi) and current_rsi > 70:
        alerts.append({
            "subject":         "[ADAQuant] OVERBOUGHT SIGNAL: ADA RSI above 70",
            "body": (
                f"OVERBOUGHT SIGNAL: ADA RSI has exceeded 70 "
                f"(current value: {current_rsi:.2f})\n\n"
                f"Symbol : {SYMBOL}\n"
                f"Time   : {current_time}\n"
                f"Price  : {current_price:.6f}\n"
                f"RSI    : {current_rsi:.2f}"
            ),
            "alert_type":      "OVERBOUGHT",
            "indicator_value": float(current_rsi),
        })

    if (
        pd.notna(current_ema50) and pd.notna(current_sma200)
        and pd.notna(prev_ema50) and pd.notna(prev_sma200)
        and current_ema50 < current_sma200
        and prev_ema50 >= prev_sma200
    ):
        alerts.append({
            "subject":         "[ADAQuant] DEATH CROSS DETECTED: EMA50 crossed below SMA200",
            "body": (
                f"DEATH CROSS DETECTED: EMA50 has crossed below SMA200\n\n"
                f"Symbol  : {SYMBOL}\n"
                f"Time    : {current_time}\n"
                f"Price   : {current_price:.6f}\n"
                f"EMA50   : {current_ema50:.6f}\n"
                f"SMA200  : {current_sma200:.6f}"
            ),
            "alert_type":      "DEATH_CROSS",
            "indicator_value": float(current_ema50),
        })

    if (
        pd.notna(current_ema50) and pd.notna(current_sma200)
        and pd.notna(prev_ema50) and pd.notna(prev_sma200)
        and current_ema50 > current_sma200
        and prev_ema50 <= prev_sma200
    ):
        alerts.append({
            "subject":         "[ADAQuant] GOLDEN CROSS DETECTED: EMA50 crossed above SMA200",
            "body": (
                f"GOLDEN CROSS DETECTED: EMA50 has crossed above SMA200\n\n"
                f"Symbol  : {SYMBOL}\n"
                f"Time    : {current_time}\n"
                f"Price   : {current_price:.6f}\n"
                f"EMA50   : {current_ema50:.6f}\n"
                f"SMA200  : {current_sma200:.6f}"
            ),
            "alert_type":      "GOLDEN_CROSS",
            "indicator_value": float(current_ema50),
        })

    if not alerts:
        print("      No alert conditions triggered.")
        return

    sns = session.client("sns", region_name=SNS_REGION)
    for alert in alerts:
        print(f"      ALERT TRIGGERED: {alert['subject']}")
        sns.publish(
            TopicArn=topic_arn,
            Subject=alert["subject"],
            Message=alert["body"],
        )
        print("      SNS message published.")
        record = {
            "timestamp":       datetime.now(timezone.utc).isoformat(),
            "symbol":          SYMBOL,
            "alert_type":      alert["alert_type"],
            "indicator_value": alert["indicator_value"],
            "close_price":     float(current_price),
            "date":            str(current["time"].date()),
        }
        _upload_alert_to_s3(session, record)


# =============================================================================
# MAIN — bucle continuo
# =============================================================================
def main() -> None:
    print("=" * 60)
    print("  ADAQuant — SNS Alerts (Timestream real-time)")
    print(f"  Source : {TIMESTREAM_DATABASE}.{TIMESTREAM_TABLE}")
    print(f"  Region : {TIMESTREAM_REGION}")
    print(f"  Checking every {CHECK_INTERVAL}s — Ctrl+C to stop")
    print("=" * 60)

    session   = check_aws_credentials()
    topic_arn = get_or_create_topic(session)
    subscribe_email(session, topic_arn)

    print("\nStarting monitoring loop...\n")

    cycle = 1
    try:
        while True:
            print(f"--- Cycle {cycle} | {datetime.now().strftime('%H:%M:%S')} ---")
            df = read_from_timestream(session)
            evaluate_and_alert(session, df, topic_arn)
            print(f"    Next check in {CHECK_INTERVAL}s...\n")
            time.sleep(CHECK_INTERVAL)
            cycle += 1
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")


if __name__ == "__main__":
    main()