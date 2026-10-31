# -*- coding: utf-8 -*-
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


# ── Configuración ─────────────────────────────────────────────

# Broker address and credentials come in as Glue job parameters
# (--KAFKA_BOOTSTRAP_SERVERS, --KAFKA_USERNAME, --KAFKA_PASSWORD), never from the code.
_kafka = getResolvedOptions(sys.argv, ["KAFKA_BOOTSTRAP_SERVERS", "KAFKA_USERNAME", "KAFKA_PASSWORD"])
BOOTSTRAP_SERVERS = _kafka["KAFKA_BOOTSTRAP_SERVERS"]
USERNAME = _kafka["KAFKA_USERNAME"]
PASSWORD = _kafka["KAFKA_PASSWORD"]

TOPIC_INPUT = "imat3a-ADA"
TOPIC_OUTPUT = "imat3a-ADA-VWAP"

CHECKPOINT = "s3://adaquant-data-bucket/checkpoints/vwap2/"

# ─────────────────────────────────────────────────────────────

JAAS_CONFIG = (
    "org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{USERNAME}" password="{PASSWORD}";'
)


# 1. Inicializar Glue

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

sc.setLogLevel("WARN")


# 2. Leer del topic Kafka de entrada

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .option("subscribe", TOPIC_INPUT) \
    .option("startingOffsets", "latest") \
    .load()


# 3. Parsear el JSON del value

schema = StructType([
    StructField("symbol", StringType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
])

df = df_raw.select(
    F.col("timestamp").alias("event_time"),
    F.from_json(
        F.col("value").cast("string"),
        schema
    ).alias("data"),
).select(
    "event_time",
    "data.symbol",
    "data.close",
    "data.volume"
)


# 4. Calcular VWAP por ventanas deslizantes de 5 min (slide 1 min)

df_vwap = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        F.window("event_time", "5 minutes", "1 minute"),
        F.col("symbol")
    ) \
    .agg(
        (
            F.sum(F.col("close") * F.col("volume"))
            / F.sum("volume")
        ).alias("vwap")
    ) \
    .select(
        F.col("symbol").alias("key"),
        F.to_json(
            F.struct(
                F.date_format(
                    F.col("window.start"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ).alias("window_start"),

                F.date_format(
                    F.col("window.end"),
                    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
                ).alias("window_end"),

                F.col("symbol"),
                F.round(
                    F.col("vwap"),
                    6
                ).alias("vwap")
            )
        ).alias("value")
    )


# 5. Escribir al topic Kafka de salida cada minuto

query = df_vwap.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", JAAS_CONFIG) \
    .option("topic", TOPIC_OUTPUT) \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("update") \
    .trigger(processingTime="1 minute") \
    .start()


query.awaitTermination()

job.commit()