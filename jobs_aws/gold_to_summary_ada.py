"""
gold_to_summary_ada.py
AWS Glue Job — Capa Oro → Capa Resumen (ADA)

Agrega toda la capa Gold por símbolo, año y mes, calculando:
  - max_close, min_close, avg_close, stddev_close
  - max_volume, avg_volume
  - days_overbought   : días con RSI > 70              (NULL si rsi no existe)
  - days_oversold     : días con RSI < 30              (NULL si rsi no existe)
  - days_above_sma200 : días con close > sma_200       (NULL si sma_200 no existe)
  - days_below_sma200 : días con close < sma_200       (NULL si sma_200 no existe)
  - golden_crosses    : cruces al alza EMA50/SMA200    (NULL si ema_50/sma_200 no existen)
  - death_crosses     : cruces a la baja EMA50/SMA200  (NULL si ema_50/sma_200 no existen)
  - max_atr, avg_atr                                   (NULL si atr_14 no existe)

Las columnas KPI opcionales se fijan a NULL cuando los datos Gold provienen de
una versión anterior del job SilverToGold que no las calculaba todavía.
El schema de salida es siempre completo e idéntico independientemente de qué
columnas haya en los datos fuente.

Lógica de cruces (idéntica a kafka/sns_alerts.py):
  Golden Cross : current_ema50 >  current_sma200  AND  prev_ema50 <= prev_sma200
  Death Cross  : current_ema50 <  current_sma200  AND  prev_ema50 >= prev_sma200
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# INICIALIZACIÓN GLUE
# =============================================================================
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# =============================================================================
# CONFIGURACIÓN
# =============================================================================
GLUE_DATABASE   = "trade_data_imat3a02"
GOLD_S3_PATH    = "s3://adaquant-data-bucket/gold/ada/"
SUMMARY_TABLE   = "summary_ada_ada"
SUMMARY_S3_PATH = "s3://adaquant-data-bucket/summary/ada/"

# Columnas KPI que pueden no existir si los datos Gold fueron generados por
# una versión antigua de SilverToGold_KPIs_ADA.
OPTIONAL_KPI_COLS = {"rsi", "sma_200", "ema_50", "atr_14"}

# =============================================================================
# LECTURA DESDE S3 (capa Oro)
#
# Se lee directamente desde S3 con recurse=True para evitar depender del
# schema registrado en el catálogo, que puede estar desactualizado.
# =============================================================================
gold_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [GOLD_S3_PATH],
        "recurse": True,
    },
    format="parquet",
    format_options={"mergeSchema": "true"},
    transformation_ctx="gold_dyf",
)

df = gold_dyf.toDF()

# Columnas presentes en los datos (se usa para guardias defensivas más abajo)
existing_cols = set(df.columns)

# Log del schema real para facilitar debugging en CloudWatch
print(">>> SCHEMA REAL DE LA CAPA GOLD:")
df.printSchema()
print(f">>> COLUMNAS DISPONIBLES: {sorted(existing_cols)}")
print(f">>> COLUMNAS KPI OPCIONALES ENCONTRADAS: {sorted(OPTIONAL_KPI_COLS & existing_cols)}")
print(f">>> COLUMNAS KPI OPCIONALES AUSENTES:    {sorted(OPTIONAL_KPI_COLS - existing_cols)}")

# =============================================================================
# TIPADO EXPLÍCITO
# Solo se castean columnas que existen en los datos.
# =============================================================================
df = (
    df
    .withColumn("trade_ts", F.to_timestamp("trade_ts"))
    .withColumn("close",    F.col("close").cast("double"))
    .withColumn("volume",   F.col("volume").cast("double"))
    .filter(F.col("trade_ts").isNotNull())
    # year y month se derivan de trade_ts porque las columnas de partición
    # no están embebidas en los ficheros Parquet, solo en la ruta S3.
    .withColumn("year",  F.year("trade_ts"))
    .withColumn("month", F.month("trade_ts"))
)

for kpi_col in ["rsi", "sma_200", "ema_50", "atr_14"]:
    if kpi_col in existing_cols:
        df = df.withColumn(kpi_col, F.col(kpi_col).cast("double"))
    else:
        df = df.withColumn(kpi_col, F.lit(None).cast("double"))

# =============================================================================
# DETECCIÓN DE GOLDEN CROSS Y DEATH CROSS
#
# Lógica idéntica a kafka/sns_alerts.py, traducida a Spark lag().
# Si ema_50 o sma_200 no existían en los datos originales, los flags quedan
# siempre en 0 (no se detecta ningún cruce) porque ambas columnas son NULL.
# =============================================================================
w_ordered = Window.partitionBy("symbol").orderBy("trade_ts")

df = (
    df
    .withColumn("_prev_ema50",  F.lag("ema_50").over(w_ordered))
    .withColumn("_prev_sma200", F.lag("sma_200").over(w_ordered))
)

df = (
    df
    .withColumn(
        "_golden_cross",
        F.when(
            F.col("ema_50").isNotNull()       &
            F.col("sma_200").isNotNull()      &
            F.col("_prev_ema50").isNotNull()  &
            F.col("_prev_sma200").isNotNull() &
            (F.col("ema_50")      >  F.col("sma_200"))       &
            (F.col("_prev_ema50") <= F.col("_prev_sma200")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "_death_cross",
        F.when(
            F.col("ema_50").isNotNull()       &
            F.col("sma_200").isNotNull()      &
            F.col("_prev_ema50").isNotNull()  &
            F.col("_prev_sma200").isNotNull() &
            (F.col("ema_50")      <  F.col("sma_200"))       &
            (F.col("_prev_ema50") >= F.col("_prev_sma200")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .drop("_prev_ema50", "_prev_sma200")
)

# =============================================================================
# AGREGACIÓN MENSUAL — groupBy symbol, year, month
# =============================================================================
df_summary = (
    df
    .groupBy("symbol", "year", "month")
    .agg(
        # Precio de cierre (siempre disponible)
        F.max("close").alias("max_close"),
        F.min("close").alias("min_close"),
        F.avg("close").alias("avg_close"),
        F.stddev("close").alias("stddev_close"),

        # Volumen (siempre disponible)
        F.max("volume").alias("max_volume"),
        F.avg("volume").alias("avg_volume"),

        # Señales RSI (NULL por fila si rsi ausente → sum = 0, que es correcto)
        F.sum(F.when(F.col("rsi") > 70, 1).otherwise(0)).alias("days_overbought"),
        F.sum(F.when(F.col("rsi") < 30, 1).otherwise(0)).alias("days_oversold"),

        # Posición relativa al SMA 200
        F.sum(
            F.when(F.col("close") > F.col("sma_200"), 1).otherwise(0)
        ).alias("days_above_sma200"),
        F.sum(
            F.when(F.col("close") < F.col("sma_200"), 1).otherwise(0)
        ).alias("days_below_sma200"),

        # Cruces de medias
        F.sum("_golden_cross").alias("golden_crosses"),
        F.sum("_death_cross").alias("death_crosses"),

        # ATR (NULL por fila si atr_14 ausente → max/avg = NULL, que es correcto)
        F.max("atr_14").alias("max_atr"),
        F.avg("atr_14").alias("avg_atr"),
    )
)

# =============================================================================
# ESCRITURA EN CAPA RESUMEN — Glue Data Catalog + S3 Parquet con Snappy
# =============================================================================
summary_dyf = DynamicFrame.fromDF(df_summary, glueContext, "summary_dyf")

summary_sink = glueContext.getSink(
    path=SUMMARY_S3_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
    enableUpdateCatalog=True,
    transformation_ctx="summary_sink",
)

summary_sink.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE,
    catalogTableName=SUMMARY_TABLE,
)

summary_sink.setFormat("glueparquet", compression="snappy")
summary_sink.writeFrame(summary_dyf)

job.commit()
