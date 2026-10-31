"""
spark_job_ada_gold_pyspark.py
AWS Glue Job — Capa Plata → Capa Oro (ADA)

Indicadores calculados con puras funciones Spark (sin toPandas):
  - SMA 200          : exacto, ventana deslizante de 200 filas
  - EMA 50           : aproximación EMA truncada (ventana K_EMA50)
  - RSI 14           : versión SMA de gains/losses (paralelizable; Wilder RSI es recursivo)
  - MACD             : EMA12 - EMA26 (aproximación truncada)
  - MACD Signal      : EMA9 sobre MACD (aproximación truncada)
  - MACD Hist        : MACD - MACD_Signal
  - Bollinger Bands  : SMA20 ± 2×stddev20  → bb_upper, bb_middle, bb_lower
  - ATR 14           : media de True Range en ventana de 14 filas → atr_14
  - Stochastic 14    : %K y %D (SMA3 de %K) → stoch_k, stoch_d

Gestión del warm-up period (primeras N filas sin datos suficientes):
  Cada indicador devuelve NULL hasta tener el mínimo de filas requerido:
  - SMA_200          → NULL hasta fila 200
  - EMA_50           → NULL hasta fila 50
  - RSI_14           → NULL hasta fila 15 (necesita 14 deltas = 15 precios)
  - MACD             → NULL hasta fila 26 (necesita EMA26)
  - MACD Signal/Hist → NULL hasta fila 35 (26 para MACD + 9 para señal)
  - Bollinger Bands  → NULL hasta fila 20
  - ATR_14           → NULL hasta fila 15
  - Stochastic %K    → NULL hasta fila 14
  - Stochastic %D    → NULL hasta fila 16

Nota sobre EMA truncada:
  EMA(t) ≈ Σ decay^(n-1-i) * x_i  /  Σ decay^(n-1-i)
  donde decay = 1 - alpha,  alpha = 2 / (span + 1)
  Cuanto mayor sea K (tamaño de ventana), más se aproxima al EMA real.
  Valores recomendados: K = 5-10x el span del EMA.
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
GLUE_DATABASE = "trade_data_imat3a02"
SILVER_TABLE  = "silver_ada_ada"
GOLD_TABLE    = "gold_ada_ada"
GOLD_S3_PATH  = "s3://adaquant-data-bucket/gold/ada/"

# Tamaños de ventana para la aproximación EMA truncada.
# Regla general: K ≥ 5 × span para menos del 1% de error.
K_EMA50 = 300  # para EMA 50
K_EMA12 = 200  # para EMA 12 (MACD)
K_EMA26 = 300  # para EMA 26 (MACD)
K_EMA9  = 120  # para EMA 9  (MACD Signal)

# Mínimo de filas necesarias por indicador (warm-up period)
MIN_SMA200   = 200  # 200 precios para SMA 200
MIN_EMA50    = 50   # 50 precios para EMA 50
MIN_RSI      = 15   # 15 precios → 14 deltas para RSI 14
MIN_MACD     = 26   # 26 precios para tener EMA26
MIN_MACD_SIG = 35   # 26 (MACD) + 9 (señal) para MACD Signal y Histogram
MIN_BB20     = 20   # 20 precios para Bollinger Bands 20
MIN_ATR      = 15   # 15 precios → 14 TRs para ATR 14
MIN_STOCH_K  = 14   # 14 precios para Stochastic %K
MIN_STOCH_D  = 16   # 14 + 3 - 1 = 16 precios para Stochastic %D (SMA3 de %K)


# =============================================================================
# HELPER — EMA TRUNCADA A PARTIR DE UN ARRAY ORDENADO
# =============================================================================
def ema_from_array(arr_col: str, span: int) -> F.Column:
    """
    Calcula una EMA aproximada usando funciones higher-order de Spark SQL
    sobre un array de valores ya ordenados cronológicamente.

    Parameters
    ----------
    arr_col : str
        Nombre de la columna que contiene el array de valores (doubles).
    span : int
        Período del EMA (e.g. 50, 12, 26, 9).

    Returns
    -------
    F.Column
        Columna con el valor EMA aproximado.
    """
    alpha = 2.0 / (span + 1.0)
    decay = 1.0 - alpha

    return F.expr(f"""
        CASE
            WHEN {arr_col} IS NULL OR size({arr_col}) = 0 THEN NULL
            ELSE
                aggregate(
                    zip_with(
                        {arr_col},
                        sequence(0, size({arr_col}) - 1),
                        (x, i) -> x * pow({decay}, CAST(size({arr_col}) - 1 - i AS DOUBLE))
                    ),
                    CAST(0.0 AS DOUBLE),
                    (acc, v) -> acc + v
                )
                /
                aggregate(
                    sequence(0, size({arr_col}) - 1),
                    CAST(0.0 AS DOUBLE),
                    (acc, i) -> acc + pow({decay}, CAST(size({arr_col}) - 1 - i AS DOUBLE))
                )
        END
    """)


def collect_close_array(window) -> F.Column:
    """
    Recoge el historial de 'close' en la ventana dada como array ordenado
    cronológicamente (sort_array por trade_ts ascendente).
    """
    return F.transform(
        F.sort_array(
            F.collect_list(
                F.struct(F.col("trade_ts").alias("k"), F.col("close").alias("v"))
            ).over(window)
        ),
        lambda x: x["v"],
    )


def collect_macd_array(window) -> F.Column:
    """
    Igual que collect_close_array pero sobre la columna 'macd_line'.
    """
    return F.transform(
        F.sort_array(
            F.collect_list(
                F.struct(F.col("trade_ts").alias("k"), F.col("macd_line").alias("v"))
            ).over(window)
        ),
        lambda x: x["v"],
    )


# =============================================================================
# LECTURA DESDE CATÁLOGO GLUE (capa Plata)
# =============================================================================
silver_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name=SILVER_TABLE,
    transformation_ctx="silver_dyf",
)

df = silver_dyf.toDF()

# Tipado y limpieza básica
df = (
    df
    .withColumn("trade_ts", F.to_timestamp("trade_ts"))
    .withColumn("close",    F.col("close").cast("double"))
    .withColumn("open",     F.col("open").cast("double"))
    .withColumn("high",     F.col("high").cast("double"))
    .withColumn("low",      F.col("low").cast("double"))
    .withColumn("volume",   F.col("volume").cast("double"))
    .withColumn("day",      F.col("day").cast("long"))
    .withColumn("month",    F.col("month").cast("int"))
    .withColumn("year",     F.col("year").cast("int"))
    .filter(F.col("trade_ts").isNotNull())
)

# =============================================================================
# VENTANAS BASE
# =============================================================================
# partitionBy("symbol") → Spark procesa cada símbolo en paralelo en nodos
# distintos, sin mezclar datos entre activos y sin mover todo a un solo nodo.
w_base = Window.partitionBy("symbol").orderBy("trade_ts")

# Número de fila dentro de cada símbolo (empieza en 1).
# Se usa para el warm-up period: devolver NULL cuando no hay suficientes datos.
#
#   Fila 1        → solo hay 1 precio, ningún indicador es válido
#   Filas 1-14    → RSI no válido (faltan deltas)
#   Filas 1-25    → MACD no válido (falta EMA26)
#   Filas 1-34    → MACD Signal no válido
#   Filas 1-49    → EMA50 no válido
#   Filas 1-199   → SMA200 no válido
df = df.withColumn("_row_num", F.row_number().over(w_base))

# =============================================================================
# 1) SMA 200 — exacto
#    Warm-up: NULL en filas 1-199. Válido desde fila 200.
# =============================================================================
w_sma200 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-199, 0)

df = df.withColumn(
    "sma_200",
    F.when(
        F.col("_row_num") >= MIN_SMA200,
        F.avg("close").over(w_sma200)
    ).otherwise(F.lit(None).cast("double"))
)

# =============================================================================
# 2) EMA 50 — aproximación truncada
#    Warm-up: NULL en filas 1-49. Válido desde fila 50.
# =============================================================================
w_ema50 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-(K_EMA50 - 1), 0)

df = (
    df
    .withColumn("_close_hist_50", collect_close_array(w_ema50))
    .withColumn(
        "ema_50",
        F.when(
            F.col("_row_num") >= MIN_EMA50,
            ema_from_array("_close_hist_50", span=50)
        ).otherwise(F.lit(None).cast("double"))
    )
    .drop("_close_hist_50")
)

# =============================================================================
# 3) RSI 14 — versión SMA (paralelizable)
#
#  Wilder RSI usa una media móvil exponencial recursiva que requiere el valor
#  anterior, lo que rompe la paralelización nativa de Spark. La versión SMA
#  (average de gains/losses en ventana de 14) es equivalente en comportamiento
#  de señal y completamente vectorizable con window functions.
#
#  Warm-up: necesitamos 15 precios para tener 14 deltas completos.
#  Sin este guard, la fila 1 produce RSI=100 porque lag() devuelve NULL
#  → delta=NULL → gain=0, loss=0 → avg_loss==0 → rama RSI=100 (incorrecto).
# =============================================================================
w_rsi = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-13, 0)

df = (
    df
    .withColumn("_prev_close", F.lag("close").over(w_base))
    .withColumn("_delta",      F.col("close") - F.col("_prev_close"))
    .withColumn("_gain",  F.when(F.col("_delta") > 0,  F.col("_delta")).otherwise(F.lit(0.0)))
    .withColumn("_loss",  F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(F.lit(0.0)))
    .withColumn("_avg_gain", F.avg("_gain").over(w_rsi))
    .withColumn("_avg_loss", F.avg("_loss").over(w_rsi))
    .withColumn(
        "rsi",
        F.when(F.col("_row_num") < MIN_RSI, F.lit(None).cast("double"))  # warm-up
         .when(F.col("_avg_loss") == 0,     F.lit(100.0))                 # sin bajadas
         .otherwise(
             F.lit(100.0) - (F.lit(100.0) / (F.lit(1.0) + F.col("_avg_gain") / F.col("_avg_loss")))
         )
    )
    .drop("_prev_close", "_delta", "_gain", "_loss", "_avg_gain", "_avg_loss")
)

# =============================================================================
# 4) MACD = EMA12 - EMA26  (aproximación truncada)
#    Warm-up: NULL en filas 1-25. Válido desde fila 26.
# =============================================================================
# EMA 12
w_ema12 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-(K_EMA12 - 1), 0)
df = (
    df
    .withColumn("_close_hist_12", collect_close_array(w_ema12))
    .withColumn("_ema_12", ema_from_array("_close_hist_12", span=12))
    .drop("_close_hist_12")
)

# EMA 26
w_ema26 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-(K_EMA26 - 1), 0)
df = (
    df
    .withColumn("_close_hist_26", collect_close_array(w_ema26))
    .withColumn("_ema_26", ema_from_array("_close_hist_26", span=26))
    .drop("_close_hist_26")
)

# Línea MACD con guard de warm-up
df = df.withColumn(
    "macd_line",
    F.when(
        F.col("_row_num") >= MIN_MACD,
        F.col("_ema_12") - F.col("_ema_26")
    ).otherwise(F.lit(None).cast("double"))
).drop("_ema_12", "_ema_26")

# =============================================================================
# 5) MACD Signal = EMA9 sobre macd_line  (aproximación truncada)
#    Warm-up: NULL en filas 1-34. Válido desde fila 35.
# =============================================================================
w_ema9 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-(K_EMA9 - 1), 0)
df = (
    df
    .withColumn("_macd_hist_9", collect_macd_array(w_ema9))
    .withColumn(
        "macd_signal",
        F.when(
            F.col("_row_num") >= MIN_MACD_SIG,
            ema_from_array("_macd_hist_9", span=9)
        ).otherwise(F.lit(None).cast("double"))
    )
    .drop("_macd_hist_9")
)

# =============================================================================
# 6) MACD Histogram
#    Warm-up: NULL en filas 1-34. Válido desde fila 35.
# =============================================================================
df = df.withColumn(
    "macd_histogram",
    F.when(
        F.col("_row_num") >= MIN_MACD_SIG,
        F.col("macd_line") - F.col("macd_signal")
    ).otherwise(F.lit(None).cast("double"))
)

# =============================================================================
# 7) BOLLINGER BANDS (20-period)
#    bb_middle = SMA20 (20-day simple moving average)
#    bb_upper  = SMA20 + 2 × stddev20
#    bb_lower  = SMA20 - 2 × stddev20
#    Warm-up: NULL en filas 1-19. Válido desde fila 20.
# =============================================================================
w_bb20 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-19, 0)

df = (
    df
    .withColumn("_sma_20",    F.avg("close").over(w_bb20))
    .withColumn("_stddev_20", F.stddev("close").over(w_bb20))
    .withColumn(
        "bb_middle",
        F.when(F.col("_row_num") >= MIN_BB20, F.col("_sma_20"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "bb_upper",
        F.when(F.col("_row_num") >= MIN_BB20, F.col("_sma_20") + F.lit(2.0) * F.col("_stddev_20"))
         .otherwise(F.lit(None).cast("double"))
    )
    .withColumn(
        "bb_lower",
        F.when(F.col("_row_num") >= MIN_BB20, F.col("_sma_20") - F.lit(2.0) * F.col("_stddev_20"))
         .otherwise(F.lit(None).cast("double"))
    )
    .drop("_sma_20", "_stddev_20")
)

# =============================================================================
# 8) ATR — Average True Range (14-period)
#    True Range = max(high - low, |high - prev_close|, |low - prev_close|)
#    ATR 14     = 14-period average of True Range
#    Warm-up: NULL en filas 1-14. Válido desde fila 15.
# =============================================================================
w_atr14 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-13, 0)

df = (
    df
    .withColumn("_prev_close_atr", F.lag("close").over(w_base))
    .withColumn(
        "_true_range",
        F.greatest(
            F.col("high") - F.col("low"),
            F.abs(F.col("high") - F.col("_prev_close_atr")),
            F.abs(F.col("low")  - F.col("_prev_close_atr")),
        )
    )
    .withColumn(
        "atr_14",
        F.when(F.col("_row_num") >= MIN_ATR, F.avg("_true_range").over(w_atr14))
         .otherwise(F.lit(None).cast("double"))
    )
    .drop("_prev_close_atr", "_true_range")
)

# =============================================================================
# 9) STOCHASTIC OSCILLATOR (14-period)
#    highest_high = max(high) en ventana de 14 días
#    lowest_low   = min(low)  en ventana de 14 días
#    %K = (close - lowest_low) / (highest_high - lowest_low) × 100
#    %D = SMA3 de %K
#    Warm-up %K: NULL en filas 1-13. Válido desde fila 14.
#    Warm-up %D: NULL en filas 1-15. Válido desde fila 16.
# =============================================================================
w_stoch14 = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-13, 0)
w_stoch_d = Window.partitionBy("symbol").orderBy("trade_ts").rowsBetween(-2, 0)

df = (
    df
    .withColumn("_highest_high_14", F.max("high").over(w_stoch14))
    .withColumn("_lowest_low_14",   F.min("low").over(w_stoch14))
    .withColumn(
        "_stoch_k_raw",
        F.when(
            F.col("_row_num") >= MIN_STOCH_K,
            F.when(
                (F.col("_highest_high_14") - F.col("_lowest_low_14")) == 0,
                F.lit(50.0)
            ).otherwise(
                (F.col("close") - F.col("_lowest_low_14"))
                / (F.col("_highest_high_14") - F.col("_lowest_low_14"))
                * F.lit(100.0)
            )
        ).otherwise(F.lit(None).cast("double"))
    )
    .drop("_highest_high_14", "_lowest_low_14")
)

df = (
    df
    .withColumn("stoch_k", F.col("_stoch_k_raw"))
    .withColumn(
        "stoch_d",
        F.when(F.col("_row_num") >= MIN_STOCH_D, F.avg("_stoch_k_raw").over(w_stoch_d))
         .otherwise(F.lit(None).cast("double"))
    )
    .drop("_stoch_k_raw")
)

# =============================================================================
# SELECCIÓN FINAL DE COLUMNAS (se elimina _row_num auxiliar)
# =============================================================================
df_gold = df.select(
    "trade_ts",
    "symbol",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "day",
    "month",
    "year",
    "sma_200",
    "ema_50",
    "rsi",
    "macd_line",
    "macd_signal",
    "macd_histogram",
    "bb_upper",
    "bb_middle",
    "bb_lower",
    "atr_14",
    "stoch_k",
    "stoch_d",
)

# =============================================================================
# ESCRITURA EN CAPA ORO — Glue Data Catalog + S3 Parquet particionado
# =============================================================================
gold_dyf = DynamicFrame.fromDF(df_gold, glueContext, "gold_dyf")

gold_sink = glueContext.getSink(
    path=GOLD_S3_PATH,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
    enableUpdateCatalog=True,
    transformation_ctx="gold_sink",
)

gold_sink.setCatalogInfo(
    catalogDatabase=GLUE_DATABASE,
    catalogTableName=GOLD_TABLE,
)

gold_sink.setFormat("glueparquet", compression="snappy")
gold_sink.writeFrame(gold_dyf)

job.commit()
