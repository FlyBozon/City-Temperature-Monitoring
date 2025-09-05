# spark_streaming.py
# -------------------------------------------
# City Temperature Monitoring (Kafka -> Spark Structured Streaming)
# - Wykrywanie anomalii: OUT_OF_RANGE + SUDDEN_SPIKE
# - Okna czasowe 1 min (avg/std/count per city)
# - Zapis do Parquet + logi na konsolę
# -------------------------------------------

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, expr, lit, lag, abs as abs_, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --------------------------
# Konfiguracja z ENV
# --------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "city-temperatures")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # 'latest' w trybie prod
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/output")

# Progi anomalii "poza zakresem"
ANOMALY_MIN = float(os.getenv("ANOMALY_MIN", "0.0"))
ANOMALY_MAX = float(os.getenv("ANOMALY_MAX", "30.0"))

# Próg nagłego skoku/spadku
SPIKE_DELTA = float(os.getenv("SPIKE_DELTA", "8.0"))  # °C

# Parametry okien
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")

# Ile rekordów podglądu wypisywać na konsolę
CONSOLE_ROWS = int(os.getenv("CONSOLE_ROWS", "10"))

# --------------------------
# SparkSession
# --------------------------
spark = (
    SparkSession.builder
    .appName("CityTemperatureMonitoring")
    # w małych demach ograniczamy liczbę partycji shuffle
    .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "1"))
    .getOrCreate()
)

spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

# --------------------------
# Schemat wejścia (JSON)
# --------------------------
schema = StructType([
    StructField("city", StringType(), False),
    StructField("temperature", DoubleType(), False),
    StructField("ts_iso", StringType(), False),  # np. "2025-09-04T16:43:31.193167+00:00"
])

# --------------------------
# Wejście: Kafka -> DataFrame
# --------------------------
kaf = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)  # 'latest' w prod; 'earliest' do reprocessingu
    .load()
)

parsed = (
    kaf.select(from_json(col("value").cast("string"), schema).alias("json"))
       .select("json.*")
       .withColumn("event_time", to_timestamp(col("ts_iso")))
       .dropna(subset=["event_time", "temperature", "city"])
)

# Watermark do ograniczenia opóźnionych zdarzeń
stream_with_watermark = parsed.withWatermark("event_time", WATERMARK_DELAY)

# --------------------------
# Anomalie: OUT_OF_RANGE
# --------------------------
out_of_range = (
    stream_with_watermark
    .filter((col("temperature") < lit(ANOMALY_MIN)) | (col("temperature") > lit(ANOMALY_MAX)))
    .select(
        col("city"),
        col("temperature"),
        col("event_time"),
        lit("OUT_OF_RANGE").alias("anomaly_type")
    )
)

# Zapis OUT_OF_RANGE -> Parquet
out_of_range_parquet = (
    out_of_range.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", f"{OUTPUT_DIR}/anomalies_out_of_range_parquet")
    .option("checkpointLocation", f"{OUTPUT_DIR}/chk_out_of_range")
    .start()
)

# Podgląd OUT_OF_RANGE w konsoli
out_of_range_console = (
    out_of_range.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", CONSOLE_ROWS)
    .start()
)

# --------------------------
# Anomalie: SUDDEN_SPIKE (delta vs poprzedni pomiar w mieście)
# Uwaga: do wykrycia używamy foreachBatch (operacje per-mikropartia),
#        gdzie możemy użyć okna LAG po sortowaniu w ramach batcha.
# --------------------------
def detect_spikes(batch_df, batch_id: int):
    if batch_df is None or batch_df.rdd.isEmpty():
        return

    from pyspark.sql.window import Window

    w = Window.partitionBy("city").orderBy(col("event_time").asc())
    with_prev = batch_df.select("city", "temperature", "event_time") \
                        .withColumn("prev_temp", lag("temperature", 1).over(w))

    spikes = (with_prev
              .withColumn("delta", when(col("prev_temp").isNull(), lit(0.0))
                          .otherwise(abs_(col("temperature") - col("prev_temp"))))
              .filter(col("delta") >= lit(SPIKE_DELTA))
              .select(
                  col("city"),
                  col("temperature"),
                  col("event_time"),
                  lit("SUDDEN_SPIKE").alias("anomaly_type"),
              ))

    # Zapis mikropartii do Parquet (append)
    if not spikes.rdd.isEmpty():
        spikes.write.mode("append").parquet(f"{OUTPUT_DIR}/anomalies_spikes_parquet")

spikes_stream = (
    stream_with_watermark.writeStream
    .outputMode("append")  # sygnał do triggerów; właściwy zapis robimy w foreachBatch
    .foreachBatch(detect_spikes)
    .option("checkpointLocation", f"{OUTPUT_DIR}/chk_spikes")
    .start()
)

# --------------------------
# Metryki w oknach czasowych (1 min)
# --------------------------
metrics_1m = (
    stream_with_watermark
    .groupBy(window(col("event_time"), WINDOW_DURATION), col("city"))
    .agg(
        avg("temperature").alias("avg_temp"),
        expr("stddev_samp(temperature)").alias("std_temp"),
        count("*").alias("count")
    )
    .select(
        col("city"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temp"),
        col("std_temp"),
        col("count")
    )
)

metrics_parquet = (
    metrics_1m.writeStream
    .outputMode("append")
    .format("parquet")
    .option("path", f"{OUTPUT_DIR}/metrics_1m_parquet")
    .option("checkpointLocation", f"{OUTPUT_DIR}/chk_metrics_1m")
    .start()
)

# (opcjonalny) podgląd surowych rekordów – przydatne w debug
debug_console = (
    parsed.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", CONSOLE_ROWS)
    .start()
)

# --------------------------
# Czekamy na zakończenie
# --------------------------
for q in [out_of_range_parquet, out_of_range_console, spikes_stream, metrics_parquet, debug_console]:
    q.awaitTermination()
