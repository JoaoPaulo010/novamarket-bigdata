"""
Kafka -> Structured Streaming -> transformação -> agregação -> saída

Duas consultas (queries) rodando juntas:
 1) RAW: grava cada mensagem do Kafka exatamente como chegou (JSON) em
    datalake/raw/orders, com metadados do Kafka (partição, offset, timestamp).
 2) TEMPO REAL: faturamento por categoria em janelas de 1 minuto usando
    EVENT TIME + WATERMARK de 5 minutos, exibido no console.

Uso:
    python /opt/project/spark/streaming_job.py
"""
import os

from pyspark.sql import SparkSession, functions as F
from schemas import SCHEMA

DATALAKE = os.getenv("DATALAKE", "/opt/project/datalake")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

spark = (SparkSession.builder
         .appName("novamarket-streaming")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------- Fonte: Kafka ----------
kafka_df = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", "orders")
            .option("startingOffsets", "earliest")
            .load())

# ---------- 1) RAW: dado bruto, sem tratamento ----------
raw = kafka_df.select(
    F.col("value").cast("string").alias("value"),
    F.col("key").cast("string").alias("key"),
    "partition", "offset",
    F.col("timestamp").alias("kafka_timestamp"),     # processing/ingestion time
)
raw_query = (raw.writeStream.format("json")
             .option("path", f"{DATALAKE}/raw/orders")
             .option("checkpointLocation", f"{DATALAKE}/_checkpoints/raw_orders")
             .trigger(processingTime="10 seconds")
             .start())

# ---------- 2) Transformação + agregação em tempo real ----------
orders = (kafka_df
          .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("o"),
                  F.col("timestamp").alias("processing_time"))
          .select("o.*", "processing_time")
          .withColumn("event_time", F.to_timestamp("event_time"))
          .withColumn("revenue", F.col("price") * F.col("quantity")))

realtime = (orders
            .withWatermark("event_time", "5 minutes")       # tolera 5 min de atraso
            .groupBy(F.window("event_time", "1 minute"), "category")
            .agg(F.round(F.sum("revenue"), 2).alias("faturamento"),
                 F.count("*").alias("pedidos")))

console_query = (realtime.writeStream
                 .outputMode("update")          # mostra janelas que mudaram
                 .format("console")
                 .option("truncate", "false")
                 .option("numRows", 30)
                 .trigger(processingTime="10 seconds")
                 .start())

print(">>> Streaming iniciado. Spark UI: http://localhost:4040")
spark.streams.awaitAnyTermination()
