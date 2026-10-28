"""
RAW (JSON) -> TRUSTED (Parquet)

- Faz o parse do JSON bruto com schema explícito
- Remove registros inválidos (sem event_id, preço <= 0, quantidade <= 0)
- Remove DUPLICADOS pelo event_id
- Converte tipos e grava em Parquet particionado por data do evento
"""
import os

from pyspark.sql import SparkSession, functions as F

from schemas import SCHEMA

DATALAKE = os.getenv("DATALAKE", "/opt/project/datalake")

spark = (SparkSession.builder.appName("novamarket-raw-to-trusted")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.json(f"{DATALAKE}/raw/orders")
total_raw = raw.count()

trusted = (raw
           .select(F.from_json("value", SCHEMA).alias("o"),
                   "partition", "offset", "kafka_timestamp")
           .select("o.*", "partition", "offset", "kafka_timestamp")
           .withColumn("event_time", F.to_timestamp("event_time"))
           .filter(F.col("event_id").isNotNull()
                   & (F.col("price") > 0) & (F.col("quantity") > 0))
           .dropDuplicates(["event_id"])
           .withColumn("revenue", F.round(F.col("price") * F.col("quantity"), 2))
           .withColumn("event_date", F.to_date("event_time")))

(trusted.write.mode("overwrite")
 .partitionBy("event_date")
 .parquet(f"{DATALAKE}/trusted/orders"))

total_trusted = spark.read.parquet(f"{DATALAKE}/trusted/orders").count()
print(f"[TRUSTED] lidos RAW={total_raw} | gravados TRUSTED={total_trusted} "
      f"| removidos (duplicados/invalidos)={total_raw - total_trusted}")
spark.stop()
