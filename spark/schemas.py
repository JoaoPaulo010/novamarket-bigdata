"""Schema único dos eventos de pedido (usado no streaming e no batch)."""
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("campaign", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("event_time", StringType()),
])
