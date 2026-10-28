"""
TRUSTED (Parquet) -> REFINED (Parquet)

Indicadores de negócio:
  - faturamento por categoria
  - quantidade de pedidos e ticket médio
  - produtos mais vendidos (join com a dimensão de produtos)
  - vendas por campanha
Também imprime o plano de execução (explain) de uma consulta com join.
"""
import os

from pyspark.sql import SparkSession, functions as F

DATALAKE = os.getenv("DATALAKE", "/opt/project/datalake")

spark = (SparkSession.builder.appName("novamarket-trusted-to-refined")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

orders = spark.read.parquet(f"{DATALAKE}/trusted/orders")
products = spark.read.csv(f"{DATALAKE}/reference/products.csv", header=True)
out = f"{DATALAKE}/refined"

# 1) Faturamento por categoria
fat_categoria = (orders.groupBy("category")
                 .agg(F.round(F.sum("revenue"), 2).alias("faturamento"),
                      F.countDistinct("order_id").alias("pedidos"))
                 .orderBy(F.desc("faturamento")))

# 2) Quantidade de pedidos e ticket médio (por dia)
kpis = (orders.groupBy("event_date")
        .agg(F.countDistinct("order_id").alias("qtd_pedidos"),
             F.round(F.sum("revenue"), 2).alias("faturamento"))
        .withColumn("ticket_medio",
                    F.round(F.col("faturamento") / F.col("qtd_pedidos"), 2)))

# 3) Produtos mais vendidos (join com dimensão pequena -> broadcast)
top_produtos = (orders.join(F.broadcast(products.drop("category")), "product_id")
                .groupBy("product_id", "product_name")
                .agg(F.sum("quantity").alias("unidades"),
                     F.round(F.sum("revenue"), 2).alias("faturamento"))
                .orderBy(F.desc("unidades")))

# 4) Vendas por campanha
por_campanha = (orders.groupBy("campaign")
                .agg(F.countDistinct("order_id").alias("pedidos"),
                     F.round(F.sum("revenue"), 2).alias("faturamento"))
                .orderBy(F.desc("faturamento")))

print("\n===== PLANO DE EXECUÇÃO (top_produtos) =====")
top_produtos.explain(mode="formatted")

for name, df in [("faturamento_categoria", fat_categoria), ("kpis_diarios", kpis),
                 ("top_produtos", top_produtos), ("vendas_campanha", por_campanha)]:
    df.coalesce(1).write.mode("overwrite").parquet(f"{out}/{name}")
    print(f"\n[REFINED] {name}")
    df.show(10, truncate=False)

spark.stop()
