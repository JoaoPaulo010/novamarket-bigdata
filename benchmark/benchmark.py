"""
Benchmark ANTES x DEPOIS — NovaMarket

Raciocínio: Métrica -> Gargalo -> Hipótese -> Ajuste -> Nova medição

Gera um volume sintético de pedidos (padrão: 1 milhão) para que os efeitos
fiquem visíveis, e mede 3 cenários:
  1) Formato:   ler JSON           x  ler Parquet
  2) Join:      sort-merge (shuffle) x broadcast join
  3) Partições: 200 partições de shuffle (padrão) x 8 partições

Métricas coletadas pela API REST da Spark UI (a mesma informação das telas
"Jobs" e "Stages"): tempo, nº de tarefas e volume de shuffle.

O AQE (Adaptive Query Execution) é DESLIGADO de propósito, para que o Spark
não corrija sozinho os problemas e a diferença apareça na medição.

Uso:
    python /opt/project/benchmark/benchmark.py --rows 1000000
"""
import argparse
import json
import os
import shutil
import statistics
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F

OUT = os.getenv("BENCH_DIR", "/tmp/novamarket_bench")
HERE = os.path.dirname(os.path.abspath(__file__))


def ui_metrics(spark, group):
    """Soma tarefas e shuffle dos stages dos jobs de um job group (Spark UI REST)."""
    time.sleep(1.5)  # dá tempo da UI registrar os eventos
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    jobs = json.load(urllib.request.urlopen(f"{base}/jobs"))
    stage_ids = {s for j in jobs if j.get("jobGroup") == group for s in j["stageIds"]}
    tasks = shuffle = 0
    for sid in stage_ids:
        for att in json.load(urllib.request.urlopen(f"{base}/stages/{sid}")):
            if att["status"] == "COMPLETE":
                tasks += att["numTasks"]
                shuffle += att["shuffleWriteBytes"]
    return tasks, shuffle


def run(spark, name, action, repeat=3):
    """Executa `action` algumas vezes; devolve mediana do tempo e métricas da última."""
    times = []
    for i in range(repeat):
        group = f"{name}-{i}"
        spark.sparkContext.setJobGroup(group, name)
        t0 = time.perf_counter()
        action()
        times.append(time.perf_counter() - t0)
    tasks, shuffle = ui_metrics(spark, group)
    return {"tempo_s": round(statistics.median(times), 2), "tarefas": tasks,
            "shuffle_mb": round(shuffle / 1024 / 1024, 2)}


def dir_size_mb(path):
    total = sum(os.path.getsize(os.path.join(r, f))
                for r, _, fs in os.walk(path) for f in fs)
    return round(total / 1024 / 1024, 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("novamarket-benchmark")
             .config("spark.sql.adaptive.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    noop = lambda df: df.write.format("noop").mode("overwrite").save()

    # ---------- dados sintéticos ----------
    shutil.rmtree(OUT, ignore_errors=True)
    cats = F.array(*[F.lit(c) for c in ["eletronicos", "moda", "casa", "beleza", "esportes"]])
    orders = (spark.range(args.rows)
              .withColumn("order_id", F.concat(F.lit("O"), F.col("id")))
              .withColumn("product_id", (F.rand(1) * 5000).cast("int"))
              .withColumn("category", cats[(F.rand(2) * 5).cast("int")])
              .withColumn("quantity", (F.rand(3) * 3 + 1).cast("int"))
              .withColumn("price", F.round(F.rand(4) * 500 + 10, 2))
              .withColumn("campaign", F.lit("black_friday"))
              .drop("id"))
    products = (spark.range(5000).withColumnRenamed("id", "product_id")
                .withColumn("product_name", F.concat(F.lit("Produto "), F.col("product_id"))))

    orders.write.json(f"{OUT}/orders_json")
    orders.write.parquet(f"{OUT}/orders_parquet")
    products.write.parquet(f"{OUT}/products")
    results = []

    # ---------- 1) FORMATO: JSON x Parquet ----------
    q = lambda df: df.groupBy("category").agg(F.sum(F.col("price") * F.col("quantity")))
    a = run(spark, "json", lambda: noop(q(spark.read.json(f"{OUT}/orders_json"))))
    b = run(spark, "parquet", lambda: noop(q(spark.read.parquet(f"{OUT}/orders_parquet"))))
    a["tamanho_mb"], b["tamanho_mb"] = dir_size_mb(f"{OUT}/orders_json"), dir_size_mb(f"{OUT}/orders_parquet")
    results.append(("Formato", "JSON", "Parquet", a, b))

    # ---------- 2) JOIN: sort-merge x broadcast ----------
    o = spark.read.parquet(f"{OUT}/orders_parquet")
    p = spark.read.parquet(f"{OUT}/products")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # força shuffle
    smj = o.join(p, "product_id").groupBy("product_name").agg(F.sum("quantity"))
    print("\n===== ANTES: plano com SortMergeJoin =====")
    smj.explain()
    a = run(spark, "smj", lambda: noop(smj))
    bj = o.join(F.broadcast(p), "product_id").groupBy("product_name").agg(F.sum("quantity"))
    print("\n===== DEPOIS: plano com BroadcastHashJoin =====")
    bj.explain()
    b = run(spark, "bhj", lambda: noop(bj))
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
    results.append(("Join", "sort-merge (alto shuffle)", "broadcast join", a, b))

    # ---------- 3) PARTIÇÕES: 200 x 8 ----------
    agg = lambda: noop(o.groupBy("category", "product_id").agg(F.sum("price")))
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    a = run(spark, "p200", agg)
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    b = run(spark, "p8", agg)
    results.append(("Partições", "200 partições (padrão)", "8 partições", a, b))

    # ---------- relatório ----------
    lines = ["| Aspecto | Antes | Alteração | Tempo antes | Tempo depois | Ganho | "
             "Tarefas antes/depois | Shuffle antes/depois (MB) |",
             "|---|---|---|---|---|---|---|---|"]
    for asp, antes, depois, a, b in results:
        ganho = round((1 - b["tempo_s"] / a["tempo_s"]) * 100)
        lines.append(f"| {asp} | {antes} | {depois} | {a['tempo_s']} s | {b['tempo_s']} s | "
                     f"{ganho}% | {a['tarefas']} / {b['tarefas']} | "
                     f"{a['shuffle_mb']} / {b['shuffle_mb']} |")
    fmt = results[0]
    lines.append(f"\nTamanho em disco: JSON = {fmt[3]['tamanho_mb']} MB | "
                 f"Parquet = {fmt[4]['tamanho_mb']} MB")
    lines.append(f"\nLinhas: {args.rows:,} | cores: {spark.sparkContext.defaultParallelism} "
                 f"| AQE desligado | tempo = mediana de 3 execuções")
    report = "\n".join(lines)
    print("\n" + report)
    with open(os.path.join(HERE, "results.md"), "w") as f:
        f.write("# Resultados do benchmark\n\n" + report + "\n")
    spark.stop()


if __name__ == "__main__":
    main()
