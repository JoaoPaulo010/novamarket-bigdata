"""
DAG da NovaMarket: orquestra o lote RAW -> TRUSTED -> REFINED.

check_input -> raw_to_trusted -> trusted_to_refined -> quality_check -> publish

- retries=2: se uma tarefa falhar, o Airflow tenta de novo sozinho
- schedule @hourly: elimina a execução manual dos scripts
- logs de cada tarefa ficam na interface do Airflow (monitoramento de falhas)
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = "/opt/project"
DATALAKE = f"{PROJECT}/datalake"


def check_input():
    path = f"{DATALAKE}/raw/orders"
    files = [f for f in os.listdir(path) if f.endswith(".json")] if os.path.isdir(path) else []
    if not files:
        raise ValueError("Nenhum arquivo na camada RAW. O streaming está rodando?")
    print(f"OK: {len(files)} arquivos encontrados na RAW")


def quality_check():
    import pandas as pd
    df = pd.read_parquet(f"{DATALAKE}/trusted/orders")
    checks = {
        "tem_linhas": len(df) > 0,
        "sem_duplicados": df["event_id"].is_unique,
        "preco_positivo": bool((df["price"] > 0).all()),
        "sem_categoria_nula": bool(df["category"].notna().all()),
    }
    print(f"Linhas na TRUSTED: {len(df)} | checagens: {checks}")
    falhas = [k for k, ok in checks.items() if not ok]
    if falhas:
        raise ValueError(f"Qualidade reprovada: {falhas}")


def publish():
    info = {"publicado_em": datetime.now().isoformat(),
            "tabelas": sorted(os.listdir(f"{DATALAKE}/refined"))}
    with open(f"{DATALAKE}/refined/_PUBLISHED.json", "w") as f:
        json.dump(info, f, indent=2)
    print(f"Publicado: {info}")


default_args = {"owner": "novamarket", "retries": 2, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="novamarket_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["novamarket", "bigdata"],
) as dag:
    t1 = PythonOperator(task_id="check_input", python_callable=check_input)
    t2 = BashOperator(task_id="raw_to_trusted",
                      bash_command=f"python {PROJECT}/spark/batch_trusted.py")
    t3 = BashOperator(task_id="trusted_to_refined",
                      bash_command=f"python {PROJECT}/spark/batch_refined.py")
    t4 = PythonOperator(task_id="quality_check", python_callable=quality_check)
    t5 = PythonOperator(task_id="publish", python_callable=publish)

    t1 >> t2 >> t3 >> t4 >> t5
