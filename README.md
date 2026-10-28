# NovaMarket — Do Evento ao Insight

Prova de conceito de um pipeline de Big Data **Kafka → Spark Structured Streaming → Data Lake (RAW/TRUSTED/REFINED em Parquet) → Spark SQL**, orquestrado pelo **Airflow**. Roda inteiro na máquina local com Docker.

## Arquitetura (Kappa)

```
producer.py ──► Kafka (tópico "orders", 3 partições, chave = customer_id)
                  │
                  ▼
      Spark Structured Streaming (streaming_job.py)
        ├── RAW: mensagem original em JSON ───────────────┐
        └── agregação em janela de 1 min + watermark 5 min │
            (faturamento por categoria quase em tempo real)│
                                                           ▼
   Airflow DAG (de hora em hora):  check_input → raw_to_trusted → trusted_to_refined → quality_check → publish
                                                   (Parquet, sem          (indicadores
                                                    duplicados)            em Parquet)
Monitoramento: Spark UI (4040) · Airflow UI (8080) · logs
```

Abordagem **Kappa**: existe um único caminho de entrada (o log do Kafka). O lote da DAG não é um segundo pipeline com outra fonte; ele apenas consolida o que o streaming gravou na RAW. Se a regra de negócio mudar, basta reprocessar a RAW (ou reler o tópico desde o início).

## Tecnologias

Docker Compose · Apache Kafka 3.7 (modo KRaft) · Apache Spark 3.5.1 (PySpark, modo local) · Apache Airflow 2.10 · Parquet · Python 3.11

## Estrutura

```
producer/    gerador de eventos -> Kafka
spark/       streaming_job.py (Kafka->RAW + tempo real), batch_trusted.py, batch_refined.py
airflow/     dags/novamarket_pipeline.py
datalake/    raw/ trusted/ refined/ reference/products.csv
benchmark/   benchmark.py e results.md (antes x depois)
monitoring/  guia de evidências (Spark UI, Airflow, logs)
docker/      Dockerfile (Airflow + Java + PySpark + kafka-python)
```

## Como executar

```bash
# 0) (Linux) para o container conseguir escrever na pasta do projeto
echo "AIRFLOW_UID=$(id -u)" > .env

# 1) Subir Kafka + Airflow (o primeiro build demora alguns minutos)
docker compose up -d --build

# 2) Criar o tópico com 3 partições
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

# 3) Terminal A — iniciar o streaming (Spark UI em http://localhost:4040)
docker compose exec airflow python /opt/project/spark/streaming_job.py

# 4) Terminal B — gerar eventos
docker compose exec airflow python /opt/project/producer/producer.py --events 3000 --rate 50

# 5) Ver as mensagens chegando no Kafka (opcional)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic orders --property print.key=true --max-messages 5

# 6) Demonstrar evento atrasado (com o streaming rodando)
docker compose exec airflow python /opt/project/producer/producer.py --late-demo

# 7) Airflow: http://localhost:8080  (usuário admin; senha abaixo) -> ligar e disparar a DAG
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt

# 8) Benchmark antes x depois
docker compose exec airflow python /opt/project/benchmark/benchmark.py --rows 1000000
```

Sem Airflow, o lote também roda à mão: `python spark/batch_trusted.py` e depois `python spark/batch_refined.py`.

## Ingestão (Kafka)

| Item | Decisão |
|---|---|
| Tópico | `orders` |
| Mensagem | JSON: `event_id, order_id, customer_id, product_id, category, campaign, quantity, price, event_time` |
| Chave | `customer_id` — eventos do mesmo cliente caem na mesma partição (ordem garantida por cliente) e 500 clientes distribuem bem a carga |
| Partições | 3 — permite até 3 consumidores em paralelo no mesmo grupo; em produção aumentaria conforme o throughput |
| Produtor | `kafka-python`, `acks=all` (não perde mensagem confirmada) e `linger_ms=10` (envio em lotes) |
| Consumidor | Spark Structured Streaming, `startingOffsets=earliest`, progresso salvo em checkpoint |
| Retenção | 7 dias (permite reprocessar — base da arquitetura Kappa) |

Por que não usar `category` como chave? São só 5 categorias e "eletrônicos" concentraria o faturamento: geraria partição quente (*skew*).

## Organização do Data Lake

| Camada | Formato | Conteúdo |
|---|---|---|
| `raw/orders` | JSON | mensagem exatamente como veio do Kafka + partição, offset e horário de ingestão. Nada é descartado (nem atrasados nem duplicados). |
| `trusted/orders` | Parquet, particionado por `event_date` | tipos corretos, sem inválidos, sem duplicados (`dropDuplicates(event_id)`), coluna `revenue` |
| `refined/*` | Parquet | `faturamento_categoria`, `kpis_diarios` (pedidos e ticket médio), `top_produtos`, `vendas_campanha` |

**Por que Parquet?** É colunar (lê só as colunas da consulta), comprimido e guarda o schema com tipos. No benchmark, 1 milhão de pedidos ocupou 110,5 MB em JSON e 11,4 MB em Parquet, e a mesma agregação ficou 42% mais rápida.

## Streaming: event time × processing time

- **event time**: quando o pedido aconteceu (`event_time`, gerado no produtor).
- **processing time**: quando o Spark processou a mensagem (`timestamp` do Kafka / horário do micro-batch).

A agregação usa janelas de 1 minuto por **event time** com `withWatermark("event_time", "5 minutes")`. No `--late-demo`, o evento com 3 min de atraso ainda atualiza a janela antiga (aparece a categoria `evento_atrasado` no console); o evento com 20 min de atraso é descartado da agregação, mas continua salvo na RAW.

## Otimização (resultados medidos)

Métrica → Gargalo → Hipótese → Ajuste → Nova medição. Detalhes em `benchmark/results.md` (1 milhão de linhas, AQE desligado, mediana de 3 execuções):

| Aspecto | Antes | Alteração | Tempo antes → depois | Outras métricas |
|---|---|---|---|---|
| Join | sort-merge (shuffle dos dois lados) | broadcast join | 3,17 s → 1,28 s (−60%) | tarefas 432 → 232; shuffle 6,63 MB → 1,30 MB |
| Formato | JSON | Parquet | 2,22 s → 1,29 s (−42%) | disco 110,5 MB → 11,4 MB |
| Partições | 200 partições de shuffle | 8 partições | 1,40 s → 0,44 s (−69%) | tarefas 216 → 24 |

O gargalo principal foi o **join** com a dimensão de produtos: o `explain()` mostrou `SortMergeJoin` com dois `Exchange hashpartitioning` (shuffle). Como a dimensão é pequena, a hipótese foi enviá-la inteira a cada executor (*broadcast*). Depois do ajuste o plano mostra `BroadcastHashJoin` e o shuffle caiu cerca de 80%. O `batch_refined.py` já usa `F.broadcast(products)`.

## Limitações

- Spark em modo local (um nó só); Kafka com 1 broker e replicação 1 — sem tolerância a falha de máquina.
- O streaming roda fora do Airflow (processo contínuo, iniciado à mão).
- Data Lake em disco local, sem catálogo de metadados nem controle de acesso.
- Parquet puro não tem transações ACID: se o job cair no meio da escrita da TRUSTED, a pasta pode ficar inconsistente (Delta Lake resolveria).
- Dados sintéticos; os tempos variam conforme a máquina.
