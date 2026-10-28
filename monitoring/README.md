# Monitoramento — evidências

| Onde | Endereço | O que mostrar |
|---|---|---|
| Spark UI | http://localhost:4040 | Aba **Structured Streaming**: taxa de entrada (linhas/s), duração de cada micro-batch e o *watermark* avançando. Aba **SQL**: plano do job com `BroadcastHashJoin`. |
| Airflow | http://localhost:8080 | Visão **Graph** da DAG `novamarket_pipeline` com as 5 tarefas verdes; log da tarefa `quality_check` mostrando as checagens. |
| Logs do job | terminal | `[TRUSTED] lidos RAW=... gravados TRUSTED=... removidos=...` mostra quantos duplicados foram eliminados. |
| Kafka | terminal | `kafka-consumer-groups.sh --describe` mostra o *lag* por partição. |

## Comportamentos explicados pelas evidências

1. **Duplicados**: o log do `raw_to_trusted` mostra que a RAW tem mais linhas que a TRUSTED — a diferença são os eventos duplicados que o produtor injeta (~5%) e que o `dropDuplicates(event_id)` remove.
2. **Atraso**: na aba Structured Streaming o watermark fica ~5 min atrás do maior `event_time` recebido. Eventos mais velhos que isso são descartados da agregação em tempo real (mas continuam salvos na RAW).
3. **Gargalo de join**: na aba SQL/Stages, o sort-merge join aparece com 2 *Exchanges* (shuffle dos dois lados); com broadcast, a tabela grande não é embaralhada.

Salve os prints desta pasta como `spark_ui_streaming.png`, `airflow_dag.png` etc.

## Comandos úteis

```bash
# lag do consumidor (o Structured Streaming usa um group id gerado pelo Spark)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --all-groups

# logs das tarefas do Airflow ficam também em
docker compose exec airflow ls /opt/airflow/logs
```
