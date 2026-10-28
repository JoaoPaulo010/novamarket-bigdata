# Evidências da execução — 28/10/2026

Saídas reais capturadas do ambiente local (Docker Desktop, 16 núcleos). Organizadas pelos blocos do `ROTEIRO_VIDEO.md`.

## Bloco 3 — Kafka

Tópico `orders`: 3 partições, replicação 1.

```
FIM: enviados=3000 duplicados=154 atrasados=165
```

## Bloco 4 — Streaming e evento atrasado

```
[late-demo] ATRASADO (dentro do watermark): event_time=2026-10-28T15:47:37
[late-demo] MUITO ATRASADO (fora do watermark): event_time=2026-10-28T15:30:37
```

Saída do streaming (`monitoring/streaming.log`): só o evento de 3 min entrou na agregação.

```
|{2026-10-28 15:47:00, 2026-10-28 15:48:00}|evento_atrasado|5573.31    |1      |
```

O de 20 min ficou de fora da janela em tempo real, mas está na RAW e por isso aparece na REFINED (`evento_atrasado`, 2 pedidos).

## Bloco 5 — Data Lake (log do `raw_to_trusted`)

```
[TRUSTED] lidos RAW=3002 | gravados TRUSTED=2848 | removidos (duplicados/invalidos)=154
```

Os 154 removidos são exatamente os 154 duplicados injetados pelo produtor.

### REFINED (log do `trusted_to_refined`)

```
[REFINED] faturamento_categoria
|category       |faturamento|pedidos|
|eletronicos    |1225019.66 |576    |
|esportes       |195045.48  |581    |
|casa           |180505.8   |598    |
|moda           |111649.12  |542    |
|beleza         |60781.56   |547    |
|evento_atrasado|5851.51    |2      |

[REFINED] kpis_diarios
|event_date|qtd_pedidos|faturamento|ticket_medio|
|2026-10-28|2843       |1778853.13 |625.7       |

[REFINED] top_produtos (5 primeiros)
|product_id|product_name   |unidades|faturamento|
|P006      |Toalha de Banho|618     |35169.04   |
|P009      |Tenis Corrida  |592     |112593.89  |
|P005      |Jogo de Panelas|590     |145336.76  |
|P010      |Bola Futebol   |582     |82729.79   |
|P001      |Smartphone X   |578     |1043896.95 |

[REFINED] vendas_campanha
|campaign    |pedidos|faturamento|
|frete_gratis|693    |483672.12  |
|black_friday|741    |477207.62  |
|nenhuma     |733    |418222.69  |
|cupom10     |681    |399750.7   |
```

## Bloco 6 — Airflow (log do `quality_check`)

```
Linhas na TRUSTED: 2848 | checagens: {'tem_linhas': True, 'sem_duplicados': True, 'preco_positivo': True, 'sem_categoria_nula': True}
```

Execuções da DAG `novamarket_pipeline`, todas com as 5 tarefas em success:
`scheduled__2026-10-28T14:00`, `manual__2026-10-28T15:52:58`, `scheduled__2026-10-28T15:00`.

## Bloco 7 — Gargalo do join (`benchmark.py`)

**ANTES: SortMergeJoin, com shuffle (`Exchange`) dos dois lados**

```
+- SortMergeJoin [product_id], [product_id], Inner
   :- Sort [product_id ASC]
   :  +- Exchange hashpartitioning(product_id, 200)     <- shuffle dos pedidos (1 milhão)
   :     +- FileScan parquet orders_parquet
   +- Sort [product_id ASC]
      +- Exchange hashpartitioning(product_id, 200)     <- shuffle dos produtos
         +- FileScan parquet products
```

**DEPOIS: BroadcastHashJoin, a tabela grande não é embaralhada**

```
+- BroadcastHashJoin [product_id], [product_id], Inner, BuildRight
   :- FileScan parquet orders_parquet                    <- sem Exchange
   +- BroadcastExchange                                  <- produtos enviados inteiros
      +- FileScan parquet products
```

Resultados completos em `benchmark/results.md`:

| Aspecto | Antes → Depois | Ganho | Outras métricas |
|---|---|---|---|
| Join | 3,17 s → 1,28 s | −60% | tarefas 432 → 232; shuffle 6,63 → 1,30 MB |
| Formato JSON → Parquet | 2,22 s → 1,29 s | −42% | disco 110,5 → 11,4 MB |
| Partições 200 → 8 | 1,40 s → 0,44 s | −69% | tarefas 216 → 24 |
