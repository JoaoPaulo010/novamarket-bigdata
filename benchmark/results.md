# Resultados do benchmark

| Aspecto | Antes | Alteração | Tempo antes | Tempo depois | Ganho | Tarefas antes/depois | Shuffle antes/depois (MB) |
|---|---|---|---|---|---|---|---|
| Formato | JSON | Parquet | 2.22 s | 1.29 s | 42% | 232 / 217 | 0.01 / 0.01 |
| Join | sort-merge (alto shuffle) | broadcast join | 3.17 s | 1.28 s | 60% | 432 / 232 | 6.63 / 1.3 |
| Partições | 200 partições (padrão) | 8 partições | 1.4 s | 0.44 s | 69% | 216 / 24 | 6.02 / 5.36 |

Tamanho em disco: JSON = 110.5 MB | Parquet = 11.4 MB

Linhas: 1,000,000 | cores: 16 | AQE desligado | tempo = mediana de 3 execuções
