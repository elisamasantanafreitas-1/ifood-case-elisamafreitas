# Resultados

> Preencha as tabelas abaixo com os números obtidos ao rodar
> `analysis/02_respostas.ipynb`. Este arquivo é o resumo executivo do case —
> quem avalia deve conseguir entender o resultado sem abrir um notebook.

## Contexto da execução

| Item | Valor |
|---|---|
| Período ingerido | Janeiro a Maio de 2023 |
| Tipos de corrida | `yellow`, `green` |
| Arquivos na landing | _preencher_ |
| Corridas na trusted (yellow) | _preencher_ |
| Corridas na trusted (green) | _preencher_ |
| Corridas aprovadas no fato | _preencher_ |
| Corridas em quarentena | _preencher_ (_%_) |
| Data da execução | _preencher_ |

## Qualidade de dados

| Regra | Bloqueante | Linhas reprovadas | % |
|---|---|---|---|
| `fora_da_janela_de_analise` | sim | _preencher_ | _preencher_ |
| `duracao_nao_positiva` | sim | _preencher_ | _preencher_ |
| `duracao_acima_de_24h` | sim | _preencher_ | _preencher_ |
| `total_amount_negativo` | sim | _preencher_ | _preencher_ |
| `total_amount_nulo` | sim | _preencher_ | _preencher_ |
| `passenger_count_ausente` | não | _preencher_ | _preencher_ |

## Pergunta 1 — média de `total_amount` por mês (yellow)

### Leitura A · ticket médio por corrida

| Mês | Corridas | Ticket médio (US$) | Mediana (US$) | Receita do mês (US$) |
|---|---|---|---|---|
| 2023-01 | _preencher_ | _preencher_ | _preencher_ | _preencher_ |
| 2023-02 | _preencher_ | _preencher_ | _preencher_ | _preencher_ |
| 2023-03 | _preencher_ | _preencher_ | _preencher_ | _preencher_ |
| 2023-04 | _preencher_ | _preencher_ | _preencher_ | _preencher_ |
| 2023-05 | _preencher_ | _preencher_ | _preencher_ | _preencher_ |

### Leitura B · faturamento médio mensal da frota yellow

| Métrica | Valor |
|---|---|
| Meses considerados | 5 |
| Receita total do período | _preencher_ |
| **Receita média mensal** | **_preencher_** |
| Corridas médias por mês | _preencher_ |

### Sensibilidade aos valores negativos

| Cenário | Corridas | Ticket médio | Receita |
|---|---|---|---|
| Aprovadas (fato) | _preencher_ | _preencher_ | _preencher_ |
| Aprovadas + negativos | _preencher_ | _preencher_ | _preencher_ |

**Conclusão:** _preencher — o impacto foi relevante ou desprezível?_

## Pergunta 2 — média de `passenger_count` por hora (maio/2023, yellow + green)

### Leitura A · ocupação média por corrida

| Hora | Corridas | Média de passageiros |
|---|---|---|
| 00 | _preencher_ | _preencher_ |
| 01 | _preencher_ | _preencher_ |
| ... | ... | ... |
| 23 | _preencher_ | _preencher_ |

### Leitura B · passageiros por hora num dia típico

| Hora | Passageiros no mês | Passageiros / dia típico |
|---|---|---|
| 00 | _preencher_ | _preencher_ |
| ... | ... | ... |

## Leitura de negócio

_Duas ou três frases sobre o que os números dizem: em que horas a frota está mais
carregada, se a ocupação muda ao longo do dia, e como isso se relaciona com o volume
de corridas._
