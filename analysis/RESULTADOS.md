# Resultados

Resumo executivo da execução do pipeline sobre as corridas de táxi de Nova York,
janeiro a maio de 2023, no Databricks Free Edition com compute serverless.

## Contexto da execução

| Item | Valor |
|---|---|
| Período ingerido | Janeiro a Maio de 2023 |
| Tipos de corrida | `yellow`, `green` |
| Arquivos na landing | 10 (5 yellow + 5 green) |
| Corridas na trusted (yellow) | 16.186.386 |
| Corridas aprovadas no fato (yellow) | 16.038.626 |
| Corridas em quarentena (yellow) | 147.760 — 0,91% |

Volumetria por mês de competência na camada raw (yellow):

| Mês do arquivo | Corridas |
|---|---|
| 2023-01 | 3.066.766 |
| 2023-02 | 2.913.955 |
| 2023-03 | 3.403.766 |
| 2023-04 | 3.288.250 |
| 2023-05 | 3.513.649 |

Cada partição veio de exatamente um arquivo de origem, confirmado pela coluna de
linhagem `_source_file`.

## Qualidade de dados

Motivos registrados em `ifood_case.refined.rej_taxi_trip`:

| Regra | Bloqueante | Ocorrências |
|---|---|---|
| `total_amount_negativo` | sim | 142.323 |
| `duracao_nao_positiva` | sim | 6.596 |
| `fora_da_janela_de_analise` | sim | 113 |
| `duracao_acima_de_24h` | sim | 94 |
| `pickup_datetime_nulo` | sim | 0 |
| `dropoff_datetime_nulo` | sim | 0 |
| `total_amount_nulo` | sim | 0 |
| `passenger_count_ausente` | não | não bloqueia |

Menos de 1% da base saiu das análises, e nenhuma linha foi apagada: todas estão na
quarentena com o motivo, auditáveis e reprocessáveis.

Os 113 registros fora da janela são o caso mais ilustrativo. São corridas com
timestamp corrompido na origem — datas de 2001, 2008 e similares. São poucos, mas
sem esse filtro apareceriam como meses fantasma no resultado da Pergunta 1: um
`GROUP BY` por mês de embarque devolveria linhas como `2001-01` ao lado dos meses
reais.

## Pergunta 1 — média de `total_amount` por mês (yellow)

> Qual a média de valor total (`total_amount`) recebido em um mês considerando
> todos os yellow táxis da frota?

O enunciado admite duas leituras, com números diferentes. Ambas abaixo.

### Leitura A · ticket médio por corrida

| Mês | Corridas | Ticket médio (US$) | Mediana (US$) | Receita do mês (US$) |
|---|---|---|---|---|
| 2023-01 | 3.040.380 | 27,44 | 20,16 | 83.440.046,19 |
| 2023-02 | 2.887.817 | 27,33 | 20,30 | 78.933.892,69 |
| 2023-03 | 3.372.428 | 28,26 | 20,64 | 95.313.160,53 |
| 2023-04 | 3.257.297 | 28,76 | 21,00 | 93.673.461,72 |
| 2023-05 | 3.480.704 | 29,46 | 21,48 | 102.538.428,01 |

**Ticket médio do período: US$ 28,30**

### Leitura B · faturamento médio mensal da frota yellow

| Métrica | Valor |
|---|---|
| Meses considerados | 5 |
| Receita total do período | US$ 453.898.989,14 |
| **Receita média mensal** | **US$ 90.779.797,83** |
| Corridas médias por mês | 3.207.725 |

### Análise de sensibilidade aos valores negativos

Quanto a decisão de quarentenar os `total_amount` negativos altera a resposta:

| Cenário | Corridas | Ticket médio (US$) | Receita (US$) |
|---|---|---|---|
| Aprovadas (fato) | 16.038.626 | 28,3004 | 453.898.989,14 |
| Aprovadas + negativos | 16.180.008 | 27,8375 | 450.410.956,55 |

As 141.382 corridas com valor negativo deslocam o ticket médio em -1,64% e a
receita em -0,77%. O impacto é pequeno, então a decisão é segura para publicar.
Se a área de negócio confirmar que esses registros são estornos que devem compor
a receita líquida, basta reincorporá-los a partir da quarentena — nenhum dado foi
perdido.

### Interpretação

O ticket médio sobe de forma contínua e suave: de US$ 27,44 em janeiro para
US$ 29,46 em maio, alta de 7,3% em cinco meses. É um movimento gradual, sem
saltos, compatível com reajuste tarifário e inflação — não com mudança de
comportamento do passageiro.

A receita mensal oscila muito mais: de US$ 78,9 milhões a US$ 102,5 milhões, 30%
de variação. Como o preço por corrida quase não muda, essa oscilação é explicada
quase inteiramente por **volume**. Fevereiro é o menor mês do período por ter
menos dias; maio é o maior.

Vale registrar que a mediana fica consistentemente uns US$ 7 a 8 abaixo da média
(US$ 21,48 contra US$ 29,46 em maio). É a assinatura de uma cauda de corridas
caras — aeroporto, tarifas negociadas, viagens longas — puxando a média para
cima. Para acompanhamento operacional da corrida típica, a mediana descreve
melhor a realidade.

A distinção entre as duas leituras importa na prática: para monitorar saúde de
preço, a métrica é o ticket médio; para planejar receita, é o faturamento — e o
que precisa ser previsto é o número de corridas, não o valor de cada uma.

## Pergunta 2 — média de `passenger_count` por hora (maio/2023)

> Qual a média de passageiros (`passenger_count`) por cada hora do dia que pegaram
> táxi no mês de maio considerando todos os táxis da frota?

**Escopo — "toda a frota":** yellow + green. As bases FHV e High Volume FHV da TLC
(Uber, Lyft) não são táxis licenciados e sequer publicam `passenger_count`.

**Filtro:** corridas com `passenger_count` nulo ou zero saem desta análise
específica, não da base. No agregado horário, a diferença entre `trip_count` e
`trips_with_passenger_count` é de cerca de 8%; tratar esses registros como
corridas sem ninguém dentro afundaria a média artificialmente.

### Leitura A · ocupação média por corrida

| Hora | Corridas | Média de passageiros |
|---|---|---|
| 00 | 89.604 | 1,4269 |
| 01 | 58.249 | 1,4366 |
| 02 | 37.563 | 1,4543 |
| 03 | 24.547 | 1,4498 |
| 04 | 16.074 | 1,4037 |
| 05 | 18.566 | 1,2846 |
| 06 | 46.481 | 1,2617 |
| 07 | 94.141 | 1,2814 |
| 08 | 128.361 | 1,2937 |
| 09 | 144.061 | 1,3110 |
| 10 | 156.646 | 1,3465 |
| 11 | 170.683 | 1,3615 |
| 12 | 183.864 | 1,3747 |
| 13 | 187.785 | 1,3830 |
| 14 | 204.579 | 1,3880 |
| 15 | 209.228 | 1,3992 |
| 16 | 209.709 | 1,3961 |
| 17 | 228.829 | 1,3871 |
| 18 | 242.994 | 1,3812 |
| 19 | 217.742 | 1,3900 |
| 20 | 193.101 | 1,3996 |
| 21 | 196.885 | 1,4183 |
| 22 | 181.643 | 1,4268 |
| 23 | 141.613 | 1,4214 |

O mesmo número calculado direto do fato, sem passar pelo agregado, bate linha a
linha — o que valida a tabela agregada.

### Leitura B · passageiros por hora num dia típico de maio

Maio tem 31 dias, e todas as 24 horas têm dado nos 31 dias.

| Hora | Passageiros no mês | Passageiros / dia típico |
|---|---|---|
| 00 | 127.852 | 4.124,3 |
| 01 | 83.682 | 2.699,4 |
| 02 | 54.627 | 1.762,2 |
| 03 | 35.588 | 1.148,0 |
| 04 | 22.563 | 727,8 |
| 05 | 23.849 | 769,3 |
| 06 | 58.644 | 1.891,7 |
| 07 | 120.634 | 3.891,4 |
| 08 | 166.063 | 5.356,9 |
| 09 | 188.857 | 6.092,2 |
| 10 | 210.925 | 6.804,0 |
| 11 | 232.387 | 7.496,4 |
| 12 | 252.750 | 8.153,2 |
| 13 | 259.702 | 8.377,5 |
| 14 | 283.949 | 9.159,6 |

### Abertura por tipo de táxi

| Hora | Yellow | Green |
|---|---|---|
| 00 | 1,4274 | 1,3794 |
| 01 | 1,4380 | 1,3289 |
| 02 | 1,4554 | 1,3808 |
| 03 | 1,4524 | 1,3165 |
| 04 | 1,4049 | 1,3477 |
| 05 | 1,2844 | 1,2895 |
| 06 | 1,2613 | 1,2790 |
| 07 | 1,2821 | 1,2571 |
| 08 | 1,2957 | 1,2106 |
| 09 | 1,3121 | 1,2600 |
| 10 | 1,3477 | 1,2874 |
| 11 | 1,3623 | 1,3234 |
| 12 | 1,3762 | 1,2963 |

### Interpretação

A resposta numérica é que a ocupação média fica entre 1,2617 e 1,4543 passageiro
por corrida. Mas o dado interessante não é o intervalo — é **onde** cada extremo
cai, e o formato da curva.

A ocupação desenha um **U ao longo do dia**, e ele é quase o espelho da curva de
volume. Entre 0h e 4h o volume é o menor do dia (16.074 corridas às 4h), mas a
ocupação é a mais alta: 1,4543 às 2h. São grupos voltando juntos de bar,
restaurante, evento. Às 6h acontece o inverso exato: a ocupação despenca para
1,2617, o mínimo absoluto, porque é o passageiro solitário indo trabalhar. A
partir daí ela sobe de volta ao longo do dia e fecha a noite novamente acima de
1,42 às 22h.

Ou seja: **o horário muda o tipo de viagem, não apenas a quantidade.** A
madrugada e a noite são sociais e compartilhadas; o início da manhã é individual
e pendular.

Isso tem consequência prática direta. Dimensionar frota pela ocupação média seria
um erro grosseiro — pela Leitura A, 4h da manhã (1,4037) e 18h (1,3812) parecem
praticamente equivalentes. Pela Leitura B, que é a que importa para operação, às
4h circulam 16.074 corridas e às 18h circulam 242.994: **quinze vezes mais**. As
duas leituras respondem perguntas diferentes, e por isso as duas foram entregues.

Yellow e green têm perfis parecidos, com o yellow consistentemente um pouco mais
ocupado na madrugada — coerente com a concentração do yellow em Manhattan, onde
está a vida noturna, enquanto o green atende os boroughs externos.

## Como reproduzir

```sql
-- Pergunta 1
SELECT reference_month, trip_count,
       ROUND(avg_total_amount_per_trip, 2) AS ticket_medio,
       ROUND(total_revenue, 2) AS receita
FROM ifood_case.refined.agg_trip_monthly
WHERE trip_type = 'yellow'
ORDER BY reference_month;

-- Pergunta 2
SELECT LPAD(pickup_hour, 2, '0') AS hora,
       SUM(trips_with_passenger_count) AS corridas,
       ROUND(SUM(total_passengers) / SUM(trips_with_passenger_count), 4) AS media_passageiros
FROM ifood_case.refined.agg_trip_hourly
WHERE pickup_year = '2023' AND pickup_month = '05'
GROUP BY pickup_hour
ORDER BY pickup_hour;

-- Qualidade
SELECT motivo, COUNT(*) AS linhas
FROM ifood_case.refined.rej_taxi_trip
LATERAL VIEW explode(_rejection_reasons) AS motivo
GROUP BY motivo ORDER BY linhas DESC;
```