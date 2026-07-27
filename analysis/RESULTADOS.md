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

Linhas encaminhadas à quarentena, contadas em `ifood_case.refined.rej_taxi_trip` por motivo.
**Atenção ao escopo:** as regras rodam sobre a frota inteira, então a contagem abaixo é de
yellow **mais** green, enquanto a volumetria da seção anterior é só de yellow. Uma linha pode
acumular mais de um motivo, por isso a soma dos motivos não coincide com o número de linhas
em quarentena.

| Regra bloqueante | Linhas na quarentena (frota completa) |
|---|---|
| `total_amount_negativo` | 142.323 |
| `duracao_nao_positiva` | 6.596 |
| `fora_da_janela_de_analise` | 113 |
| `duracao_acima_de_24h` | 94 |
| `pickup_datetime_nulo` | 0 |
| `dropoff_datetime_nulo` | 0 |
| `total_amount_nulo` | 0 |

A oitava regra, `passenger_count_ausente`, é não bloqueante por decisão de projeto: a corrida
sem contagem de passageiros continua válida para receita. Como ela não retira a linha do
fato, não aparece nesta tabela. A medição de todas as regras, bloqueantes ou não, com total
avaliado e percentual de reprovação, fica em `ifood_case.quality.dq_results`.

No recorte yellow, 0,91% das corridas saíram das análises. Nenhuma linha foi apagada: todas
estão na quarentena com o motivo, auditáveis e reprocessáveis.

Os 113 registros fora da janela são o caso mais ilustrativo. São corridas com timestamp
anômalo na origem, com datas de 2001, 2008 e similares. A causa não é observável a partir das
colunas disponíveis; o que se pode afirmar é que estão fora do período que o case pede. São poucos, mas
sem esse filtro apareceriam como meses fantasma no resultado da Pergunta 1: um
`GROUP BY` por mês de embarque devolveria linhas como `2001-01` ao lado dos meses
reais.

## Pergunta 1 — média de `total_amount` por mês (yellow)

> Qual a média de valor total (`total_amount`) recebido em um mês considerando
> todos os yellow táxis da frota?

A referência direta à coluna `total_amount` leva ao ticket médio por corrida agrupado por
mês, que é a Leitura A. Como o enunciado também comporta ler "valor total recebido em um mês"
como o faturamento do mês, a Leitura B traz a média das somas mensais. As duas dão números
diferentes e ambas estão abaixo.

### Leitura A · ticket médio por corrida

| Mês | Corridas | Ticket médio (US$) | Mediana (US$) | Receita do mês (US$) |
|---|---|---|---|---|
| 2023-01 | 3.040.380 | 27,44 | 20,16 | 83.440.046,19 |
| 2023-02 | 2.887.817 | 27,33 | 20,30 | 78.933.892,69 |
| 2023-03 | 3.372.428 | 28,26 | 20,64 | 95.313.160,53 |
| 2023-04 | 3.257.297 | 28,76 | 21,00 | 93.673.461,72 |
| 2023-05 | 3.480.704 | 29,46 | 21,48 | 102.538.428,01 |

**Ticket médio ponderado do período: US$ 28,30** (receita total ÷ corridas totais). A média
simples dos cinco tickets mensais é US$ 28,25.

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

As 141.382 corridas do segundo cenário são as yellow reprovadas **exclusivamente** por
`total_amount_negativo`. A diferença para as 142.323 ocorrências da tabela de qualidade tem
duas origens: aquela contagem inclui green, e algumas linhas acumulam outro motivo além do
valor negativo, o que as manteria fora do fato de qualquer forma.

Reincorporá-las desloca o ticket médio em -1,64% e a receita em -0,77%. O impacto é pequeno,
mas a semântica desses registros não está definida no enunciado: podem ser estornos,
cancelamentos ou erro de taxímetro. Por isso os dois cenários são apresentados lado a lado
em vez de um só. A decisão definitiva dependeria de validação com a área de negócio. Como nada foi apagado,
as linhas podem ser inspecionadas direto na quarentena ou reincorporadas ao fato mediante
ajuste da regra e reprocessamento.

### Interpretação

O ticket médio cai levemente de janeiro para fevereiro (US$ 27,44 para US$ 27,33) e sobe de
forma consistente de março a maio, fechando em US$ 29,46. A alta líquida no período é de
7,3%, sem saltos bruscos.

A receita oscila mais: de US$ 78,9 milhões a US$ 102,5 milhões, 30% entre o menor e o maior
mês. Decompondo o trecho de fevereiro a maio, as corridas crescem 20,5% e o ticket 7,8%,
resultando em 29,9% de receita. O volume é o fator predominante, com contribuição relevante
do ticket.

O que as cinco colunas do case **não** permitem é atribuir causa a esse crescimento. Reajuste
tarifário, inflação, mudança na distância média das viagens ou na proporção de corridas de
aeroporto produziriam todos o mesmo efeito no `total_amount`. Distinguir entre eles exigiria
distância, tarifa base e localização.

Vale registrar que a mediana fica consistentemente uns US$ 7 a 8 abaixo da média (US$ 21,48
contra US$ 29,46 em maio). A distribuição tem cauda longa à direita: um grupo pequeno de
corridas caras puxa a média para cima. Identificar o que compõe essa cauda exigiria
`trip_distance` e `RatecodeID`, que estão fora das colunas do case. Para acompanhamento
operacional da corrida típica, a mediana descreve melhor a realidade.

A distinção entre as duas leituras importa na prática: para monitorar saúde de preço, a
métrica é o ticket médio; para planejar receita, é o faturamento. Prever receita exige
modelar os dois componentes, volume e ticket. No período analisado o volume foi o
predominante, mas a variação do ticket teve contribuição relevante.

## Pergunta 2 — média de `passenger_count` por hora (maio/2023)

> Qual a média de passageiros (`passenger_count`) por cada hora do dia que pegaram
> táxi no mês de maio considerando todos os táxis da frota?

**Escopo — "todos os táxis da frota":** yellow + green. As bases FHV e High Volume FHV,
esta última cobrindo plataformas como Uber e Lyft, são categorias de serviço distintas dos
táxis de medalhão, ainda que também reguladas pela TLC, e não publicam `passenger_count`.
Ficam fora desta análise por esses dois motivos.

**Filtro:** a análise usa `passenger_count > 0`, o que exclui dois casos distintos. Os nulos
o `AVG` já ignoraria sozinho, então não mudam o resultado. Os **zeros** é que importam, e excluí-los é uma escolha, não uma consequência do `AVG`:
mantê-los puxaria as médias para baixo. A hipótese adotada é que zero representa ausência de
informação e não uma corrida sem ocupantes, mas o enunciado não define essa semântica e as
cinco colunas não permitem verificá-la. Os números abaixo são, portanto, a versão curada; a
média literal com zeros incluídos seria menor em todas as horas. A distribuição completa de
`passenger_count`, separando nulo, zero, negativo e positivo, está em
`analysis/01_analise_exploratoria.ipynb`. A exclusão vale para esta análise, não para a base.

### Leitura A · ocupação média por corrida

| Hora | Corridas com `passenger_count > 0` | Média de passageiros |
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

Maio tem 31 dias. O mínimo do dia está às 4h, com cerca de 728 passageiros por hora num dia
típico; às 14h são cerca de 9.160. O pico não está às 14h e sim no fim da tarde, acompanhando
a curva de volume de corridas. A tabela completa das 24 horas e a abertura por tipo de táxi
estão em `analysis/02_respostas.ipynb`, seções 2.B e 2.C.

### Interpretação

A resposta numérica é que a ocupação média fica entre 1,2617 e 1,4543 passageiro
por corrida. Mas o dado interessante não é o intervalo — é **onde** cada extremo
cai, e o formato da curva.

A ocupação desenha um **U ao longo do dia**, quase o espelho da curva de volume. Entre 0h e
4h o volume é o menor do dia (16.074 corridas às 4h), mas a ocupação é a mais alta: 1,4543 às
2h. Às 6h acontece o inverso: a ocupação cai para 1,2617, o mínimo absoluto, justamente
quando o volume começa a subir. A partir daí ela se recupera ao longo do dia e fecha a noite
novamente acima de 1,42 às 22h.

O padrão é claro; a explicação é hipótese. Viagens de lazer compartilhadas na madrugada e
deslocamento pendular individual pela manhã explicariam bem o formato, mas as colunas
exigidas pelo case não trazem propósito da viagem, origem, destino nem dia da semana.
Confirmar isso exigiria cruzar com `PULocationID`/`DOLocationID` e separar dia útil de fim de
semana, o que está fora do escopo pedido.

Isso tem consequência prática direta. Dimensionar frota apenas pela ocupação média seria
insuficiente: pela ocupação, 4h da manhã (1,4037) e 18h (1,3812) parecem praticamente
equivalentes. Em volume não são. Às 4h a frota registra 16.074 corridas; às 18h, 242.994.
São **15,1 vezes mais**, e a mesma proporção se reflete no total de passageiros
transportados. Ocupação e volume respondem perguntas diferentes, e por isso as duas leituras
foram entregues.

Yellow e green têm perfis parecidos. Entre 0h e 4h o yellow é o mais ocupado em todas as
horas, com diferença de até 0,14 passageiro por corrida às 3h; às 5h e 6h a relação se
inverte e o green fica ligeiramente à frente. Uma explicação possível é a diferença de área
de atuação entre os dois tipos, já que a regulação da TLC restringe o green na região central
de Manhattan, mas isso é informação externa: as colunas do case não trazem localização, e
confirmar exigiria `PULocationID`.

## Consultas principais

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

-- Qualidade (frota completa; acrescente WHERE trip_type = 'yellow' para o recorte)
SELECT motivo, COUNT(*) AS linhas
FROM ifood_case.refined.rej_taxi_trip
LATERAL VIEW explode(_rejection_reasons) AS motivo
GROUP BY motivo ORDER BY linhas DESC;
```