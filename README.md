# Case Técnico Data Architect — corridas de táxi de NY

Pipeline de ingestão, modelagem e análise das corridas de táxi de Nova York (NYC TLC),
janeiro a maio de 2023, em PySpark sobre Databricks com Delta Lake.

Os arquivos originais ficam preservados numa landing zone, as tabelas de consumo são
consultáveis por SQL, e as duas perguntas do case estão respondidas em
[`analysis/RESULTADOS.md`](analysis/RESULTADOS.md).

---

## Arquitetura

```mermaid
flowchart LR
    TLC[("NYC TLC<br/>parquet público")]

    subgraph UC["Unity Catalog · ifood_case"]
        LAND["<b>landing</b><br/>Volume<br/>arquivo original intacto"]
        RAW["<b>raw</b><br/>Delta · tudo string<br/>+ linhagem"]
        TRU["<b>trusted</b><br/>Delta · tipado<br/>colunas do case"]
        REF["<b>refined</b><br/>fato unificado<br/>+ agregados"]
        QUA["<b>quality</b><br/>resultado das regras"]
        REJ["<b>quarentena</b><br/>rej_taxi_trip"]
    end

    SQL(["Usuário final · SQL"])

    TLC -->|extractor HTTP| LAND
    LAND -->|normaliza nomes<br/>cast p/ string| RAW
    RAW -->|seleciona e tipa| TRU
    TRU -->|unifica frota<br/>aplica regras| REF
    TRU -.->|regras reprovadas| REJ
    REF --> QUA
    TRU --> SQL
    REF --> SQL
```

Cada camada tem um contrato explícito:

| Camada | Formato | Contrato |
|---|---|---|
| `landing` | Parquet original (Volume) | Byte a byte como veio da TLC. Nada é transformado. Permite reprocessar tudo do zero sem depender do site da origem. |
| `raw` | Delta | Representação normalizada e integral da origem, sem descarte de linhas. Nomes em snake_case, todas as colunas de origem em `string`, colunas de linhagem, particionada pelo mês de competência do arquivo. Nenhuma linha descartada. |
| `trusted` | Delta | Camada de consumo. Apenas as colunas exigidas pelo case, com a grafia exigida e os tipos corretos. Ainda sem filtros. |
| `refined` | Delta | Fato unificado da frota, já submetido às regras de qualidade, mais agregados analíticos. |
| `quality` | Delta | Resultado da última execução das regras: quantas linhas cada regra reprovou. |

---

## Modelo de dados

### `trusted.ny_taxi_trip_yellow` / `..._green`

| Coluna | Tipo | Descrição |
|---|---|---|
| `VendorID` | `bigint` | Provedor do registro (1 = Creative Mobile, 2 = Curb/VeriFone) |
| `passenger_count` | `double` | Passageiros informados pelo motorista |
| `total_amount` | `double` | Valor total cobrado do passageiro, em US$ |
| `tpep_pickup_datetime` / `lpep_pickup_datetime` | `timestamp` | Início da corrida |
| `tpep_dropoff_datetime` / `lpep_dropoff_datetime` | `timestamp` | Fim da corrida |
| `_source_file` | `string` | Caminho do arquivo de origem (linhagem) |
| `_ingested_at` | `timestamp` | Momento da ingestão |
| `ref_year`, `ref_month` | `string` | Partição: mês de competência do arquivo |

O prefixo `tpep`/`lpep` foi mantido porque é a nomenclatura da própria TLC, e é a grafia
que o enunciado pede para o yellow.

### `refined.fct_taxi_trip`

Fato unificado da frota. O que ele acrescenta em relação à trusted:

| Coluna | Tipo | Descrição |
|---|---|---|
| `trip_type` | `string` | `yellow` ou `green`, discriminador da frota |
| `pickup_datetime`, `dropoff_datetime` | `timestamp` | Nomes canônicos, independentes do tipo de táxi |
| `trip_duration_minutes` | `double` | Duração calculada da corrida |
| `pickup_year`, `pickup_month` | `string` | Partição: mês do evento |
| `pickup_hour` | `int` | Hora do embarque (0 a 23) |
| `pickup_date` | `date` | Data do embarque |

### Tabelas derivadas

| Tabela | Para que serve |
|---|---|
| `refined.rej_taxi_trip` | Quarentena: linhas reprovadas, com `_rejection_reasons` |
| `refined.agg_trip_monthly` | Receita, ticket médio e mediana por tipo e mês |
| `refined.agg_trip_hourly` | Corridas e passageiros por tipo, mês e hora do dia |
| `quality.dq_results` | Uma linha por regra: avaliadas, reprovadas, % |

---

## Decisões técnicas

### A raw guarda tudo como texto

Foi a primeira coisa que precisou de decisão, porque quebrou na prática. A TLC muda o tipo
físico de coluna entre meses do mesmo ano: `airport_fee` sai como `int64` em janeiro/2023 e
como `double` de fevereiro em diante. Ler os cinco arquivos juntos falha.

Materializar a raw com todas as colunas de origem em `string` faz a ingestão sobreviver a
qualquer mudança de tipo na origem, e concentra a tipagem num lugar só, a trusted, onde
temos controle do schema final.

O mesmo arquivo tem outra pegadinha do mesmo tipo: a coluna aparece como `airport_fee` em
janeiro e `Airport_fee` depois. Dava pra resolver confiando no `spark.sql.caseSensitive`
estar desligado, mas isso é depender de uma configuração de sessão que qualquer um pode
mudar. Preferi normalizar os nomes para snake_case na raw e restaurar a grafia exigida pelo
case (`VendorID`) no mapeamento da trusted, que é explícito e está coberto por teste.

### Mês do arquivo e mês da corrida são coisas diferentes

Os arquivos da TLC contêm registros com data de 2001, 2008 e até 2090, além de corridas de
meses vizinhos. Tratar as duas coisas como se fossem a mesma produz um bug silencioso:
um `GROUP BY` por mês de embarque, sem filtro, devolve linhas fantasma no resultado. Foram
113 registros nesse caso, poucos, mas suficientes para sujar a resposta da Pergunta 1.

Por isso raw e trusted particionam por `ref_year`/`ref_month`, que é a competência do
arquivo e governa o reprocessamento, enquanto a refined particiona por
`pickup_year`/`pickup_month`, que é o evento e governa a análise.

### `replaceWhere` em vez de `partitionOverwriteMode=dynamic`

O modo dinâmico depende de uma configuração de sessão do Delta e, quando ela não está
habilitada, degrada para um overwrite total da tabela sem avisar. O `replaceWhere` declara
no predicado exatamente qual fatia está sendo substituída, então o pipeline pode ser
reexecutado à vontade, na janela toda ou num mês só, sempre com o mesmo resultado.

### Quarentena em vez de `WHERE`

Filtrar linha ruim direto na query esconde a decisão dentro do SQL de quem analisa. Aqui a
linha reprovada vai para `rej_taxi_trip` carregando o motivo, o que permite medir quanto foi
retirado, auditar se a regra estava certa e reincorporar depois. Foi o que tornou possível a
análise de sensibilidade dos valores negativos: sabendo exatamente quais linhas saíram, dá
pra medir que a decisão custa 0,77% da receita.

### Uma camada refined além da trusted

O case exige a trusted com colunas específicas por tipo de táxi, mas as perguntas falam de
"toda a frota". Sem uma camada de unificação, todo analista teria que escrever `UNION ALL`
na mão e lembrar que yellow usa `tpep_` e green usa `lpep_`. A `fct_taxi_trip` resolve isso
uma vez, no pipeline.

### Funções puras separadas de I/O

Toda a lógica de transformação está em `src/processing/transformations.py` e
`src/quality/expectations.py`, como funções que recebem e devolvem `DataFrame`. É o que
permite testar as regras com `pytest` num Spark local, sem cluster e sem Delta, e é a razão
de um projeto de dados ter suíte de testes rodando em CI.

---

## Qualidade de dados

Regras aplicadas na transição trusted → refined:

| Regra | Bloqueante | O que barra |
|---|---|---|
| `pickup_datetime_nulo` | sim | Corrida sem horário de início |
| `dropoff_datetime_nulo` | sim | Corrida sem horário de término |
| `duracao_nao_positiva` | sim | Término anterior ou igual ao início |
| `duracao_acima_de_24h` | sim | Duração implausível para táxi urbano |
| `fora_da_janela_de_analise` | sim | Pickup fora de Jan-Mai/2023 (datas corrompidas na origem) |
| `total_amount_nulo` | sim | Sem valor não há como compor receita |
| `total_amount_negativo` | sim | Provável estorno; vai para quarentena e é analisado à parte |
| `passenger_count_ausente` | **não** | Contagem nula ou zero. A corrida continua valendo para receita; só as análises de passageiro filtram |

A distinção entre bloqueante e não bloqueante é o ponto da tabela. Uma corrida sem
`passenger_count` ainda é uma corrida com receita legítima, e são cerca de 8% da base.
Descartá-la da análise de faturamento enviesaria o resultado em troca de nada.

Na execução completa, 0,91% das corridas yellow foram para a quarentena. Os números por
regra estão em [`analysis/RESULTADOS.md`](analysis/RESULTADOS.md).

---

## Como executar

### Pré-requisitos

Uma conta no [Databricks Free Edition](https://www.databricks.com/learn/free-edition).

O enunciado sugere o Community Edition, mas ele foi descontinuado em 1º de janeiro de 2026 e
substituído pelo Free Edition. Vale notar que o Community Edition nunca teve Unity Catalog,
que é o que este projeto usa para catálogo, schemas e Volumes.

**Sobre acesso à internet.** O Free Edition restringe a saída de rede a um conjunto de
domínios confiáveis, e o CDN da TLC não está nessa lista. Duas saídas: verificar a conta com
o LinkedIn, que libera acesso externo e faz o download automático funcionar, ou usar o plano
B de upload manual documentado dentro do `01_landing.ipynb`. O resultado é idêntico nos dois
caminhos.

### Passo a passo

1. **Importe o repositório.** No workspace do Databricks: `Workspace` → `Create` →
   `Git folder` → cole a URL deste repositório.
2. **Rode `src/notebooks/00_setup.ipynb`.** Cria o catálogo `ifood_case`, os cinco schemas e
   o Volume da landing. Roda uma vez só. O nome do catálogo aparece tanto em `src/config.py`
   quanto nas células SQL dos notebooks, então trocá-lo exige alterar os dois lugares.
3. **Rode os notebooks do pipeline, em ordem:**

   | Notebook | O que faz | Tempo aproximado |
   |---|---|---|
   | `src/notebooks/01_landing.ipynb` | Baixa os 10 parquets da TLC para o Volume | 3 a 6 min |
   | `src/notebooks/02_raw.ipynb` | Materializa as tabelas raw em Delta | 2 a 4 min |
   | `src/notebooks/03_trusted.ipynb` | Aplica o schema de consumo | 1 a 3 min |
   | `src/notebooks/04_refined.ipynb` | Fato unificado, quarentena, DQ e agregados | 3 a 6 min |

4. **Rode as análises:**

   | Notebook | O que faz |
   |---|---|
   | `analysis/01_analise_exploratoria.ipynb` | Perfil dos dados, anomalias e o efeito das regras |
   | `analysis/02_respostas.ipynb` | As respostas às perguntas do case |

Os números da execução completa estão consolidados em
[`analysis/RESULTADOS.md`](analysis/RESULTADOS.md).

### Reprocessamento

O pipeline é idempotente: pode ser reexecutado quantas vezes for necessário sem duplicar
dado, porque o `replaceWhere` substitui as partições em vez de acrescentá-las.

Vale ser preciso sobre o alcance disso. `INGESTION_START` e `INGESTION_END` controlam apenas
o que o extrator baixa. As camadas seguintes leem tudo o que estiver na landing, então uma
reexecução reprocessa a janela inteira disponível. Restringir o processamento a um mês
específico exigiria filtrar os caminhos em `LandingToRawProcessor.list_source_files`, o que
está listado nos próximos passos.

### Consumindo os dados

```sql
-- Camada de consumo, por tipo de táxi
SELECT VendorID, passenger_count, total_amount,
       tpep_pickup_datetime, tpep_dropoff_datetime
FROM ifood_case.trusted.ny_taxi_trip_yellow
LIMIT 100;

-- Fato unificado da frota
SELECT trip_type, pickup_datetime, passenger_count, total_amount
FROM ifood_case.refined.fct_taxi_trip
WHERE pickup_year = '2023' AND pickup_month = '05'
LIMIT 100;
```

---

## Testes

A lógica de transformação e as regras de qualidade rodam em Spark local, sem Databricks:

```bash
pip install -r requirements.txt
pytest
ruff check src tests
```

O que a suíte cobre:

* normalização de nomes, incluindo o conflito real `airport_fee` / `Airport_fee`;
* união de arquivos com tipos físicos diferentes entre meses;
* aplicação de schema, com falha explícita quando uma coluna obrigatória some;
* cada regra de qualidade, uma a uma, e o acúmulo de vários motivos na mesma linha;
* o fato de `passenger_count` ausente não bloquear a corrida;
* geração do predicado `replaceWhere`;
* calendário de ingestão e construção das URLs da TLC.

O workflow em `.github/workflows/ci.yml` roda lint e testes a cada push e pull request.

---

## Estrutura do repositório

```
ifood-case/
├─ src/
│  ├─ config.py                        # parâmetros centrais do pipeline
│  ├─ ingestion/
│  │  └─ ny_taxi_trip_extractor.py     # download HTTP para a landing
│  ├─ processing/
│  │  ├─ transformations.py            # funções puras (testáveis)
│  │  ├─ landing_to_raw.py
│  │  ├─ raw_to_trusted.py
│  │  └─ trusted_to_refined.py         # inclui os agregados
│  ├─ quality/
│  │  └─ expectations.py               # regras de qualidade e quarentena
│  ├─ utils/
│  │  ├─ delta.py                      # escrita idempotente com replaceWhere
│  │  └─ logging.py
│  └─ notebooks/                       # orquestradores do pipeline
├─ analysis/
│  ├─ 01_analise_exploratoria.ipynb
│  ├─ 02_respostas.ipynb
│  └─ RESULTADOS.md                    # resumo executivo dos números
├─ tests/
├─ .github/workflows/ci.yml
├─ pyproject.toml
├─ requirements.txt
└─ README.md
```

Os notebooks são orquestradores finos. Toda a lógica vive em módulos Python versionados e
testáveis, e nenhuma regra de negócio está escrita dentro de um notebook.

---

## Respostas do case

As duas perguntas admitem mais de uma leitura em português, e a leitura escolhida muda o
número. Em vez de escolher em silêncio, `analysis/02_respostas.ipynb` responde as duas e
recomenda qual usar.

**Pergunta 1, média de `total_amount` por mês (yellow).** A leitura direta é a média da
coluna `total_amount` agrupada por mês, o ticket médio por corrida: US$ 28,30 ponderado no
período. Como o enunciado também admite ler "valor total recebido em um mês" como o
faturamento do mês, a média das somas mensais aparece em seguida: US$ 90,8 milhões.
Acompanha ainda a análise de sensibilidade dos valores negativos.

**Pergunta 2, média de `passenger_count` por hora em maio.** A leitura direta é a ocupação
média por corrida em cada hora. Ao lado dela está o volume de passageiros por hora num dia
típico, que é o que serve para dimensionar operação. As duas contam histórias diferentes:
às 4h e às 18h a ocupação é quase igual, mas o volume às 18h é quinze vezes maior.

"Todos os táxis da frota" foi lido como yellow mais green, porque a Pergunta 1 diz "todos os
yellow táxis" e a Pergunta 2 abandona o "yellow", o que sugere ampliação deliberada de
escopo. As bases FHV e High Volume FHV (Uber, Lyft) ficam de fora: não são táxis licenciados
e sequer publicam `passenger_count`. O recorte só de yellow também está disponível, na
abertura por tipo de táxi do notebook de respostas.

---

## Limitações e próximos passos

* **Orquestração.** Os notebooks rodam em sequência manual. O passo natural é um Databricks
  Job com dependência entre tarefas, ou um Asset Bundle versionado junto ao código.
* **Carga incremental.** Hoje a janela inteira é reprocessada a cada execução, porque
  `list_source_files` varre toda a landing. O primeiro passo é fazê-la receber a janela de
  competência; o passo seguinte é Auto Loader na landing com `MERGE` incremental adiante.
* **Tipo monetário.** `total_amount` é `double`, herdado do parquet de origem. Para
  agregação financeira em produção, `decimal(18,2)` evita imprecisão de ponto flutuante.
* **Particionamento.** Ano e mês acompanha o padrão de publicação da fonte e é adequado
  neste volume. Em escala maior, liquid clustering evitaria partições pequenas demais.
* **Contrato de schema.** As regras validam valores, não estrutura. Um teste que falhe
  quando a TLC adicionar ou remover uma coluna fecharia o ciclo.
* **Fuso horário.** Os timestamps da TLC são horário local de Nova York e foram mantidos
  assim. Cruzar com fonte externa exige normalizar o fuso antes.
* **Compute serverless.** O Free Edition não suporta `PERSIST TABLE`, então o `cache()` que
  eu usava entre as duas ramificações da quarentena teve que sair. Num cluster dedicado ele
  evitaria reavaliar o plano duas vezes.