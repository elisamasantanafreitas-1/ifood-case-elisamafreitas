"""Regras de qualidade de dados aplicadas entre a trusted e a refined.

Modelo adotado: **quarentena, não descarte silencioso**. Toda linha reprovada
vai para uma tabela de rejeitados com o motivo da reprovação, o que permite
auditar o que foi retirado das análises e reprocessar depois se a regra mudar.

Cada regra é expressa como uma condição que deve ser **verdadeira** para a
linha ser considerada boa.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src import config

REJECTION_COLUMN = "_rejection_reasons"


@dataclass(frozen=True)
class Rule:
    """Uma expectativa de qualidade.

    Attributes:
        name: Identificador curto, usado como motivo de rejeição.
        description: Explicação em português para o relatório de qualidade.
        condition: Função que devolve a ``Column`` booleana. Linha boa => True.
        blocking: Se ``True``, a linha vai para a quarentena. Se ``False``, a
            regra só é medida e reportada, mas a linha segue para a refined.
    """

    name: str
    description: str
    condition: Callable[[], Column]
    blocking: bool = True


def build_rules() -> list[Rule]:
    """Regras aplicadas ao DataFrame canônico da camada refined."""
    return [
        Rule(
            name="pickup_datetime_nulo",
            description="Corrida sem horário de início não é analisável.",
            condition=lambda: F.col("pickup_datetime").isNotNull(),
        ),
        Rule(
            name="dropoff_datetime_nulo",
            description="Corrida sem horário de término não é analisável.",
            condition=lambda: F.col("dropoff_datetime").isNotNull(),
        ),
        Rule(
            name="duracao_nao_positiva",
            description="Término anterior ou igual ao início: registro inconsistente.",
            condition=lambda: F.col("trip_duration_minutes") > 0,
        ),
        Rule(
            name="duracao_acima_de_24h",
            description=(
                "Corrida com mais de 24 horas é implausível para táxi urbano e "
                "normalmente indica falha no taxímetro."
            ),
            condition=lambda: F.col("trip_duration_minutes")
            <= config.MAX_TRIP_DURATION_MINUTES,
        ),
        Rule(
            name="fora_da_janela_de_analise",
            description=(
                "Arquivos da TLC contêm timestamps corrompidos (2001, 2008, 2090). "
                "Só entram corridas com pickup dentro da janela pedida no case."
            ),
            condition=lambda: (F.col("pickup_datetime") >= F.lit(config.ANALYSIS_START).cast("timestamp"))
            & (F.col("pickup_datetime") < F.lit(config.ANALYSIS_END).cast("timestamp")),
        ),
        Rule(
            name="total_amount_nulo",
            description="Sem valor total não há como compor receita.",
            condition=lambda: F.col("total_amount").isNotNull(),
        ),
        Rule(
            name="total_amount_negativo",
            description=(
                "Valores negativos provavelmente são estornos/ajustes. Não são "
                "descartados: vão para a quarentena e podem ser reincorporados "
                "caso a área de negócio confirme que representam receita."
            ),
            condition=lambda: F.col("total_amount") >= 0,
        ),
        Rule(
            name="passenger_count_ausente",
            description=(
                "Contagem de passageiros nula ou zero. NÃO bloqueia: a corrida "
                "continua válida para receita, e as análises de passageiros "
                "filtram esse caso explicitamente."
            ),
            condition=lambda: F.col("passenger_count").isNotNull()
            & (F.col("passenger_count") > 0),
            blocking=False,
        ),
    ]


def with_rejection_reasons(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Adiciona a coluna com o array de motivos de rejeição (regras bloqueantes)."""
    reasons = [
        F.when(~rule.condition(), F.lit(rule.name))
        for rule in rules
        if rule.blocking
    ]
    if not reasons:
        return df.withColumn(REJECTION_COLUMN, F.array())
    return df.withColumn(
        REJECTION_COLUMN, F.array_compact(F.array(*reasons))
    )


def split_valid_and_rejected(
    df: DataFrame, rules: list[Rule]
) -> tuple[DataFrame, DataFrame]:
    """Divide o DataFrame em (aprovados, quarentena).

    Returns:
        Tupla ``(valid, rejected)``. ``valid`` não carrega a coluna de motivos;
        ``rejected`` carrega, com pelo menos um motivo por linha.
    """
    flagged = with_rejection_reasons(df, rules)
    valid = flagged.filter(F.size(REJECTION_COLUMN) == 0).drop(REJECTION_COLUMN)
    rejected = flagged.filter(F.size(REJECTION_COLUMN) > 0)
    return valid, rejected


def measure(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Mede todas as regras e devolve um relatório tabular.

    O resultado tem uma linha por regra com total de linhas avaliadas,
    reprovadas e o percentual de reprovação.
    """
    total = F.count(F.lit(1))
    aggregations = []
    for rule in rules:
        aggregations.append(
            F.sum(F.when(~rule.condition(), F.lit(1)).otherwise(F.lit(0))).alias(rule.name)
        )

    row = df.agg(total.alias("_total"), *aggregations).collect()[0].asDict()
    total_rows = row.pop("_total")

    spark = df.sparkSession
    rules_by_name = {r.name: r for r in rules}
    records = [
        (
            name,
            rules_by_name[name].description,
            bool(rules_by_name[name].blocking),
            int(total_rows),
            int(failed or 0),
            round((failed or 0) * 100.0 / total_rows, 4) if total_rows else 0.0,
        )
        for name, failed in row.items()
    ]
    return spark.createDataFrame(
        records,
        schema=(
            "rule_name string, description string, blocking boolean, "
            "rows_evaluated bigint, rows_failed bigint, failure_pct double"
        ),
    )