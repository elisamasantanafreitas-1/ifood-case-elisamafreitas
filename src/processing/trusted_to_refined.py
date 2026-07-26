"""Camada refined: fato unificado da frota, quarentena e tabelas agregadas.

É aqui que:

* yellow e green viram uma única tabela ``fct_taxi_trip``, com ``trip_type``
  como discriminador, de modo que perguntas sobre "toda a frota" sejam uma
  consulta só;
* as regras de qualidade são aplicadas, mandando as linhas reprovadas para a
  tabela de quarentena ``rej_taxi_trip`` com o motivo;
* nascem os agregados que respondem diretamente as perguntas do case.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.processing.transformations import to_canonical_trip
from src.quality import expectations
from src.utils.delta import overwrite_table, write_partitions
from src.utils.logging import get_logger

logger = get_logger(__name__)


class TrustedToRefinedProcessor:
    """Constrói o fato unificado, a quarentena e os agregados de consumo."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # -------------------------------------------------------------- unificação

    def build_canonical(
        self, trip_types: tuple[config.TripTypeConfig, ...] = config.TRIP_TYPES
    ) -> DataFrame:
        """Une todas as tabelas trusted no formato canônico."""
        frames = []
        for trip_type in trip_types:
            table = config.trusted_table(trip_type.name)
            logger.info("Lendo %s", table)
            frames.append(
                to_canonical_trip(
                    self.spark.table(table),
                    trip_type=trip_type.name,
                    pickup=trip_type.pickup_column,
                    dropoff=trip_type.dropoff_column,
                )
            )
        return reduce(lambda acc, nxt: acc.unionByName(nxt), frames)

    # ------------------------------------------------------------------ fato

    def process_fact(
        self, trip_types: tuple[config.TripTypeConfig, ...] = config.TRIP_TYPES
    ) -> tuple[DataFrame, DataFrame, DataFrame]:
        """Materializa fato, quarentena e relatório de qualidade.

        Returns:
            Tupla ``(fato, quarentena, relatorio_dq)``.
        """
        canonical = self.build_canonical(trip_types)
        rules = expectations.build_rules()

        report = expectations.measure(canonical, rules).withColumn(
            "measured_at", F.current_timestamp()
        )

        valid, rejected = expectations.split_valid_and_rejected(canonical, rules)

        write_partitions(
            valid,
            config.FACT_TABLE,
            config.REFINED_PARTITION_COLUMNS,
            comment=(
                "Fato unificado de corridas (yellow + green) aprovado nas regras "
                "de qualidade. Particionado pelo mês do evento (pickup)."
            ),
        )

        # A quarentena não é particionada pelo pickup porque justamente parte
        # das linhas tem pickup corrompido ou nulo.
        overwrite_table(
            rejected,
            config.QUARANTINE_TABLE,
            comment="Corridas reprovadas nas regras de qualidade, com o motivo da reprovação.",
        )

        overwrite_table(
            report,
            config.DQ_RESULTS_TABLE,
            comment="Resultado da última execução das regras de qualidade.",
        )
        return valid, rejected, report

    # -------------------------------------------------------------- agregados

    def process_aggregates(self) -> tuple[DataFrame, DataFrame]:
        """Cria os agregados mensal e horário a partir do fato."""
        fact = self.spark.table(config.FACT_TABLE)

        monthly = (
            fact.groupBy("trip_type", "pickup_year", "pickup_month")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_total_amount_per_trip"),
                F.expr("percentile_approx(total_amount, 0.5)").alias("median_total_amount"),
                F.sum("passenger_count").alias("total_passengers"),
            )
            .withColumn(
                "reference_month",
                F.concat_ws("-", F.col("pickup_year"), F.col("pickup_month")),
            )
            .orderBy("trip_type", "pickup_year", "pickup_month")
        )
        overwrite_table(
            monthly,
            config.AGG_MONTHLY_TABLE,
            comment="Agregado mensal por tipo de corrida: receita, ticket médio e volume.",
        )

        hourly = (
            fact.groupBy("trip_type", "pickup_year", "pickup_month", "pickup_hour")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum(
                    F.when(F.col("passenger_count") > 0, F.col("passenger_count"))
                ).alias("total_passengers"),
                F.count(
                    F.when(F.col("passenger_count") > 0, F.lit(1))
                ).alias("trips_with_passenger_count"),
                F.avg(
                    F.when(F.col("passenger_count") > 0, F.col("passenger_count"))
                ).alias("avg_passenger_count"),
                F.countDistinct("pickup_date").alias("distinct_days"),
            )
            .orderBy("trip_type", "pickup_year", "pickup_month", "pickup_hour")
        )
        overwrite_table(
            hourly,
            config.AGG_HOURLY_TABLE,
            comment=(
                "Agregado por hora do dia e mês, por tipo de corrida. "
                "Métricas de passageiros ignoram corridas sem passenger_count."
            ),
        )
        return monthly, hourly
