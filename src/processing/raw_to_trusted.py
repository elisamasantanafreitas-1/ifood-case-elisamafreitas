"""Camada trusted: seleção e tipagem das colunas de consumo.

Contrato desta camada:

* apenas as colunas exigidas pelo case, com a grafia exigida pelo case
  (``VendorID``, ``passenger_count``, ``total_amount``,
  ``tpep_pickup_datetime``, ``tpep_dropoff_datetime``), mais linhagem;
* tipos corretos (timestamp, double, bigint);
* nenhuma linha descartada — limpeza acontece na refined, de forma auditável.

A trusted é a camada que já pode ser consultada via SQL pelos usuários finais.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src import config
from src.processing.transformations import apply_schema
from src.utils.delta import write_partitions
from src.utils.logging import get_logger

logger = get_logger(__name__)


class RawToTrustedProcessor:
    """Aplica o schema de consumo sobre a camada raw."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def process(self, trip_type: config.TripTypeConfig) -> DataFrame:
        source = config.raw_table(trip_type.name)
        target = config.trusted_table(trip_type.name)
        logger.info("Processando %s -> %s", source, target)

        df = self.spark.table(source)
        typed = apply_schema(df, trip_type.trusted_schema)

        write_partitions(
            typed,
            target,
            config.PARTITION_COLUMNS,
            comment=(
                f"Camada de consumo das corridas {trip_type.name}. "
                "Colunas exigidas pelo case, já tipadas. Sem filtros aplicados."
            ),
        )
        return typed
