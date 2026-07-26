"""Camada raw: arquivos originais da landing viram tabelas Delta.

Contrato desta camada:

* nenhuma linha é descartada;
* nomes de coluna normalizados para snake_case;
* todos os valores em ``string`` (schema-on-read) — resolve a mudança de tipo
  físico da TLC entre janeiro e os meses seguintes;
* colunas de linhagem ``_source_file`` e ``_ingested_at``;
* particionada pelo mês de competência do arquivo (``ref_year``/``ref_month``).
"""

from __future__ import annotations

import glob
import os
import re
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config
from src.processing.transformations import cast_all_to_string, normalize_column_names
from src.utils.delta import write_partitions
from src.utils.logging import get_logger

logger = get_logger(__name__)

_PARTITION_PATTERN = re.compile(r"ref_year=(\d{4})/ref_month=(\d{2})")


class LandingToRawProcessor:
    """Lê os parquets da landing e materializa a tabela raw."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ---------------------------------------------------------------- helpers

    @staticmethod
    def list_source_files(trip_type_root: str) -> list[str]:
        """Lista os arquivos parquet da landing para um tipo de corrida."""
        pattern = os.path.join(trip_type_root, "ref_year=*", "ref_month=*", "*.parquet")
        return sorted(glob.glob(pattern))

    @staticmethod
    def partition_from_path(path: str) -> tuple[str, str]:
        """Extrai ``(ref_year, ref_month)`` do caminho do arquivo.

        Deliberadamente derivamos a partição do caminho em vez de confiar na
        inferência do Spark: a inferência converte ``ref_month=01`` em inteiro
        ``1`` e perderia o zero à esquerda.
        """
        match = _PARTITION_PATTERN.search(path.replace("\\", "/"))
        if not match:
            raise ValueError(f"Caminho fora do padrão esperado de partição: {path}")
        return match.group(1), match.group(2)

    def _read_one(self, path: str) -> DataFrame:
        ref_year, ref_month = self.partition_from_path(path)
        df = self.spark.read.parquet(path)
        df = cast_all_to_string(normalize_column_names(df))
        return (
            df.withColumn(config.COL_SOURCE_FILE, F.lit(path))
            .withColumn(config.COL_INGESTED_AT, F.current_timestamp())
            .withColumn("ref_year", F.lit(ref_year))
            .withColumn("ref_month", F.lit(ref_month))
        )

    # ------------------------------------------------------------------- api

    def read_landing(self, trip_type_root: str) -> DataFrame:
        """Une todos os arquivos da landing em um único DataFrame."""
        paths = self.list_source_files(trip_type_root)
        if not paths:
            raise ValueError(f"Nenhum arquivo encontrado em {trip_type_root}")

        logger.info("Encontrados %s arquivos em %s", len(paths), trip_type_root)
        frames = (self._read_one(p) for p in paths)
        return reduce(
            lambda acc, nxt: acc.unionByName(nxt, allowMissingColumns=True), frames
        )

    def process(self, trip_type: config.TripTypeConfig) -> DataFrame:
        """Executa a carga landing -> raw para um tipo de corrida."""
        root = config.landing_trip_type_root(trip_type.name)
        target = config.raw_table(trip_type.name)
        logger.info("Processando %s -> %s", root, target)

        df = self.read_landing(root)
        write_partitions(
            df,
            target,
            config.PARTITION_COLUMNS,
            comment=(
                f"Camada raw das corridas {trip_type.name} da NYC TLC. "
                "Todas as colunas em string (schema-on-read), sem descarte de linhas."
            ),
        )
        return df
