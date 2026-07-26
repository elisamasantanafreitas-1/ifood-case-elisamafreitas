"""Transformações puras sobre DataFrames.

Todo o conhecimento de negócio de transformação vive aqui, em funções que
recebem e devolvem ``DataFrame`` sem tocar em I/O. Isso permite testar a
lógica com ``pytest`` em um Spark local, sem Databricks e sem Delta.
"""

from __future__ import annotations

import re

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

_NON_ALNUM = re.compile(r"[^0-9a-zA-Z]+")
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def to_snake_case(name: str) -> str:
    """Converte um nome de coluna para snake_case minúsculo.

    A TLC não é consistente entre meses: 2023-01 publica ``airport_fee`` e
    meses seguintes publicam ``Airport_fee``. Em vez de depender do
    ``spark.sql.caseSensitive=false``, normalizamos explicitamente na raw.
    """
    name = _CAMEL_BOUNDARY.sub("_", name.strip())
    name = _NON_ALNUM.sub("_", name)
    return name.strip("_").lower()


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Aplica :func:`to_snake_case` a todas as colunas do DataFrame."""
    return df.select([F.col(f"`{c}`").alias(to_snake_case(c)) for c in df.columns])


def cast_all_to_string(df: DataFrame) -> DataFrame:
    """Converte todas as colunas para ``string``.

    É o que permite unir arquivos de meses diferentes sem quebrar quando a TLC
    muda o tipo físico de uma coluna (``airport_fee`` é ``int64`` em 2023-01 e
    ``double`` a partir de 2023-02). A tipagem correta é aplicada na trusted.
    """
    return df.select([F.col(f"`{c}`").cast("string").alias(c) for c in df.columns])


def apply_schema(df: DataFrame, schema: dict[str, tuple[str, str]]) -> DataFrame:
    """Seleciona, renomeia e tipa colunas conforme o mapa de schema.

    Args:
        df: DataFrame de origem (colunas em snake_case, todas string).
        schema: ``{coluna_destino: (coluna_origem, tipo_destino)}``.

    Raises:
        ValueError: se alguma coluna de origem não existir no DataFrame.
    """
    missing = [src for src, _ in schema.values() if src not in df.columns]
    if missing:
        raise ValueError(
            f"Colunas ausentes na origem: {sorted(set(missing))}. "
            f"Disponíveis: {sorted(df.columns)}"
        )
    return df.select(
        [
            F.col(f"`{source}`").cast(target_type).alias(target)
            for target, (source, target_type) in schema.items()
        ]
    )


def duration_minutes(pickup: str, dropoff: str) -> Column:
    """Duração da corrida em minutos, como expressão de coluna."""
    return (F.col(dropoff).cast("long") - F.col(pickup).cast("long")) / 60.0


def to_canonical_trip(df: DataFrame, trip_type: str, pickup: str, dropoff: str) -> DataFrame:
    """Converte uma tabela trusted (yellow ou green) no formato canônico.

    O case exige que a camada de consumo tenha ``tpep_pickup_datetime`` etc.,
    o que só faz sentido para o yellow. A trusted preserva os nomes originais
    de cada tipo; a refined unifica em ``pickup_datetime`` / ``dropoff_datetime``
    e acrescenta ``trip_type``, para que uma única tabela responda perguntas
    sobre "toda a frota".
    """
    return (
        df.select(
            F.lit(trip_type).alias("trip_type"),
            F.col("VendorID").alias("vendor_id"),
            F.col(pickup).alias("pickup_datetime"),
            F.col(dropoff).alias("dropoff_datetime"),
            F.col("passenger_count"),
            F.col("total_amount"),
            F.col("ref_year"),
            F.col("ref_month"),
            F.col("_source_file"),
        )
        .withColumn("trip_duration_minutes", duration_minutes("pickup_datetime", "dropoff_datetime"))
        .withColumn("pickup_year", F.date_format("pickup_datetime", "yyyy"))
        .withColumn("pickup_month", F.date_format("pickup_datetime", "MM"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
    )
