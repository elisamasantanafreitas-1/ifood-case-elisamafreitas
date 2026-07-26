"""Fixtures compartilhadas dos testes.

Os testes rodam em uma SparkSession local, sem Databricks e sem Delta: as
funções de transformação e de qualidade foram escritas como funções puras
sobre DataFrame justamente para permitir isso.
"""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ifood-case-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
