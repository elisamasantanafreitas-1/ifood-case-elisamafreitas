"""Testes do predicado de sobrescrita idempotente."""

from __future__ import annotations

import pytest

from src.utils.delta import build_replace_where


class TestBuildReplaceWhere:
    def test_uma_particao(self, spark):
        df = spark.createDataFrame([("2023", "01", 1)], "ref_year string, ref_month string, v int")
        assert build_replace_where(df, ("ref_year", "ref_month")) == "(ref_year = '2023' AND ref_month = '01')"

    def test_varias_particoes_ficam_ordenadas_e_deduplicadas(self, spark):
        df = spark.createDataFrame(
            [("2023", "02", 1), ("2023", "01", 2), ("2023", "01", 3)],
            "ref_year string, ref_month string, v int",
        )
        predicado = build_replace_where(df, ("ref_year", "ref_month"))
        assert predicado == (
            "(ref_year = '2023' AND ref_month = '01') OR "
            "(ref_year = '2023' AND ref_month = '02')"
        )

    def test_dataframe_vazio_falha(self, spark):
        df = spark.createDataFrame([], "ref_year string, ref_month string")
        with pytest.raises(ValueError):
            build_replace_where(df, ("ref_year", "ref_month"))
