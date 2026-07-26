"""Testes das regras de qualidade e do mecanismo de quarentena."""

from __future__ import annotations

import datetime as dt

import pytest

from src.quality import expectations

CANONICAL_SCHEMA = (
    "trip_type string, vendor_id bigint, pickup_datetime timestamp, "
    "dropoff_datetime timestamp, passenger_count double, total_amount double, "
    "trip_duration_minutes double"
)


def _row(
    pickup=dt.datetime(2023, 5, 10, 10, 0),
    dropoff=dt.datetime(2023, 5, 10, 10, 30),
    passenger_count=2.0,
    total_amount=25.0,
):
    duration = None
    if pickup is not None and dropoff is not None:
        duration = (dropoff - pickup).total_seconds() / 60.0
    return ("yellow", 1, pickup, dropoff, passenger_count, total_amount, duration)


@pytest.fixture
def rules():
    return expectations.build_rules()


class TestSplitValidAndRejected:
    def test_corrida_saudavel_passa(self, spark, rules):
        df = spark.createDataFrame([_row()], CANONICAL_SCHEMA)
        valid, rejected = expectations.split_valid_and_rejected(df, rules)
        assert valid.count() == 1
        assert rejected.count() == 0

    @pytest.mark.parametrize(
        ("linha", "motivo"),
        [
            (
                _row(
                    pickup=dt.datetime(2023, 5, 10, 10, 30),
                    dropoff=dt.datetime(2023, 5, 10, 10, 0),
                ),
                "duracao_nao_positiva",
            ),
            (
                _row(
                    pickup=dt.datetime(2023, 5, 1, 0, 0),
                    dropoff=dt.datetime(2023, 5, 3, 0, 0),
                ),
                "duracao_acima_de_24h",
            ),
            (
                _row(
                    pickup=dt.datetime(2008, 12, 31, 23, 0),
                    dropoff=dt.datetime(2008, 12, 31, 23, 30),
                ),
                "fora_da_janela_de_analise",
            ),
            (_row(total_amount=-15.0), "total_amount_negativo"),
            (_row(total_amount=None), "total_amount_nulo"),
            (_row(dropoff=None), "dropoff_datetime_nulo"),
        ],
    )
    def test_linhas_problematicas_vao_para_quarentena(self, spark, rules, linha, motivo):
        df = spark.createDataFrame([linha], CANONICAL_SCHEMA)
        valid, rejected = expectations.split_valid_and_rejected(df, rules)

        assert valid.count() == 0
        assert rejected.count() == 1
        assert motivo in rejected.collect()[0][expectations.REJECTION_COLUMN]

    def test_passenger_count_ausente_nao_bloqueia_receita(self, spark, rules):
        """Corrida sem passageiro informado ainda vale para a análise de receita."""
        df = spark.createDataFrame([_row(passenger_count=None)], CANONICAL_SCHEMA)
        valid, rejected = expectations.split_valid_and_rejected(df, rules)

        assert valid.count() == 1
        assert rejected.count() == 0

    def test_linha_pode_acumular_varios_motivos(self, spark, rules):
        linha = _row(
            pickup=dt.datetime(2002, 1, 1, 0, 0),
            dropoff=dt.datetime(2001, 12, 31, 0, 0),
            total_amount=-1.0,
        )
        df = spark.createDataFrame([linha], CANONICAL_SCHEMA)
        _, rejected = expectations.split_valid_and_rejected(df, rules)

        motivos = set(rejected.collect()[0][expectations.REJECTION_COLUMN])
        assert {"duracao_nao_positiva", "fora_da_janela_de_analise", "total_amount_negativo"} <= motivos


class TestMeasure:
    def test_relatorio_tem_uma_linha_por_regra(self, spark, rules):
        df = spark.createDataFrame([_row(), _row(total_amount=-5.0)], CANONICAL_SCHEMA)
        report = expectations.measure(df, rules)

        assert report.count() == len(rules)
        por_regra = {r["rule_name"]: r for r in report.collect()}
        assert por_regra["total_amount_negativo"]["rows_failed"] == 1
        assert por_regra["total_amount_negativo"]["rows_evaluated"] == 2
        assert por_regra["total_amount_negativo"]["failure_pct"] == pytest.approx(50.0)
