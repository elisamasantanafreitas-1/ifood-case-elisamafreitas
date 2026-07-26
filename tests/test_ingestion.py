"""Testes da extração e dos utilitários de caminho/partição.

Nenhum destes testes faz rede: exercitam apenas a lógica de calendário, de
construção de URL e de leitura da partição a partir do caminho.
"""

from __future__ import annotations

from datetime import date

import pytest

from src import config
from src.ingestion.ny_taxi_trip_extractor import NyTaxiTripExtractor, month_range
from src.processing.landing_to_raw import LandingToRawProcessor


class TestMonthRange:
    def test_janela_do_case_tem_cinco_meses(self):
        meses = month_range(config.INGESTION_START, config.INGESTION_END)
        assert meses == [(2023, 1), (2023, 2), (2023, 3), (2023, 4), (2023, 5)]

    def test_atravessa_virada_de_ano(self):
        assert month_range(date(2022, 11, 1), date(2023, 2, 1)) == [
            (2022, 11), (2022, 12), (2023, 1), (2023, 2),
        ]

    def test_mes_unico(self):
        assert month_range(date(2023, 3, 15), date(2023, 3, 1)) == [(2023, 3)]

    def test_intervalo_invertido_falha(self):
        with pytest.raises(ValueError):
            month_range(date(2023, 5, 1), date(2023, 1, 1))


class TestBuildUrl:
    @pytest.mark.parametrize(
        ("trip_type", "year", "month", "esperado"),
        [
            (config.YELLOW, 2023, 1, "yellow_tripdata_2023-01.parquet"),
            (config.YELLOW, 2023, 12, "yellow_tripdata_2023-12.parquet"),
            (config.GREEN, 2023, 5, "green_tripdata_2023-05.parquet"),
        ],
    )
    def test_url_respeita_o_padrao_da_tlc(self, trip_type, year, month, esperado):
        url = NyTaxiTripExtractor().build_url(trip_type, year, month)
        assert url.endswith(esperado)
        assert url.startswith("https://")


class TestLandingPath:
    def test_mes_tem_zero_a_esquerda(self):
        caminho = config.landing_path("yellow", 2023, 1)
        assert caminho.endswith("yellow/ref_year=2023/ref_month=01")


class TestPartitionFromPath:
    def test_extrai_ano_e_mes_preservando_zero(self):
        caminho = "/Volumes/ifood_case/landing/files/ny_taxi_trip/yellow/ref_year=2023/ref_month=01/data.parquet"
        assert LandingToRawProcessor.partition_from_path(caminho) == ("2023", "01")

    def test_caminho_fora_do_padrao_falha(self):
        with pytest.raises(ValueError):
            LandingToRawProcessor.partition_from_path("/tmp/qualquer/data.parquet")
