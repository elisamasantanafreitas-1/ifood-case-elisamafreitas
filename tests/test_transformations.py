"""Testes das funções puras de transformação."""

from __future__ import annotations

import datetime as dt

import pytest

from src.processing import transformations as t


class TestToSnakeCase:
    @pytest.mark.parametrize(
        ("original", "expected"),
        [
            ("VendorID", "vendor_id"),
            ("Airport_fee", "airport_fee"),
            ("airport_fee", "airport_fee"),
            ("tpep_pickup_datetime", "tpep_pickup_datetime"),
            ("RatecodeID", "ratecode_id"),
            ("  total amount ", "total_amount"),
            ("PULocationID", "pulocation_id"),
        ],
    )
    def test_normaliza_variacoes_da_tlc(self, original, expected):
        assert t.to_snake_case(original) == expected


class TestNormalizeColumnNames:
    def test_resolve_conflito_de_maiusculas_entre_meses(self, spark):
        janeiro = spark.createDataFrame([(1, 1.25)], "VendorID bigint, airport_fee double")
        fevereiro = spark.createDataFrame([(2, 1.75)], "VendorID bigint, Airport_fee double")

        jan = t.normalize_column_names(janeiro)
        fev = t.normalize_column_names(fevereiro)

        assert jan.columns == fev.columns == ["vendor_id", "airport_fee"]
        assert jan.unionByName(fev).count() == 2


class TestCastAllToString:
    def test_une_arquivos_com_tipos_fisicos_diferentes(self, spark):
        """Reproduz o problema real da TLC: airport_fee muda de int para double."""
        janeiro = spark.createDataFrame([(1, 1)], "vendor_id bigint, airport_fee bigint")
        fevereiro = spark.createDataFrame([(2, 1.75)], "vendor_id bigint, airport_fee double")

        unido = t.cast_all_to_string(janeiro).unionByName(t.cast_all_to_string(fevereiro))

        assert dict(unido.dtypes) == {"vendor_id": "string", "airport_fee": "string"}
        assert unido.count() == 2


class TestApplySchema:
    def test_renomeia_seleciona_e_tipa(self, spark):
        df = spark.createDataFrame(
            [("1", "2.0", "18.5", "2023-01-01 10:00:00", "ignorar")],
            "vendor_id string, passenger_count string, total_amount string, "
            "tpep_pickup_datetime string, coluna_extra string",
        )
        schema = {
            "VendorID": ("vendor_id", "bigint"),
            "passenger_count": ("passenger_count", "double"),
            "total_amount": ("total_amount", "double"),
            "tpep_pickup_datetime": ("tpep_pickup_datetime", "timestamp"),
        }

        resultado = t.apply_schema(df, schema)

        assert resultado.columns == list(schema.keys())
        assert dict(resultado.dtypes)["VendorID"] == "bigint"
        assert dict(resultado.dtypes)["tpep_pickup_datetime"] == "timestamp"
        linha = resultado.collect()[0]
        assert linha["VendorID"] == 1
        assert linha["total_amount"] == pytest.approx(18.5)

    def test_falha_alto_e_claro_quando_coluna_obrigatoria_some(self, spark):
        df = spark.createDataFrame([("1",)], "vendor_id string")
        with pytest.raises(ValueError, match="Colunas ausentes"):
            t.apply_schema(df, {"total_amount": ("total_amount", "double")})


class TestToCanonicalTrip:
    @pytest.fixture
    def trusted_yellow(self, spark):
        return spark.createDataFrame(
            [
                (
                    1,
                    2.0,
                    25.0,
                    dt.datetime(2023, 5, 10, 14, 30),
                    dt.datetime(2023, 5, 10, 15, 0),
                    "2023",
                    "05",
                    "/Volumes/x/data.parquet",
                )
            ],
            "VendorID bigint, passenger_count double, total_amount double, "
            "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
            "ref_year string, ref_month string, _source_file string",
        )

    def test_deriva_colunas_do_evento(self, trusted_yellow):
        canonical = t.to_canonical_trip(
            trusted_yellow, "yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime"
        ).collect()[0]

        assert canonical["trip_type"] == "yellow"
        assert canonical["trip_duration_minutes"] == pytest.approx(30.0)
        assert canonical["pickup_year"] == "2023"
        assert canonical["pickup_month"] == "05"
        assert canonical["pickup_hour"] == 14

    def test_yellow_e_green_ficam_unificaveis(self, spark, trusted_yellow):
        green = spark.createDataFrame(
            [
                (
                    2,
                    1.0,
                    12.0,
                    dt.datetime(2023, 5, 11, 8, 0),
                    dt.datetime(2023, 5, 11, 8, 20),
                    "2023",
                    "05",
                    "/Volumes/y/data.parquet",
                )
            ],
            "VendorID bigint, passenger_count double, total_amount double, "
            "lpep_pickup_datetime timestamp, lpep_dropoff_datetime timestamp, "
            "ref_year string, ref_month string, _source_file string",
        )

        unificado = t.to_canonical_trip(
            trusted_yellow, "yellow", "tpep_pickup_datetime", "tpep_dropoff_datetime"
        ).unionByName(
            t.to_canonical_trip(green, "green", "lpep_pickup_datetime", "lpep_dropoff_datetime")
        )

        assert unificado.count() == 2
        assert set(r["trip_type"] for r in unificado.collect()) == {"yellow", "green"}
