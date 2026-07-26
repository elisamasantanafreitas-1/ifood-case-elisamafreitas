"""Configuração central do pipeline.

Tudo que é parametrizável (catálogo, caminhos, janela de ingestão, schema de
cada tipo de táxi) mora aqui. Os notebooks são apenas orquestradores finos e
não devem conter constantes de negócio.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

# --------------------------------------------------------------------------- #
# Unity Catalog
# --------------------------------------------------------------------------- #

CATALOG = "ifood_case"

SCHEMA_LANDING = "landing"
SCHEMA_RAW = "raw"
SCHEMA_TRUSTED = "trusted"
SCHEMA_REFINED = "refined"
SCHEMA_QUALITY = "quality"

LANDING_VOLUME = "files"
LANDING_ROOT = f"/Volumes/{CATALOG}/{SCHEMA_LANDING}/{LANDING_VOLUME}/ny_taxi_trip"

# --------------------------------------------------------------------------- #
# Fonte de dados (NYC TLC)
# --------------------------------------------------------------------------- #

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Janela pedida pelo case: Janeiro a Maio de 2023.
INGESTION_START = date(2023, 1, 1)
INGESTION_END = date(2023, 5, 1)  # inclusive, referente ao mês de competência

# Janela válida para análise, derivada da janela de ingestão.
# Usada para separar corridas com data plausível das corridas com data corrompida
# (os arquivos da TLC contêm timestamps de 2001, 2008, 2090 etc.).
ANALYSIS_START = "2023-01-01"
ANALYSIS_END = "2023-06-01"  # exclusivo

# Duração máxima plausível de uma corrida de táxi urbano.
MAX_TRIP_DURATION_MINUTES = 24 * 60

# --------------------------------------------------------------------------- #
# Colunas de linhagem adicionadas na camada raw
# --------------------------------------------------------------------------- #

COL_SOURCE_FILE = "_source_file"
COL_INGESTED_AT = "_ingested_at"

# Partições das camadas raw/trusted. Referem-se ao mês de COMPETÊNCIA do arquivo
# publicado pela TLC — deliberadamente separado do mês do evento (pickup), que
# nem sempre coincide por causa dos registros sujos.
PARTITION_COLUMNS = ("ref_year", "ref_month")

# Partições da camada refined, baseadas na data do evento.
REFINED_PARTITION_COLUMNS = ("pickup_year", "pickup_month")


@dataclass(frozen=True)
class TripTypeConfig:
    """Descreve um tipo de corrida publicado pela TLC.

    Attributes:
        name: Identificador curto usado em nomes de tabela e caminhos.
        file_prefix: Prefixo do arquivo publicado pela TLC.
        pickup_column: Nome da coluna de início da corrida na origem.
        dropoff_column: Nome da coluna de fim da corrida na origem.
        trusted_schema: Mapa ``coluna_destino -> (coluna_origem, tipo)``.
            A coluna de origem é sempre o nome **normalizado** (snake_case
            minúsculo), porque a raw padroniza os nomes. O nome de destino
            preserva a grafia exigida pelo case (ex.: ``VendorID``).
    """

    name: str
    file_prefix: str
    pickup_column: str
    dropoff_column: str
    trusted_schema: dict[str, tuple[str, str]] = field(default_factory=dict)


YELLOW = TripTypeConfig(
    name="yellow",
    file_prefix="yellow_tripdata",
    pickup_column="tpep_pickup_datetime",
    dropoff_column="tpep_dropoff_datetime",
    trusted_schema={
        # Obrigatórias pelo enunciado do case.
        "VendorID": ("vendor_id", "bigint"),
        "passenger_count": ("passenger_count", "double"),
        "total_amount": ("total_amount", "double"),
        "tpep_pickup_datetime": ("tpep_pickup_datetime", "timestamp"),
        "tpep_dropoff_datetime": ("tpep_dropoff_datetime", "timestamp"),
        # Linhagem e partições.
        COL_SOURCE_FILE: (COL_SOURCE_FILE, "string"),
        COL_INGESTED_AT: (COL_INGESTED_AT, "timestamp"),
        "ref_year": ("ref_year", "string"),
        "ref_month": ("ref_month", "string"),
    },
)

GREEN = TripTypeConfig(
    name="green",
    file_prefix="green_tripdata",
    pickup_column="lpep_pickup_datetime",
    dropoff_column="lpep_dropoff_datetime",
    trusted_schema={
        "VendorID": ("vendor_id", "bigint"),
        "passenger_count": ("passenger_count", "double"),
        "total_amount": ("total_amount", "double"),
        "lpep_pickup_datetime": ("lpep_pickup_datetime", "timestamp"),
        "lpep_dropoff_datetime": ("lpep_dropoff_datetime", "timestamp"),
        COL_SOURCE_FILE: (COL_SOURCE_FILE, "string"),
        COL_INGESTED_AT: (COL_INGESTED_AT, "timestamp"),
        "ref_year": ("ref_year", "string"),
        "ref_month": ("ref_month", "string"),
    },
)

TRIP_TYPES: tuple[TripTypeConfig, ...] = (YELLOW, GREEN)
TRIP_TYPES_BY_NAME = {t.name: t for t in TRIP_TYPES}


# --------------------------------------------------------------------------- #
# Helpers de nomenclatura
# --------------------------------------------------------------------------- #


def landing_path(trip_type: str, year: int | str, month: int | str) -> str:
    """Caminho da partição do arquivo original na landing."""
    return f"{LANDING_ROOT}/{trip_type}/ref_year={int(year):04d}/ref_month={int(month):02d}"


def landing_trip_type_root(trip_type: str) -> str:
    return f"{LANDING_ROOT}/{trip_type}"


def raw_table(trip_type: str) -> str:
    return f"{CATALOG}.{SCHEMA_RAW}.ny_taxi_trip_{trip_type}"


def trusted_table(trip_type: str) -> str:
    return f"{CATALOG}.{SCHEMA_TRUSTED}.ny_taxi_trip_{trip_type}"


FACT_TABLE = f"{CATALOG}.{SCHEMA_REFINED}.fct_taxi_trip"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA_REFINED}.rej_taxi_trip"
AGG_MONTHLY_TABLE = f"{CATALOG}.{SCHEMA_REFINED}.agg_trip_monthly"
AGG_HOURLY_TABLE = f"{CATALOG}.{SCHEMA_REFINED}.agg_trip_hourly"
DQ_RESULTS_TABLE = f"{CATALOG}.{SCHEMA_QUALITY}.dq_results"
