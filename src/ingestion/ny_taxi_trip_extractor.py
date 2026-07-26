"""Extração dos arquivos originais da NYC TLC para a landing zone.

Decisões relevantes:

* O download é feito com ``requests`` em streaming e gravado primeiro em um
  arquivo temporário local, só então copiado para o Volume. ``dbutils.fs.cp``
  **não** aceita origem HTTP(S), e gravar direto no destino final deixaria um
  arquivo truncado no Volume caso a rede caia no meio do download.
* A landing guarda o arquivo exatamente como veio da fonte, sem nenhuma
  transformação — é a nossa capacidade de reprocessar do zero.
* O layout de diretórios usa ``ref_year=`` / ``ref_month=`` para que o Spark
  descubra as partições automaticamente na leitura.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date

import requests

from src import config
from src.utils.logging import get_logger

logger = get_logger(__name__)

_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB


@dataclass(frozen=True)
class ExtractionResult:
    """Resultado da extração de um único mês de competência."""

    trip_type: str
    year: int
    month: int
    url: str
    destination: str
    status: str  # "downloaded" | "skipped_existing" | "not_found"
    size_bytes: int = 0


def month_range(start: date, end: date) -> list[tuple[int, int]]:
    """Gera a lista de ``(ano, mês)`` entre duas datas, inclusive nas pontas.

    Implementado na mão para não depender de ``dateutil``, que nem sempre está
    presente no runtime serverless.
    """
    if start > end:
        raise ValueError(f"start ({start}) não pode ser maior que end ({end})")

    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


class NyTaxiTripExtractor:
    """Baixa os arquivos parquet publicados pela TLC para a landing zone."""

    def __init__(self, base_url: str = config.TLC_BASE_URL, timeout: int = 60) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ---------------------------------------------------------------- helpers

    def build_url(self, trip_type: config.TripTypeConfig, year: int, month: int) -> str:
        return f"{self.base_url}/{trip_type.file_prefix}_{year:04d}-{month:02d}.parquet"

    def _remote_exists(self, url: str) -> bool:
        response = requests.head(url, timeout=self.timeout)
        if response.status_code == 200:
            return True
        logger.warning("Arquivo indisponível na origem (HTTP %s): %s", response.status_code, url)
        return False

    def _download(self, url: str, destination: str) -> int:
        """Baixa ``url`` para ``destination`` via arquivo temporário local."""
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".parquet")
        os.close(tmp_fd)
        try:
            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as tmp_file:
                    for chunk in response.iter_content(chunk_size=_CHUNK_SIZE):
                        if chunk:
                            tmp_file.write(chunk)

            size = os.path.getsize(tmp_path)
            if size == 0:
                raise RuntimeError(f"Download vazio para {url}")

            # Copia para o Volume apenas depois que o arquivo está íntegro.
            shutil.copyfile(tmp_path, destination)
            return size
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    # ------------------------------------------------------------------- api

    def extract_month(
        self,
        trip_type: config.TripTypeConfig,
        year: int,
        month: int,
        overwrite: bool = True,
    ) -> ExtractionResult:
        """Extrai um único mês de competência."""
        url = self.build_url(trip_type, year, month)
        destination_dir = config.landing_path(trip_type.name, year, month)
        destination = f"{destination_dir}/data.parquet"

        if os.path.exists(destination) and not overwrite:
            logger.info("Arquivo já existe e overwrite=False, pulando: %s", destination)
            return ExtractionResult(
                trip_type.name, year, month, url, destination, "skipped_existing",
                os.path.getsize(destination),
            )

        if not self._remote_exists(url):
            return ExtractionResult(trip_type.name, year, month, url, destination, "not_found")

        logger.info("Baixando %s -> %s", url, destination)
        size = self._download(url, destination)
        logger.info("Concluído %s (%.1f MB)", destination, size / 1024 / 1024)
        return ExtractionResult(
            trip_type.name, year, month, url, destination, "downloaded", size
        )

    def extract(
        self,
        trip_type: config.TripTypeConfig,
        start: date = config.INGESTION_START,
        end: date = config.INGESTION_END,
        overwrite: bool = True,
    ) -> list[ExtractionResult]:
        """Extrai todos os meses de competência da janela informada."""
        results: list[ExtractionResult] = []
        for year, month in month_range(start, end):
            results.append(self.extract_month(trip_type, year, month, overwrite=overwrite))

        downloaded = sum(1 for r in results if r.status == "downloaded")
        logger.info(
            "Extração de '%s' finalizada: %s de %s meses baixados",
            trip_type.name, downloaded, len(results),
        )
        return results
