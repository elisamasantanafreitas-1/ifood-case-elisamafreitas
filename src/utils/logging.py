"""Configuração de logging padronizada para todo o pipeline.

Centralizar isso evita que cada módulo chame ``logging.basicConfig`` com
``force=True`` e sobrescreva a configuração dos outros.
"""

from __future__ import annotations

import logging
import sys

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_configured = False


def configure_logging(level: int = logging.INFO) -> None:
    """Configura o root logger uma única vez por sessão."""
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(level)
    # Notebooks Databricks já vêm com handlers; removemos para não duplicar linhas.
    for handler in list(root.handlers):
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    root.addHandler(handler)
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Retorna um logger já configurado."""
    configure_logging()
    return logging.getLogger(name)
