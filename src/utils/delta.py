"""Escrita em Delta com sobrescrita idempotente por partição.

Por que ``replaceWhere`` e não ``partitionOverwriteMode=dynamic``:
o modo dinâmico depende de uma configuração de sessão do Delta e falha em
silêncio quando ela não está habilitada — o resultado vira um overwrite total.
O ``replaceWhere`` com predicado explícito declara exatamente qual fatia da
tabela está sendo substituída, o que torna o reprocessamento auditável e
seguro para rodar quantas vezes for preciso.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.utils.logging import get_logger

logger = get_logger(__name__)


def build_replace_where(df: DataFrame, partition_columns: tuple[str, ...]) -> str:
    """Monta o predicado ``replaceWhere`` a partir das partições presentes no DataFrame."""
    combos = (
        df.select(*partition_columns)
        .distinct()
        .orderBy(*partition_columns)
        .collect()
    )
    if not combos:
        raise ValueError("DataFrame vazio: nada a escrever.")

    clauses = []
    for row in combos:
        parts = [f"{col} = '{row[col]}'" for col in partition_columns]
        clauses.append("(" + " AND ".join(parts) + ")")
    return " OR ".join(clauses)


def table_exists(spark, table: str) -> bool:
    try:
        return spark.catalog.tableExists(table)
    except Exception:  # noqa: BLE001 - catálogo pode não existir ainda
        return False


def write_partitions(
    df: DataFrame,
    table: str,
    partition_columns: tuple[str, ...],
    comment: str | None = None,
) -> None:
    """Escreve o DataFrame substituindo apenas as partições nele contidas."""
    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite").partitionBy(*partition_columns)

    if table_exists(spark, table):
        # `overwriteSchema` não pode ser combinado com `replaceWhere`: o Delta
        # rejeita a operação. Em reprocessamento usamos `mergeSchema`, que aceita
        # colunas novas sem reescrever a tabela inteira.
        predicate = build_replace_where(df, partition_columns)
        logger.info("Sobrescrevendo partições de %s onde %s", table, predicate)
        writer = writer.option("replaceWhere", predicate).option("mergeSchema", "true")
    else:
        logger.info("Criando tabela %s", table)

    writer.saveAsTable(table)

    if comment:
        spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    logger.info("Escrita concluída em %s", table)


def overwrite_table(df: DataFrame, table: str, comment: str | None = None) -> None:
    """Sobrescreve integralmente uma tabela (usado para agregados pequenos)."""
    spark = df.sparkSession
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )
    if comment:
        spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
    logger.info("Tabela %s sobrescrita", table)
