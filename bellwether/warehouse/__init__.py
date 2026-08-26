"""The warehouse: Spark's Parquet output, landed in Postgres for dbt.

Deliberately thin. Nothing here derives anything — Spark shapes the data on the
way in and dbt shapes it on the way out, so there is never a third place to look
for where a number came from.
"""

from bellwether.warehouse.load import COLUMNS, DDL, Loaded, counts, load, read_table

__all__ = ["COLUMNS", "DDL", "Loaded", "counts", "load", "read_table"]
