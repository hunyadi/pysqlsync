"""
pysqlsync: Synchronize schema and large volumes of data.

Copyright 2023-2026, Levente Hunyadi

:see: https://github.com/hunyadi/pysqlsync
"""

from pysqlsync.model.data_types import SqlJsonType


class PostgreSQLJsonType(SqlJsonType):
    def __str__(self) -> str:
        return "jsonb"
