from pysqlsync.model.data_types import SqlJsonType


class PostgreSQLJsonType(SqlJsonType):
    def __str__(self) -> str:
        return "jsonb"
