from pysqlsync.model.data_types import SqlBooleanType, SqlTimestampType


class MSSQLBooleanType(SqlBooleanType):
    def __str__(self) -> str:
        return "bit"


class MSSQLDateTimeType(SqlTimestampType):
    def __str__(self) -> str:
        return "datetime2"
