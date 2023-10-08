from pysqlsync.model.data_types import SqlTimestampType


class MySQLDateTimeType(SqlTimestampType):
    def __str__(self) -> str:
        return "datetime"
