from pysqlsync.model.data_types import SqlJsonType, SqlTimestampType


class SnowflakeDateTimeType(SqlTimestampType):
    "Timestamp without time zone, equivalent to TIMESTAMP_NTZ."

    def __init__(self) -> None:
        self.precision = 9

    def __str__(self) -> str:
        return "datetime"


class SnowflakeJsonType(SqlJsonType):
    "Represents JSON data extracted with PARSE_JSON."

    def __str__(self) -> str:
        return "variant"
