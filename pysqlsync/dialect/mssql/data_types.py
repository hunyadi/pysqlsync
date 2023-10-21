from pysqlsync.model.data_types import (
    SqlBooleanType,
    SqlCharacterType,
    SqlTimestampType,
)


class MSSQLBooleanType(SqlBooleanType):
    def __str__(self) -> str:
        return "bit"


class MSSQLCharacterType(SqlCharacterType):
    def __str__(self) -> str:
        if self.limit is not None:
            return f"varchar({self.limit})"
        else:
            return "varchar(max)"


class MSSQLDateTimeType(SqlTimestampType):
    def __str__(self) -> str:
        return "datetime2"