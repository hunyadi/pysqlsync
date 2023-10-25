from pysqlsync.model.data_types import (
    SqlBooleanType,
    SqlTimestampType,
    SqlVariableCharacterType,
)


class MSSQLBooleanType(SqlBooleanType):
    def __str__(self) -> str:
        return "bit"


class MSSQLVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        if self.limit is not None and self.limit > 0 and self.limit != 2147483647:
            return f"varchar({self.limit})"
        else:
            return "varchar(max)"


class MSSQLDateTimeType(SqlTimestampType):
    def __init__(self) -> None:
        self.precision = 7

    def __str__(self) -> str:
        return "datetime2"
