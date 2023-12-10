from pysqlsync.model.data_types import (
    SqlIntegerType,
    SqlTimestampType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)


class OracleIntegerType(SqlIntegerType):
    def __init__(self) -> None:
        super().__init__(8)

    def __str__(self) -> str:
        return "number"


class OracleTimestampType(SqlTimestampType):
    def __str__(self) -> str:
        if self.precision is not None and self.precision != 6:
            precision = f"({self.precision})"
        else:
            precision = ""
        if self.with_time_zone:
            time_zone = " with time zone"
        else:
            time_zone = ""
        return f"timestamp{precision}{time_zone}"


class OracleVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        if self.limit is not None and self.limit <= 4000:
            return f"varchar2({self.limit})"
        else:
            return "clob"


class OracleVariableBinaryType(SqlVariableBinaryType):
    def __str__(self) -> str:
        if self.storage is not None and self.storage <= 2000:
            return f"raw({self.storage})"
        else:
            return "blob"
