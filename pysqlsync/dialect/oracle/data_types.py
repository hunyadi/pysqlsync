from pysqlsync.model.data_types import (
    SqlFixedBinaryType,
    SqlIntegerType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)


class OracleIntegerType(SqlIntegerType):
    def __init__(self) -> None:
        super().__init__(8)

    def __str__(self) -> str:
        return "number"


class OracleVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        if self.limit is not None and self.limit <= 4000:
            return f"varchar2({self.limit})"
        else:
            return "clob"


class OracleFixedBinaryType(SqlFixedBinaryType):
    def __str__(self) -> str:
        if self.storage is not None and self.storage <= 2000:
            return f"raw({self.storage})"
        else:
            return "blob"


class OracleVariableBinaryType(SqlVariableBinaryType):
    def __str__(self) -> str:
        if self.storage is not None and self.storage <= 2000:
            return f"raw({self.storage})"
        else:
            return "blob"
