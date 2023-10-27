from pysqlsync.model.data_types import (
    SqlTimestampType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)


class MySQLDateTimeType(SqlTimestampType):
    def __str__(self) -> str:
        if self.precision is not None:
            precision = f"({self.precision})"
        else:
            precision = ""
        return f"datetime{precision}"


class MySQLVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        if self.limit is None:
            return "mediumtext"

        if self.limit < 65536:
            return f"varchar({self.limit})"
        elif self.limit < 16777216:
            return "mediumtext"
        elif self.limit < 4294967296:
            return "longtext"
        else:
            raise ValueError(f"character count exceeds maximum: {self.limit}")


class MySQLVariableBinaryType(SqlVariableBinaryType):
    def __str__(self) -> str:
        if self.storage is None:
            return "mediumblob"

        if self.storage < 65536:
            return f"varbinary({self.storage})"
        elif self.storage < 16777216:
            return "mediumblob"
        elif self.storage < 4294967296:
            return "longblob"
        else:
            raise ValueError(f"storage size exceeds maximum: {self.storage}")
