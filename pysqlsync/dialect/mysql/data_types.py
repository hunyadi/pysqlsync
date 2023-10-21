from pysqlsync.model.data_types import SqlTimestampType, SqlVariableCharacterType


class MySQLDateTimeType(SqlTimestampType):
    def __str__(self) -> str:
        return "datetime"


class MySQLVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        if self.limit is None:
            return "text"

        if self.limit < 256:
            return f"varchar({self.limit})"
        elif self.limit < 65536:
            return f"text"
        elif self.limit < 16777216:
            return "mediumtext"
        elif self.limit < 4294967296:
            return "longtext"
        else:
            raise ValueError(f"storage size exceeds maximum: {self.limit}")
