from dataclasses import dataclass

from pysqlsync.model.data_types import (
    SqlDoubleType,
    SqlFixedBinaryType,
    SqlRealType,
    SqlTimestampType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)


@dataclass
class DeltaRealType(SqlRealType):
    def __str__(self) -> str:
        return "float"


@dataclass
class DeltaDoubleType(SqlDoubleType):
    def __str__(self) -> str:
        return "double"


@dataclass
class DeltaFixedBinaryType(SqlFixedBinaryType):
    def __str__(self) -> str:
        return "binary"


@dataclass
class DeltaVariableBinaryType(SqlVariableBinaryType):
    def __str__(self) -> str:
        return "binary"


@dataclass
class DeltaVariableCharacterType(SqlVariableCharacterType):
    def __str__(self) -> str:
        return "string"


@dataclass
class DeltaTimestampType(SqlTimestampType):
    "Timestamp without time zone."

    def __init__(self) -> None:
        self.precision = 9

    def __str__(self) -> str:
        return "timestamp_ntz"
