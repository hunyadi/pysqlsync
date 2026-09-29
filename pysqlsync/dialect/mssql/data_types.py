"""
pysqlsync: Synchronize schema and large volumes of data.

Copyright 2023-2026, Levente Hunyadi

:see: https://github.com/hunyadi/pysqlsync
"""

import enum
from typing import Any, Final

import pyodbc

from pysqlsync.model.data_types import (
    SqlBooleanType,
    SqlDataType,
    SqlDateType,
    SqlDecimalType,
    SqlDoubleType,
    SqlFixedBinaryType,
    SqlFixedCharacterType,
    SqlFloatType,
    SqlIntegerType,
    SqlRealType,
    SqlTimestampType,
    SqlTimeType,
    SqlVariableBinaryType,
    SqlVariableCharacterType,
)

MAX_VARCHAR_SIZE: Final[int] = 8000
MAX_NVARCHAR_SIZE: Final[int] = 4000


class MSSQLBooleanType(SqlBooleanType):
    def __str__(self) -> str:
        return "bit"

    def value_to_sql_literal(self, value: Any) -> str:
        if not isinstance(value, bool):
            raise TypeError(f"expected: value of type `bool`, got: {type(value)}")

        return "1" if value else "0"


class MSSQLEncoding(enum.Enum):
    UTF8 = "utf-8"
    UTF16 = "utf-16"


class MSSQLVariableCharacterType(SqlVariableCharacterType):
    encoding: MSSQLEncoding | None = None

    def __init__(self, limit: int | None = None, encoding: MSSQLEncoding | None = None) -> None:
        super().__init__(limit)
        self.encoding = encoding

    def _max_size(self) -> int:
        "Returns the widest fixed-width size this column's type family accepts."

        if self.encoding is MSSQLEncoding.UTF16:
            return MAX_NVARCHAR_SIZE
        else:
            return MAX_VARCHAR_SIZE

    def __str__(self) -> str:
        if self.encoding is MSSQLEncoding.UTF16:
            char_type = "nvarchar"
        else:
            char_type = "varchar"

        if self.limit is not None and self.limit > 0 and self.limit <= self._max_size():
            return f"{char_type}({self.limit})"
        else:
            return f"{char_type}(max)"


class MSSQLDateTimeType(SqlTimestampType):
    def __init__(self) -> None:
        self.precision = 7

    def __str__(self) -> str:
        return "datetime2"


def _wide_char_transport_size(limit: int | None) -> int:
    """
    Returns the ODBC column size to declare for the transport of a wide (Unicode) character string.

    :param limit: The column's declared character limit, or `None` if unbounded.
    :returns: The limit if it fits a fixed-width wide type, or 0 to bind as a LOB.
    """

    if limit is not None and limit <= MAX_NVARCHAR_SIZE:
        return limit
    else:
        return 0


def sql_to_odbc_type(data_type: SqlDataType) -> tuple[int, int, int]:
    """
    Returns the ODBC data type associated with the SQL data type.

    Passing the right data types to `setinputsizes` eliminates data-based guessing, and can speed up `executemany`
    by a significant factor.
    """

    if isinstance(data_type, MSSQLBooleanType):
        return pyodbc.SQL_BIT, 0, 0

    elif isinstance(data_type, SqlIntegerType):
        if data_type.width == 1:
            return pyodbc.SQL_TINYINT, 0, 0
        elif data_type.width == 2:
            return pyodbc.SQL_SMALLINT, 0, 0
        elif data_type.width == 4:
            return pyodbc.SQL_INTEGER, 0, 0
        else:
            return pyodbc.SQL_BIGINT, 0, 0

    elif isinstance(data_type, SqlRealType):
        return pyodbc.SQL_REAL, 0, 0
    elif isinstance(data_type, SqlDoubleType):
        return pyodbc.SQL_DOUBLE, 0, 0
    elif isinstance(data_type, SqlFloatType):
        return pyodbc.SQL_FLOAT, data_type.precision or 53, 0
    elif isinstance(data_type, SqlDecimalType):
        return pyodbc.SQL_DECIMAL, data_type.precision or 15, data_type.scale or 0

    elif isinstance(data_type, SqlTimestampType):
        return pyodbc.SQL_TYPE_TIMESTAMP, data_type.precision or 6, 0
    elif isinstance(data_type, SqlDateType):
        return pyodbc.SQL_TYPE_DATE, 0, 0
    elif isinstance(data_type, SqlTimeType):
        return pyodbc.SQL_TYPE_TIME, data_type.precision or 6, 0

    elif isinstance(data_type, SqlFixedCharacterType):
        return pyodbc.SQL_WCHAR, _wide_char_transport_size(data_type.limit), 0
    elif isinstance(data_type, SqlVariableCharacterType):
        return pyodbc.SQL_WVARCHAR, _wide_char_transport_size(data_type.limit), 0
    elif isinstance(data_type, SqlFixedBinaryType):
        return pyodbc.SQL_BINARY, data_type.storage or 0, 0
    elif isinstance(data_type, SqlVariableBinaryType):
        return pyodbc.SQL_VARBINARY, data_type.storage or 0, 0

    return pyodbc.SQL_UNKNOWN_TYPE, 0, 0
