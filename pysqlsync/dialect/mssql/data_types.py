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
        return pyodbc.SQL_CHAR, data_type.limit or 0, 0
    elif isinstance(data_type, SqlVariableCharacterType):
        return pyodbc.SQL_VARCHAR, data_type.limit or 0, 0
    elif isinstance(data_type, SqlFixedBinaryType):
        return pyodbc.SQL_BINARY, data_type.storage or 0, 0
    elif isinstance(data_type, SqlVariableBinaryType):
        return pyodbc.SQL_VARBINARY, data_type.storage or 0, 0

    return pyodbc.SQL_UNKNOWN_TYPE, 0, 0
