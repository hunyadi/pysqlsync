import oracledb

from pysqlsync.model.data_types import (
    SqlBooleanType,
    SqlDataType,
    SqlDateType,
    SqlDecimalType,
    SqlDoubleType,
    SqlFixedCharacterType,
    SqlIntegerType,
    SqlRealType,
    SqlTimestampType,
    SqlTimeType,
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


class OracleTimeType(SqlTimeType):
    def __str__(self) -> str:
        return "interval day to second"


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


def sql_to_oracle_type(cur: oracledb.Cursor, data_type: SqlDataType) -> oracledb.Var:
    """
    Returns the Oracle data type associated with the SQL data type.

    Passing the right data types to `setinputsizes` eliminates data-based guessing, and can speed up `executemany`
    by a significant factor.
    """

    if isinstance(data_type, SqlBooleanType):
        return cur.var(oracledb.DB_TYPE_BOOLEAN)
    elif isinstance(data_type, SqlRealType):
        return cur.var(oracledb.DB_TYPE_BINARY_FLOAT)
    elif isinstance(data_type, SqlDoubleType):
        return cur.var(oracledb.DB_TYPE_BINARY_DOUBLE)
    elif isinstance(data_type, SqlIntegerType):
        if data_type.width > 4:
            return cur.var(oracledb.DB_TYPE_NUMBER)
        else:
            return cur.var(oracledb.DB_TYPE_BINARY_INTEGER, data_type.width)
    elif isinstance(data_type, SqlVariableBinaryType):
        if data_type.storage is not None:
            return cur.var(oracledb.DB_TYPE_RAW, data_type.storage)
        else:
            return cur.var(oracledb.DB_TYPE_BLOB)
    elif isinstance(data_type, SqlFixedCharacterType):
        if data_type.limit is not None:
            return cur.var(oracledb.DB_TYPE_CHAR, data_type.limit)
        else:
            return cur.var(oracledb.DB_TYPE_CHAR, 4000)
    elif isinstance(data_type, SqlVariableCharacterType):
        if data_type.limit is not None:
            return cur.var(oracledb.DB_TYPE_VARCHAR, data_type.limit)
        else:
            return cur.var(oracledb.DB_TYPE_CLOB)
    elif isinstance(data_type, SqlDateType):
        return cur.var(oracledb.DB_TYPE_DATE)
    elif isinstance(data_type, SqlTimeType):
        return cur.var(oracledb.DB_TYPE_INTERVAL_DS)
    elif isinstance(data_type, SqlDecimalType):
        return cur.var(oracledb.DB_TYPE_NUMBER)
    elif isinstance(data_type, SqlTimestampType):
        return cur.var(oracledb.DB_TYPE_TIMESTAMP)

    return cur.var(oracledb.DB_TYPE_UNKNOWN)
