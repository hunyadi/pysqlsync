import re
from dataclasses import dataclass
from typing import Any, Optional

from strong_typing.auxiliary import MaxLength, Precision, Storage, TimePrecision

from .id_types import SupportsQualifiedId


@dataclass
class SqlDataType:
    def parse_meta(self, meta: Any) -> None:
        raise TypeError(
            f"unrecognized Python type annotation for {type(self).__name__}: {meta}"
        )


@dataclass
class SqlArrayType(SqlDataType):
    element_type: SqlDataType

    def __str__(self) -> str:
        return f"{self.element_type} ARRAY"


@dataclass
class SqlUuidType(SqlDataType):
    def __str__(self) -> str:
        return "uuid"


@dataclass
class SqlBooleanType(SqlDataType):
    def __str__(self) -> str:
        return "boolean"


@dataclass
class SqlIntegerType(SqlDataType):
    width: int

    def __str__(self) -> str:
        if self.width == 2:
            return "smallint"
        elif self.width == 4:
            return "integer"
        elif self.width == 8:
            return "bigint"

        raise TypeError(f"invalid integer width: {self.width}")


@dataclass
class SqlFloatType(SqlDataType):
    """
    Floating-point numeric type.

    :param precision: Numeric precision in base 2.
    """

    precision: Optional[int] = None

    def __str__(self) -> str:
        if self.precision is not None:
            precision = self.precision
        else:
            precision = 53

        return f"float({precision})"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, Precision):
            self.precision = meta.significant_digits
        else:
            super().parse_meta(meta)


@dataclass
class SqlRealType(SqlDataType):
    def __str__(self) -> str:
        return "real"


@dataclass
class SqlDoubleType(SqlDataType):
    def __str__(self) -> str:
        return "double precision"


@dataclass
class SqlDecimalType(SqlDataType):
    """
    Fixed-point numeric type.

    :param precision: Numeric precision in base 10.
    :param scale: Scale in base 10.
    """

    precision: Optional[int] = None
    scale: Optional[int] = None

    def __str__(self) -> str:
        if self.precision is not None and self.scale is not None:
            return f"decimal({self.precision}, {self.scale})"
        elif self.precision is not None:
            return f"decimal({self.precision})"
        else:
            return "decimal"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, Precision):
            self.precision = meta.significant_digits
            self.scale = meta.decimal_digits
        else:
            super().parse_meta(meta)


@dataclass
class SqlFixedBinaryType(SqlDataType):
    storage: Optional[int] = None

    def __str__(self) -> str:
        if self.storage is not None:
            storage = f"({self.storage})"
        else:
            storage = ""
        return f"binary{storage}"


@dataclass
class SqlVariableBinaryType(SqlDataType):
    storage: Optional[int] = None

    def __str__(self) -> str:
        if self.storage is not None:
            if self.storage < 65536:
                return f"varbinary({self.storage})"
            elif self.storage < 16777216:
                return "mediumblob"  # MySQL-specific
            elif self.storage < 4294967296:
                return "longblob"  # MySQL-specific
            else:
                raise ValueError(f"storage size exceeds maximum: {self.storage}")

        return "varbinary"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, Storage):
            self.storage = meta.bytes
        else:
            super().parse_meta(meta)


@dataclass
class SqlCharacterType(SqlDataType):
    limit: Optional[int] = None
    storage: Optional[int] = None

    def __str__(self) -> str:
        if self.storage is not None:
            if self.storage < 65536:
                return f"varchar({self.storage})"
            elif self.storage < 16777216:
                return "mediumtext"  # MySQL-specific
            elif self.storage < 4294967296:
                return "longtext"  # MySQL-specific
            else:
                raise ValueError(f"storage size exceeds maximum: {self.storage}")

        if self.limit is not None:
            return f"varchar({self.limit})"
        else:
            return "text"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, MaxLength):
            self.limit = meta.value
        elif isinstance(meta, Storage):
            self.storage = meta.bytes
        else:
            super().parse_meta(meta)


@dataclass
class SqlTimestampType(SqlDataType):
    precision: Optional[int] = None
    with_time_zone: bool = False

    def __str__(self) -> str:
        if self.precision is not None:
            precision = f"({self.precision})"
        else:
            precision = ""
        if self.with_time_zone:
            time_zone = " with time zone"
        else:
            time_zone = ""  # PostgreSQL: " without time zone"
        return f"timestamp{precision}{time_zone}"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, TimePrecision):
            self.precision = meta.decimal_digits
        else:
            super().parse_meta(meta)


@dataclass
class SqlDateType(SqlDataType):
    def __str__(self) -> str:
        return "date"


@dataclass
class SqlTimeType(SqlDataType):
    precision: Optional[int] = None
    with_time_zone: bool = False

    def __str__(self) -> str:
        if self.precision is not None:
            precision = f"({self.precision})"
        else:
            precision = ""
        if self.with_time_zone:
            time_zone = " with time zone"
        else:
            time_zone = ""  # PostgreSQL: " without time zone"
        return f"time{precision}{time_zone}"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, TimePrecision):
            self.precision = meta.decimal_digits
        else:
            super().parse_meta(meta)


@dataclass
class SqlIntervalType(SqlDataType):
    def __str__(self) -> str:
        return "interval"


@dataclass
class SqlJsonType(SqlDataType):
    def __str__(self) -> str:
        return "json"


@dataclass
class SqlUserDefinedType(SqlDataType):
    ref: SupportsQualifiedId

    def __str__(self) -> str:
        return self.ref.quoted_id


def sql_data_type_from_spec(
    type_name: str,
    *,
    character_maximum_length: Optional[int] = None,
    numeric_precision: Optional[int] = None,
    numeric_scale: Optional[int] = None,
    timestamp_precision: Optional[int] = None,
) -> SqlDataType:
    if type_name == "boolean":
        return SqlBooleanType()
    elif type_name == "tinyint":
        return SqlIntegerType(1)
    elif type_name == "smallint":
        return SqlIntegerType(2)
    elif type_name == "int" or type_name == "integer":
        return SqlIntegerType(4)
    elif type_name == "bigint":
        return SqlIntegerType(8)
    elif type_name == "numeric" or type_name == "decimal":
        return SqlDecimalType(numeric_precision, numeric_scale)  # precision in base 10
    elif type_name == "real":
        if numeric_precision is None or numeric_precision == 24:
            return SqlRealType()
        else:
            return SqlFloatType(numeric_precision)  # precision in base 2
    elif type_name == "double" or type_name == "double precision":
        if numeric_precision is None or numeric_precision == 53:
            return SqlDoubleType()
        else:
            return SqlFloatType(numeric_precision)  # precision in base 2
    elif type_name == "float":
        return SqlFloatType(numeric_precision)  # precision in base 2
    elif type_name == "timestamp" or type_name == "timestamp without time zone":
        return SqlTimestampType(timestamp_precision, False)
    elif type_name == "timestamp with time zone":
        return SqlTimestampType(timestamp_precision, True)
    elif type_name == "date":
        return SqlDateType()
    elif type_name == "time" or type_name == "time without time zone":
        return SqlTimeType(timestamp_precision, False)
    elif type_name == "time with time zone":
        return SqlTimeType(timestamp_precision, True)
    elif type_name == "varchar" or type_name == "character varying":
        return SqlCharacterType(limit=character_maximum_length)
    elif type_name == "text":
        return SqlCharacterType()
    elif type_name == "mediumtext":  # MySQL-specific
        return SqlCharacterType(storage=16777215)
    elif type_name == "longtext":  # MySQL-specific
        return SqlCharacterType(storage=4294967295)
    elif type_name == "blob":  # MySQL-specific
        return SqlVariableBinaryType(storage=65535)
    elif type_name == "mediumblob":  # MySQL-specific
        return SqlVariableBinaryType(storage=16777215)
    elif type_name == "longblob":  # MySQL-specific
        return SqlVariableBinaryType(storage=4294967295)
    elif type_name == "bytea":  # PostgreSQL-specific
        return SqlVariableBinaryType()
    elif type_name == "uuid":  # PostgreSQL-specific
        return SqlUuidType()
    elif type_name == "json" or type_name == "jsonb":
        return SqlJsonType()

    m = re.fullmatch(
        r"^(?:decimal|numeric)[(](\d+),\s*(\d+)[)]$", type_name, re.IGNORECASE
    )
    if m is not None:
        return SqlDecimalType(int(m.group(1)), int(m.group(2)))

    m = re.fullmatch(r"^timestamp[(](\d+)[)]$", type_name, re.IGNORECASE)
    if m is not None:
        return SqlTimestampType(int(m.group(1)), False)

    m = re.fullmatch(r"^varchar[(](\d+)[)]$", type_name, re.IGNORECASE)
    if m is not None:
        return SqlCharacterType(int(m.group(1)))

    raise TypeError(f"unrecognized SQL type: {type_name}")
