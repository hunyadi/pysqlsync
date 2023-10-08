from dataclasses import dataclass
from functools import reduce
from typing import Any, Optional

from strong_typing.auxiliary import MaxLength, Precision, Storage, TimePrecision

from .id_types import SupportsQualifiedId


def quote(s: str) -> str:
    "Quotes a string to be embedded in an SQL statement."

    return "'" + s.replace("'", "''") + "'"


def constant(v: Any) -> str:
    "Outputs a constant value."

    if isinstance(v, str):
        return quote(v)
    elif isinstance(v, (int, float)):
        return str(v)
    elif isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    else:
        raise NotImplementedError(f"unknown constant representation for value: {v}")


def escape_like(value: str, escape_char: str) -> str:
    "Escapes a string to be embedded in an SQL LIKE '...' ESCAPE '...' expression."

    return (
        value.replace("'", "''")
        .replace(escape_char, f"{escape_char}{escape_char}")
        .replace("_", f"{escape_char}_")
        .replace("%", f"{escape_char}%")
    )


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
class SqlEnumType(SqlDataType):
    values: list[str]

    def __str__(self) -> str:
        values = ", ".join(quote(val) for val in self.values)
        return f"ENUM ({values})"


@dataclass
class SqlUserDefinedType(SqlDataType):
    ref: SupportsQualifiedId

    def __str__(self) -> str:
        return self.ref.quoted_id


class CompatibilityError(TypeError):
    "Raised when a list of types cannot be reduced into a single common type."


def compatible_type(data_types: list[SqlDataType]) -> SqlDataType:
    "Returns a single common type that all elements in a list of types can be converted into without data loss."

    return reduce(_compatible_type, data_types)


def max_or_none(left: Optional[int], right: Optional[int]) -> Optional[int]:
    if left is None or right is None:
        return None
    else:
        return max(left, right)


def _compatible_type(left: SqlDataType, right: SqlDataType) -> SqlDataType:
    if left == right:
        return left

    # integer types
    if isinstance(left, SqlIntegerType) and isinstance(right, SqlIntegerType):
        return SqlIntegerType(width=max(left.width, right.width))

    # floating-point types
    if isinstance(left, SqlRealType):
        if isinstance(right, SqlRealType):
            return SqlRealType()
        elif isinstance(right, SqlDoubleType):
            return SqlDoubleType()
    elif isinstance(left, SqlDoubleType):
        if isinstance(right, (SqlRealType, SqlDoubleType)):
            return SqlDoubleType()

    # character types
    if isinstance(left, SqlCharacterType) and isinstance(right, SqlCharacterType):
        return SqlCharacterType(
            limit=max_or_none(left.limit, right.limit),
            storage=max_or_none(left.storage, right.storage),
        )

    # binary types
    if isinstance(left, SqlFixedBinaryType):
        if isinstance(right, SqlFixedBinaryType):
            return SqlFixedBinaryType(storage=max_or_none(left.storage, right.storage))
        elif isinstance(right, SqlVariableBinaryType):
            return SqlVariableBinaryType(
                storage=max_or_none(left.storage, right.storage)
            )
    elif isinstance(left, SqlVariableBinaryType):
        if isinstance(right, (SqlFixedBinaryType, SqlVariableBinaryType)):
            return SqlVariableBinaryType(
                storage=max_or_none(left.storage, right.storage)
            )

    raise CompatibilityError()
