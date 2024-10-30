import datetime
import decimal
from dataclasses import dataclass
from functools import reduce
from typing import Any, Optional

from strong_typing.auxiliary import (
    IntegerRange,
    MaxLength,
    Precision,
    Signed,
    Storage,
    TimePrecision,
)
from strong_typing.inspection import dataclass_fields, is_dataclass_instance

from .id_types import LocalId, SupportsQualifiedId


def quote(s: str) -> str:
    "Quotes a string to be embedded in an SQL statement."

    return "'" + s.replace("'", "''") + "'"


def constant(v: Any) -> str:
    "Outputs a constant value."

    if isinstance(v, str):
        return quote(v)
    elif isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    elif isinstance(v, (int, float)):
        return str(v)
    elif isinstance(v, decimal.Decimal):
        return str(v)
    elif isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            timestamp = v.astimezone(tz=datetime.timezone.utc).replace(tzinfo=None)
        else:
            timestamp = v
        return quote(timestamp.isoformat(sep=" "))
    elif isinstance(v, tuple):
        values = ", ".join(constant(value) for value in v)
        return f"({values})"
    elif is_dataclass_instance(v):
        values = ", ".join(
            constant(getattr(v, field.name)) for field in dataclass_fields(type(v))
        )
        return f"({values})"
    else:
        raise NotImplementedError(
            f"unknown constant representation for value (of type): {v} ({type(v)})"
        )


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
    signed: bool = True
    minimum: Optional[int] = None
    maximum: Optional[int] = None

    def __str__(self) -> str:
        if self.width == 1:
            return "tinyint"
        elif self.width == 2:
            return "smallint"
        elif self.width == 4:
            return "integer"
        elif self.width == 8:
            return "bigint"

        raise TypeError(f"invalid integer width: {self.width}")

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, IntegerRange):
            self.minimum = meta.minimum
            self.maximum = meta.maximum
        elif isinstance(meta, Signed):
            self.signed = meta.is_signed
        elif isinstance(meta, Storage):
            self.width = meta.bytes
        else:
            super().parse_meta(meta)


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
        storage = f"({self.storage})" if self.storage is not None else ""
        return f"binary{storage}"


@dataclass
class SqlVariableBinaryType(SqlDataType):
    storage: Optional[int] = None

    def __str__(self) -> str:
        if self.storage is not None:
            return f"varbinary({self.storage})"
        else:
            return "blob"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, Storage):
            self.storage = meta.bytes
        else:
            super().parse_meta(meta)


@dataclass
class SqlFixedCharacterType(SqlDataType):
    limit: Optional[int] = None

    def __str__(self) -> str:
        limit = f"({self.limit})" if self.limit is not None else ""
        return f"char{limit}"


@dataclass
class SqlVariableCharacterType(SqlDataType):
    limit: Optional[int] = None

    def __str__(self) -> str:
        if self.limit is not None:
            return f"varchar({self.limit})"
        else:
            return "text"

    def parse_meta(self, meta: Any) -> None:
        if isinstance(meta, MaxLength):
            self.limit = meta.value
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
class SqlStructMember:
    name: LocalId
    data_type: SqlDataType
    nullable: bool

    def __str__(self) -> str:
        nullable = " NOT NULL" if not self.nullable else ""
        return f"{self.name} {self.data_type}{nullable}"


@dataclass
class SqlStructType(SqlDataType):
    fields: list[SqlStructMember]

    def __str__(self) -> str:
        field_list = ", ".join(str(f) for f in self.fields)
        return f"STRUCT <{field_list}>"


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
    """
    Returns a common type that can represent instances of both source types without data loss.

    The implementation makes use of `type(...)` to ensure the most specific class is instantiated when
    database-specific data types are created with inheritance.
    """

    if left == right:
        return left

    # integer types
    if isinstance(left, SqlIntegerType) and isinstance(right, SqlIntegerType):
        return type(left)(width=max(left.width, right.width))

    # floating-point types
    if isinstance(left, SqlRealType):
        if isinstance(right, SqlRealType):
            return type(left)()
        elif isinstance(right, SqlDoubleType):
            return type(right)()
    elif isinstance(left, SqlDoubleType):
        if isinstance(right, (SqlRealType, SqlDoubleType)):
            return type(left)()

    # character types
    if isinstance(left, SqlFixedCharacterType):
        if isinstance(right, SqlFixedCharacterType):
            return type(left)(limit=max_or_none(left.limit, right.limit))
        elif isinstance(right, SqlVariableCharacterType):
            return type(right)(limit=max_or_none(left.limit, right.limit))
    elif isinstance(left, SqlVariableCharacterType):
        if isinstance(right, (SqlFixedCharacterType, SqlVariableCharacterType)):
            return type(left)(limit=max_or_none(left.limit, right.limit))

    # binary types
    if isinstance(left, SqlFixedBinaryType):
        if isinstance(right, SqlFixedBinaryType):
            return type(left)(storage=max_or_none(left.storage, right.storage))
        elif isinstance(right, SqlVariableBinaryType):
            return type(right)(storage=max_or_none(left.storage, right.storage))
    elif isinstance(left, SqlVariableBinaryType):
        if isinstance(right, (SqlFixedBinaryType, SqlVariableBinaryType)):
            return type(left)(storage=max_or_none(left.storage, right.storage))

    raise CompatibilityError()
