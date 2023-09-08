from dataclasses import dataclass
from typing import Any, Optional

from strong_typing.auxiliary import MaxLength, Precision, TimePrecision

from .id_types import QualifiedId


@dataclass
class SqlDataType:
    def parse_meta(self, meta: Any) -> None:
        raise TypeError(
            f"unrecognized Python type annotation for {type(self).__name__}: {meta}"
        )


@dataclass
class SqlArrayType(SqlDataType):
    inner_type: SqlDataType

    def __str__(self) -> str:
        return f"{self.inner_type} ARRAY"


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
    def __str__(self) -> str:
        return "real"


@dataclass
class SqlDoubleType(SqlDataType):
    def __str__(self) -> str:
        return "double precision"


@dataclass
class SqlDecimalType(SqlDataType):
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
class SqlCharacterType(SqlDataType):
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
            time_zone = "with time zone"
        else:
            time_zone = "without time zone"
        return f"timestamp{precision} {time_zone}"

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
            time_zone = "with time zone"
        else:
            time_zone = "without time zone"
        return f"time{precision} {time_zone}"

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
class SqlUserDefinedType(SqlDataType):
    ref: QualifiedId

    def __str__(self) -> str:
        return str(self.ref)


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
    elif type_name == "smallint":
        return SqlIntegerType(2)
    elif type_name == "integer":
        return SqlIntegerType(4)
    elif type_name == "bigint":
        return SqlIntegerType(8)
    elif type_name == "real":
        return SqlFloatType()
    elif type_name == "double precision":
        return SqlDoubleType()
    elif type_name == "numeric":
        return SqlDecimalType(numeric_precision, numeric_scale)
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
        return SqlCharacterType(character_maximum_length)
    elif type_name == "text":
        return SqlCharacterType()
    elif type_name == "uuid":
        return SqlUuidType()
    else:
        raise TypeError(f"unrecognized SQL type: {type_name}")
