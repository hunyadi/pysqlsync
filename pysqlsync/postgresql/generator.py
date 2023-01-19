import dataclasses
import datetime
import decimal
import typing
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Optional, TextIO

from strong_typing.auxiliary import (
    MaxLength,
    Precision,
    TimePrecision,
    float32,
    float64,
    int8,
    int16,
    int32,
    int64,
    uint8,
    uint16,
    uint32,
    uint64,
)
from strong_typing.inspection import (
    is_type_enum,
    is_type_optional,
    unwrap_optional_type,
    enum_value_types,
)

from ..base import PrimaryKey, BaseGenerator, get_primary_key


def sql_quoted_id(name: str) -> str:
    id = name.replace('"', '""')
    return f'"{id}"'


class SqlDataType:
    def _unrecognized_meta(self, meta: Any) -> None:
        raise TypeError(
            f"unrecognized Python type annotation for {type(self).__name__}: {meta}"
        )


@dataclass
class SqlCharacterType(SqlDataType):
    max_len: Optional[int] = None
    compact: Optional[bool] = False

    def __init__(self, metadata: Iterable[Any], compact: bool = False) -> None:
        for meta in metadata:
            if isinstance(meta, MaxLength):
                self.max_len = meta.value
            else:
                self._unrecognized_meta(meta)
        self.compact = compact

    def __str__(self) -> str:
        type_name = "varchar" if self.compact else "character varying"
        if self.max_len is not None:
            return f"{type_name}({self.max_len})"
        else:
            return type_name


@dataclass
class SqlDecimalType(SqlDataType):
    precision: Optional[int] = None
    scale: Optional[int] = None

    def __init__(self, metadata: Iterable[Any]) -> None:
        for meta in metadata:
            if isinstance(meta, Precision):
                self.precision = meta.significant_digits
                self.scale = meta.decimal_digits
            else:
                self._unrecognized_meta(meta)

    def __str__(self) -> str:
        if self.precision is not None and self.scale is not None:
            return f"decimal({self.precision}, {self.scale})"
        elif self.precision is not None:
            return f"decimal({self.precision})"
        else:
            return "decimal"


@dataclass
class SqlTimestampType(SqlDataType):
    precision: Optional[int] = None

    def __init__(self, metadata: Iterable[Any]) -> None:
        for meta in metadata:
            if isinstance(meta, TimePrecision):
                self.precision = meta.decimal_digits
            else:
                self._unrecognized_meta(meta)

    def __str__(self) -> str:
        if self.precision is not None:
            return f"timestamp({self.precision})"
        else:
            return "timestamp"


@dataclass
class SqlTimeType(SqlDataType):
    precision: Optional[int] = None

    def __init__(self, metadata: Iterable[Any]) -> None:
        for meta in metadata:
            if isinstance(meta, TimePrecision):
                self.precision = meta.decimal_digits
            else:
                self._unrecognized_meta(meta)

    def __str__(self) -> str:
        if self.precision is not None:
            return f"time({self.precision})"
        else:
            return "time"


def python_to_sql_type(typ: type, compact: bool = False) -> str:
    "Maps a native Python type to a PostgreSQL type."

    if typ is bool:
        return "boolean"
    if typ is int:
        return "integer"
    if typ is int8:
        return "int1"  # PostgreSQL extension required
    if typ is int16:
        return "smallint"
    if typ is int32:
        return "int"
    if typ is int64:
        return "bigint"
    if typ is uint8:
        return "uint1"  # PostgreSQL extension required
    if typ is uint16:
        return "uint2"  # PostgreSQL extension required
    if typ is uint32:
        return "uint4"  # PostgreSQL extension required
    if typ is uint64:
        return "uint8"  # PostgreSQL extension required
    if typ is float32:
        return "real"
    if typ is float64 or typ is float:
        if compact:
            return "double"
        else:
            return "double precision"
    if typ is str:
        return "text"
    if typ is decimal.Decimal:
        return "decimal"
    if typ is datetime.datetime:
        return "timestamp without time zone"
    if typ is datetime.date:
        return "date"
    if typ is datetime.time:
        return "time without time zone"
    if typ is datetime.timedelta:
        return "interval"
    if typ is uuid.UUID:
        return "uuid"

    metadata = getattr(typ, "__metadata__", None)
    if metadata is not None:
        # type is Annotated[T, ...]
        inner_type = typing.get_args(typ)[0]

        if inner_type is str:
            return str(SqlCharacterType(metadata, compact=compact))
        elif inner_type is decimal.Decimal:
            return str(SqlDecimalType(metadata))
        elif inner_type is datetime.datetime:
            return str(SqlTimestampType(metadata))
        elif inner_type is datetime.time:
            return str(SqlTimeType(metadata))
        else:
            raise TypeError(f"cannot map annotated Python type: {typ}")

    if is_type_enum(typ):
        value_types = enum_value_types(typ)
        if len(value_types) > 1:
            raise TypeError(
                f"inconsistent enumeration value types for type {typ.__name__}: {value_types}"
            )

        value_type = value_types.pop()
        return python_to_sql_type(value_type, compact=compact)

    raise NotImplementedError(f"cannot map Python type: {typ}")


class Generator(BaseGenerator):
    def write_create_table_stmt(self, target: TextIO) -> None:
        class_sql_name = sql_quoted_id(self.cls.__name__)

        defs: list[str] = []
        for field in dataclasses.fields(self.cls):
            field_sql_name = sql_quoted_id(field.name)

            origin_type = typing.get_origin(field.type)
            if origin_type is PrimaryKey:
                (key_type,) = typing.get_args(field.type)
                sql_type = python_to_sql_type(key_type)
                defs.append(f"{field_sql_name} {sql_type} PRIMARY KEY")

            else:
                if is_type_optional(field.type):
                    sql_inner_type = unwrap_optional_type(field.type)
                    sql_type = python_to_sql_type(sql_inner_type)
                else:
                    sql_type = python_to_sql_type(field.type)

                if is_type_optional(field.type):
                    extended_sql_type = f"{sql_type}"
                else:
                    extended_sql_type = f"{sql_type} NOT NULL"

                defs.append(f"{field_sql_name} {extended_sql_type}")

        print(f"CREATE TABLE {class_sql_name} (", file=target)
        print(",\n".join(defs), file=target)
        print(f")\n", file=target)

    def write_insert_stmt(self, target: TextIO) -> None:
        print(f"INSERT INTO {sql_quoted_id(self.cls.__name__)}", file=target)
        field_names = [field.name for field in dataclasses.fields(self.cls)]
        field_list = ", ".join(sql_quoted_id(field_name) for field_name in field_names)
        value_list = ", ".join(f"${index}" for index in range(1, len(field_names) + 1))
        print(f"({field_list}) VALUES ({value_list})", file=target)

        primary_key_name, primary_key_type = get_primary_key(self.cls)
        print(
            f"ON CONFLICT({sql_quoted_id(primary_key_name)}) DO UPDATE SET", file=target
        )
        defs = [
            f"{sql_quoted_id(field_name)} = EXCLUDED.{sql_quoted_id(field_name)}"
            for field_name in field_names
            if field_name != primary_key_name
        ]
        print(",\n".join(defs), file=target)
