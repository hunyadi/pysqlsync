import dataclasses
import datetime
import keyword
import sys
import types
from io import StringIO
from typing import Annotated, Any, Union
from uuid import UUID

from strong_typing.auxiliary import (
    Precision,
    TimePrecision,
    float32,
    float64,
    int16,
    int32,
    int64,
)
from strong_typing.core import JsonType
from strong_typing.inspection import DataclassInstance, TypeLike

from ..model.data_types import *
from ..model.id_types import SupportsQualifiedId
from ..model.key_types import PrimaryKey
from .object_types import (
    Column,
    DiscriminatedConstraint,
    ForeignConstraint,
    ReferenceConstraint,
    Table,
)


def sql_type_to_python(sql_type: SqlDataType) -> TypeLike:
    if isinstance(sql_type, SqlBooleanType):
        return bool
    elif isinstance(sql_type, SqlIntegerType):
        if sql_type.width == 2:
            return int16
        elif sql_type.width == 4:
            return int32
        elif sql_type.width == 8:
            return int64
        return int
    elif isinstance(sql_type, SqlRealType):
        return float32
    elif isinstance(sql_type, SqlDoubleType):
        return float64
    elif isinstance(sql_type, SqlFloatType):
        if sql_type.precision is not None:
            return Annotated[float, Precision(sql_type.precision)]
        return float
    elif isinstance(sql_type, SqlTimestampType):
        if sql_type.precision is not None:
            return Annotated[datetime.datetime, TimePrecision(sql_type.precision)]
        return datetime.datetime
    elif isinstance(sql_type, SqlDateType):
        return datetime.date
    elif isinstance(sql_type, SqlTimeType):
        if sql_type.precision is not None:
            return Annotated[datetime.time, TimePrecision(sql_type.precision)]
        return datetime.time
    elif isinstance(sql_type, SqlCharacterType):
        if sql_type.limit is not None:
            return Annotated[str, MaxLength(sql_type.limit)]
        return str
    elif isinstance(sql_type, SqlUuidType):
        return UUID
    elif isinstance(sql_type, SqlJsonType):
        return JsonType
    elif isinstance(sql_type, SqlUserDefinedType):
        return sql_type.ref.compact_id

    raise TypeError(f"unrecognized SQL type: {sql_type}")


def safe_id(id: str) -> str:
    "Apply PEP 8: single trailing underscore to avoid conflicts with Python keyword"

    return f"{id}_" if keyword.iskeyword(id) else id


@dataclass
class SqlConverterOptions:
    namespaces: dict[Optional[str], types.ModuleType]


class SqlConverter:
    options: SqlConverterOptions

    def __init__(self, options: SqlConverterOptions) -> None:
        self.options = options

    def qual_to_module(self, id: SupportsQualifiedId) -> str:
        return f"{self.options.namespaces[id.scope_id].__name__}.{safe_id(id.local_id)}"

    def column_to_field(
        self, table: Table, column: Column
    ) -> tuple[str, TypeLike, dataclasses.Field]:
        """
        Generates a dataclass field corresponding to a table column.

        :param column: The database column from which to produce a dataclass field.
        """

        field_name = safe_id(column.name.local_id)

        default: Any = dataclasses.MISSING
        if column.default is not None:
            default = column.default

        field_type = sql_type_to_python(column.data_type)
        if column.name == table.primary_key:
            field_type = PrimaryKey[(field_type,)]  # type: ignore
        elif column.nullable:
            field_type = Optional[field_type]

        if table.constraints is not None:
            for c in table.constraints:
                if not isinstance(c, ReferenceConstraint):
                    continue

                if column.name != c.foreign_column:
                    continue

                if isinstance(c, ForeignConstraint):
                    field_type = self.qual_to_module(c.reference.table)
                elif isinstance(c, DiscriminatedConstraint):
                    union_types = tuple(
                        self.qual_to_module(r.table) for r in c.references
                    )
                    field_type = Union[union_types]

        return (
            field_name,
            field_type,
            dataclasses.field(default=default),
        )

    def table_to_dataclass(self, table: Table) -> type[DataclassInstance]:
        """
        Generates a dataclass type corresponding to a table schema.

        :param table: The database table from which to produce a dataclass.
        """

        fields = [
            self.column_to_field(table, column) for column in table.columns.values()
        ]
        class_name = safe_id(table.name.local_id)

        # default arguments must follow non-default arguments
        fields.sort(key=lambda f: f[2].default is not dataclasses.MISSING)

        # produce class definition with docstring
        typ = dataclasses.make_dataclass(class_name, fields)  # type: ignore
        with StringIO() as out:
            for field in dataclasses.fields(typ):
                description = field.metadata.get("description")
                if description is not None:
                    print(f":param {field.name}: {description}", file=out)
            paramstring = out.getvalue()
        with StringIO() as out:
            if table.description:
                out.write(table.description)
            if table.description and paramstring:
                out.write("\n\n")
            if paramstring:
                out.write(paramstring)
            docstring = out.getvalue()
        typ.__doc__ = docstring

        # assign the newly created type to the target module
        module = self.options.namespaces[table.name.scope_id]
        typ.__module__ = module.__name__
        setattr(sys.modules[module.__name__], class_name, typ)

        return typ


def table_to_dataclass(
    table: Table, options: SqlConverterOptions
) -> type[DataclassInstance]:
    converter = SqlConverter(options)
    return converter.table_to_dataclass(table)
