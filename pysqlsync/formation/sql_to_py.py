import dataclasses
import keyword
import sys
from datetime import date, datetime, time
from io import StringIO
from types import ModuleType
from typing import Any
from uuid import UUID

from strong_typing.core import JsonType
from strong_typing.inspection import DataclassInstance, TypeLike

from ..model.data_types import *
from ..model.key_types import PrimaryKey
from .object_types import Column, Table


def sql_type_to_python(sql_type: SqlDataType) -> TypeLike:
    if isinstance(sql_type, SqlBooleanType):
        return bool
    elif isinstance(sql_type, SqlIntegerType):
        return int
    elif isinstance(sql_type, (SqlRealType, SqlDoubleType, SqlFloatType)):
        return float
    elif isinstance(sql_type, SqlTimestampType):
        return datetime
    elif isinstance(sql_type, SqlDateType):
        return date
    elif isinstance(sql_type, SqlTimeType):
        return time
    elif isinstance(sql_type, SqlCharacterType):
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


def column_to_field(
    table: Table, column: Column
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

    return (
        field_name,
        field_type,
        dataclasses.field(default=default),
    )


def table_to_dataclass(table: Table, module: ModuleType) -> type[DataclassInstance]:
    """
    Generates a dataclass type corresponding to a table schema.

    :param table: The database table from which to produce a dataclass.
    """

    fields = [column_to_field(table, column) for column in table.columns.values()]
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
    typ.__module__ = module.__name__
    setattr(sys.modules[module.__name__], class_name, typ)

    return typ
