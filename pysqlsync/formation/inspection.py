import datetime
import decimal
import ipaddress
import uuid
from typing import Any, TypeGuard

from strong_typing.inspection import (
    DataclassInstance,
    TypeLike,
    dataclass_fields,
    is_dataclass_type,
    is_type_enum,
    is_type_optional,
    unwrap_annotated_type,
    unwrap_optional_type,
)

from ..model.properties import get_field_properties


def is_simple_type(typ: Any) -> bool:
    "True if the type is not a composite or user-defined type."

    typ = unwrap_annotated_type(typ)

    if (
        typ is bool
        or typ is int
        or typ is float
        or typ is str
        or typ is decimal.Decimal
        or typ is datetime.datetime
        or typ is datetime.date
        or typ is datetime.time
        or typ is datetime.timedelta
        or typ is uuid.UUID
        or typ is ipaddress.IPv4Address  # PostgreSQL only
        or typ is ipaddress.IPv6Address  # PostgreSQL only
    ):
        return True
    if is_type_enum(typ):
        return True
    if is_type_optional(typ):
        typ = unwrap_optional_type(typ)
        return is_simple_type(typ)
    return False


def is_entity_type(typ: Any) -> TypeGuard[type[DataclassInstance]]:
    "True for data-class types that have a primary key."

    if not is_dataclass_type(typ):
        return False

    return dataclass_has_primary_key(typ)


def is_struct_type(typ: Any) -> TypeGuard[type[DataclassInstance]]:
    "True for data-class types that have no primary key."

    if not is_dataclass_type(typ):
        return False

    return not dataclass_has_primary_key(typ)


def dataclass_has_primary_key(typ: type[DataclassInstance]) -> bool:
    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return True

    return False


def dataclass_primary_key_name(typ: type[DataclassInstance]) -> str:
    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return field.name

    raise TypeError(f"table {typ.__name__} lacks primary key")


def dataclass_primary_key_type(typ: type[DataclassInstance]) -> TypeLike:
    "Extracts the primary key data type from a dataclass."

    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return props.field_type

    raise TypeError(f"table {typ.__name__} lacks primary key")
