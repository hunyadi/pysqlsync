import datetime
import decimal
import inspect
import ipaddress
import sys
import types
import uuid
from typing import Any, Optional, TypeGuard

from strong_typing.core import JsonType
from strong_typing.inspection import (
    DataclassInstance,
    TypeLike,
    dataclass_fields,
    evaluate_member_type,
    is_dataclass_type,
    is_type_enum,
    is_type_optional,
    is_type_union,
    unwrap_annotated_type,
    unwrap_optional_type,
    unwrap_union_types,
)

from ..model.properties import get_field_properties, is_primary_key_type


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
        or typ is JsonType
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
        if is_primary_key_type(field.type):
            return True

    return False


def dataclass_primary_key_name(typ: type[DataclassInstance]) -> str:
    for field in dataclass_fields(typ):
        if is_primary_key_type(field.type):
            return field.name

    raise TypeError(f"table {typ.__name__} lacks primary key")


def dataclass_primary_key_type(typ: type[DataclassInstance]) -> TypeLike:
    "Extracts the primary key data type from a dataclass."

    for field in dataclass_fields(typ):
        props = get_field_properties(field.type)
        if props.is_primary:
            return props.field_type

    raise TypeError(f"table {typ.__name__} lacks primary key")


def get_entity_types(modules: list[types.ModuleType]) -> list[type[DataclassInstance]]:
    "Returns all entity types defined in one of the modules."

    entity_types: list[type[DataclassInstance]] = []
    for module in modules:
        for name, obj in inspect.getmembers(module, is_entity_type):
            if sys.modules[obj.__module__] in modules:
                entity_types.append(obj)
    return entity_types


def reference_to_key(typ: TypeLike, cls: type[DataclassInstance]) -> TypeLike:
    """
    Maps a foreign or discriminated reference type to the primary key type of the entity being referenced.

    :param typ: A reference type, or a regular type.
    :param cls: The entity context in which forward reference types are evaluated.
    :returns: The primary key type of the referenced entity (or entities), or the original type.
    """

    data_type = evaluate_member_type(typ, cls)
    if is_type_optional(data_type):
        # nullable type
        required_type = reference_to_key(unwrap_optional_type(data_type), cls)
        return Optional[required_type]
    elif is_entity_type(data_type):
        # foreign key reference
        return dataclass_primary_key_type(data_type)
    elif is_type_union(data_type):
        # discriminated key reference
        union_types = tuple(
            evaluate_member_type(t, cls)
            for t in unwrap_union_types(data_type)
            if t is not None
        )
        if all(is_entity_type(t) for t in union_types):
            return dataclass_primary_key_type(union_types[0])

    return data_type
