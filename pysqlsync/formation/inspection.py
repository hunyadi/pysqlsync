import datetime
import decimal
import inspect
import sys
import types
import uuid
from ipaddress import IPv4Address, IPv6Address
from typing import Any, Optional

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
from ..util.typing import TypeGuard


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
        or typ is IPv4Address  # PostgreSQL only
        or typ is IPv6Address  # PostgreSQL only
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


def is_reference_type(typ: Any, cls: type) -> bool:
    """
    True for an entity type (foreign key) or a union of entity types (discriminated key).

    :param typ: The type to test.
    :param cls: The context for evaluating forward references.
    """

    if is_entity_type(typ):
        return True
    elif is_type_union(typ):
        member_types = [
            evaluate_member_type(t, cls)
            for t in unwrap_union_types(unwrap_annotated_type(typ))
        ]
        return all(is_entity_type(t) for t in member_types)
    else:
        return False


def is_ip_address_type(field_type: type) -> bool:
    "Check if type is an IPv4 or IPv6 address type."

    if field_type is IPv4Address or field_type is IPv6Address:
        return True

    # check if type is IPv4Address | IPv6Address
    if is_type_union(field_type):
        field_type = unwrap_annotated_type(field_type)
        member_types = unwrap_union_types(field_type)
        if len(member_types) != 2:
            return False
        if IPv4Address in member_types and IPv6Address in member_types:
            return True

    return False


def dataclass_has_primary_key(typ: type[DataclassInstance]) -> bool:
    typ = unwrap_annotated_type(typ)
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
    """
    Extracts the primary key data type from a dataclass.

    This function returns the type of the primary key field without constraint annotations such as identity,
    primary key, or unique. This may be a parameterized or annotated type, e.g. `Annotated[str, MaxLength(255)]`.
    """

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
    plain_type = unwrap_annotated_type(data_type)
    if is_type_optional(plain_type):
        # nullable type
        required_type = reference_to_key(unwrap_optional_type(plain_type), cls)
        return Optional[required_type]
    elif is_entity_type(plain_type):
        # foreign key reference
        return dataclass_primary_key_type(plain_type)
    elif is_type_union(plain_type):
        # discriminated key reference
        union_types = tuple(
            evaluate_member_type(t, cls)
            for t in unwrap_union_types(plain_type)
            if t is not None
        )
        if all(is_entity_type(t) for t in union_types):
            return dataclass_primary_key_type(union_types[0])

    return data_type
