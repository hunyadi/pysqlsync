import dataclasses
import datetime
import decimal
import ipaddress
import typing
import uuid
from dataclasses import dataclass
from typing import Annotated, Any

from strong_typing.inspection import (
    DataclassInstance,
    TypeLike,
    enum_value_types,
    get_annotation,
    is_dataclass_type,
    is_type_enum,
    is_type_literal,
    is_type_optional,
    is_type_union,
    unwrap_annotated_type,
    unwrap_literal_types,
    unwrap_optional_type,
    unwrap_union_types,
)

from .key_types import PrimaryKey, PrimaryKeyTag


def is_primary_key_type(field_type: TypeLike) -> bool:
    "Checks if the field type is marked as the primary key of a table."

    return get_annotation(field_type, PrimaryKeyTag) is not None


def get_primary_key_name(class_type: type[DataclassInstance]) -> str:
    "Fetches the primary key of the table."

    for field in dataclasses.fields(class_type):
        if is_primary_key_type(field.type):
            return field.name

    raise TypeError(f"table type has no primary key: {class_type.__name__}")


def is_constraint(item: Any) -> bool:
    return isinstance(item, PrimaryKeyTag)


def tsv_type(plain_type: TypeLike) -> type:
    "Python type that represents the TSV storage type for this data."

    if is_type_enum(plain_type):
        value_types = set(enum_value_types(plain_type))
        if len(value_types) != 1:
            raise TypeError("inconsistent enumeration value types")
        return value_types.pop()
    elif is_type_literal(plain_type):
        literal_types = set(tsv_type(t) for t in unwrap_literal_types(plain_type))
        if len(literal_types) != 1:
            raise TypeError("inconsistent literal value types")
        return literal_types.pop()
    elif is_type_union(plain_type):
        union_types = set(tsv_type(t) for t in unwrap_union_types(plain_type))
        if len(union_types) != 1:
            raise TypeError("inconsistent union types")
        return union_types.pop()
    elif is_dataclass_type(plain_type):
        return dict
    elif plain_type is bool:
        return bool
    elif plain_type is bytes:
        return bytes
    elif plain_type is int:
        return int
    elif plain_type is float:
        return float
    elif plain_type is str:
        return str
    elif plain_type is uuid.UUID:
        return uuid.UUID
    elif plain_type is datetime.datetime:
        return datetime.datetime
    elif plain_type is datetime.date:
        return datetime.date
    elif plain_type is datetime.time:
        return datetime.time
    elif plain_type is decimal.Decimal:
        return decimal.Decimal
    elif plain_type is ipaddress.IPv4Address:
        return ipaddress.IPv4Address
    elif plain_type is ipaddress.IPv6Address:
        return ipaddress.IPv6Address

    origin_type = typing.get_origin(plain_type)
    if origin_type is list:  # JSON
        return list
    elif origin_type is dict:  # JSON
        return dict

    raise TypeError(f"unsupported TSV value type: {plain_type}")


@dataclass
class FieldProperties:
    """
    Captures type information associated with a field type.

    :param plain_type: Unadorned type without any metadata.
    :param metadata: Any metadata that is not a constraint such as identity, primary key or unique.
    :param is_primary: True if the field is a primary key.
    """

    plain_type: TypeLike
    nullable: bool
    metadata: tuple[Any, ...]
    is_primary: bool

    @property
    def field_type(self) -> TypeLike:
        "Type without constraint annotations such as identity, primary key, or unique."

        if self.metadata:
            # type becomes Annotated[T, ...]
            return Annotated[(self.plain_type, *self.metadata)]
        else:
            # type becomes a regular type
            return self.plain_type

    @property
    def tsv_type(self) -> type:
        "Python type that represents the TSV storage type for this data."

        return tsv_type(self.plain_type)


def get_field_properties(field_type: TypeLike) -> FieldProperties:
    if is_type_optional(field_type):
        nullable = True
        field_type = unwrap_optional_type(field_type)
    else:
        nullable = False

    metadata = getattr(field_type, "__metadata__", None)
    if metadata is None:
        # field has a type without annotations or constraints
        return FieldProperties(
            plain_type=field_type, nullable=nullable, metadata=(), is_primary=False
        )

    # field has a type of Annotated[T, ...]
    plain_type = unwrap_annotated_type(field_type)

    # check for constraints
    is_primary = is_primary_key_type(field_type)

    # filter annotations that represent constraints
    metadata = tuple(item for item in metadata if not is_constraint(item))

    return FieldProperties(
        plain_type=plain_type,
        nullable=nullable,
        metadata=metadata,
        is_primary=is_primary,
    )


@dataclass
class ClassProperties:
    fields: tuple[FieldProperties, ...]

    @property
    def tsv_types(self) -> tuple[type, ...]:
        return tuple(prop.tsv_type for prop in self.fields)


def get_class_properties(class_type: type[DataclassInstance]) -> ClassProperties:
    return ClassProperties(
        tuple(
            get_field_properties(field.type) for field in dataclasses.fields(class_type)
        )
    )
