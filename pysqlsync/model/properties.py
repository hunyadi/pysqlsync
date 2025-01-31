import dataclasses
import datetime
import decimal
import ipaddress
import typing
import uuid
from dataclasses import dataclass
from typing import Annotated, Any, Union

from strong_typing.inspection import (
    DataclassInstance,
    TypeLike,
    dataclass_fields,
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

from .key_types import DefaultTag, IdentityTag, PrimaryKeyTag, UniqueTag


def is_primary_key_type(field_type: TypeLike) -> bool:
    "Checks if the field type is marked as the primary key of a table."

    return get_annotation(field_type, PrimaryKeyTag) is not None


def is_identity_type(field_type: TypeLike) -> bool:
    "Checks if the field type is marked as an identity column in a table."

    return get_annotation(field_type, IdentityTag) is not None


def is_unique_type(field_type: TypeLike) -> bool:
    "Checks if the field type is marked as a unique column in a table."

    return get_annotation(field_type, UniqueTag) is not None


def get_primary_key_name(class_type: type[DataclassInstance]) -> str:
    "Fetches the primary key of the table."

    for field in dataclass_fields(class_type):
        if is_primary_key_type(field.type):
            return field.name

    raise TypeError(f"table type has no primary key: {class_type.__name__}")


def get_primary_key_name_type(class_type: type[DataclassInstance]) -> tuple[str, type]:
    "Returns the primary key of the table and its plain representation type."

    for field in dataclass_fields(class_type):
        props = get_field_properties(field.type)
        if props.is_primary:
            return field.name, props.tsv_type

    raise TypeError(f"table type has no primary key: {class_type.__name__}")


def is_constraint(item: Any) -> bool:
    return isinstance(item, (PrimaryKeyTag, IdentityTag, UniqueTag))


def tsv_type(plain_type: TypeLike) -> type:
    "Python type that represents the TSV storage type for this data."

    plain_type = unwrap_annotated_type(plain_type)

    if is_type_enum(plain_type):
        return str
    elif is_type_literal(plain_type):
        literal_types = set(tsv_type(t) for t in unwrap_literal_types(plain_type))
        if len(literal_types) != 1:
            raise TypeError(f"inconsistent literal value types: {literal_types}")
        return literal_types.pop()
    elif is_type_union(plain_type):
        union_types = set(tsv_type(t) for t in unwrap_union_types(plain_type))
        if len(union_types) != 1:
            if (
                len(union_types) == 2
                and ipaddress.IPv4Address in union_types
                and ipaddress.IPv6Address in union_types
            ):
                return typing.cast(type, plain_type)
            raise TypeError(f"inconsistent union types: {union_types}")
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
    elif origin_type is set:  # JSON
        return set
    elif origin_type is dict:  # JSON
        return dict

    raise TypeError(f"unsupported TSV value type: {plain_type}")


@dataclass
class FieldProperties:
    """
    Captures type information associated with a field type.

    :param plain_type: Unadorned type without any metadata.
    :param nullable: True if the field is optional.
    :param metadata: Any metadata that is not a constraint such as identity, primary key or unique.
    :param is_primary: True if the field is a primary key.
    :param is_identity: True if the field is generated as identity.
    :param is_unique: True if values of this type must be unique.
    """

    plain_type: TypeLike
    nullable: bool
    metadata: tuple[Any, ...]
    is_primary: bool
    is_identity: bool
    is_unique: bool

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


def _get_field_metadata(field_type: TypeLike) -> list[Any]:
    # field has a type of Annotated[T, ...]
    return list(getattr(field_type, "__metadata__", ()))


def get_field_properties(field_type: TypeLike) -> FieldProperties:
    "Extracts column properties such as primary key, identity or unique constraints."

    metadata = _get_field_metadata(field_type)
    plain_type = unwrap_annotated_type(field_type)

    # check if field has DEFAULT as a union type member
    if is_type_union(plain_type):
        union_types = list(unwrap_union_types(plain_type))
        if DefaultTag in union_types:
            union_types.remove(DefaultTag)
            plain_type = Union[tuple(union_types)]  # may cease to become a union type

    # check if field is nullable
    if is_type_optional(plain_type):
        nullable = True
        plain_type = unwrap_optional_type(plain_type)
    else:
        nullable = False

    metadata.extend(_get_field_metadata(plain_type))
    plain_type = unwrap_annotated_type(plain_type)

    if metadata is None:
        # field has a type without annotations or constraints
        return FieldProperties(
            plain_type=plain_type,
            nullable=nullable,
            metadata=(),
            is_primary=False,
            is_identity=False,
            is_unique=False,
        )

    # check for constraints
    is_primary = any(isinstance(m, PrimaryKeyTag) for m in metadata)
    is_identity = any(isinstance(m, IdentityTag) for m in metadata)
    is_unique = any(isinstance(m, UniqueTag) for m in metadata)

    # filter annotations that represent constraints
    metadata = [item for item in metadata if not is_constraint(item)]

    return FieldProperties(
        plain_type=plain_type,
        nullable=nullable,
        metadata=tuple(metadata),
        is_primary=is_primary,
        is_identity=is_identity,
        is_unique=is_unique,
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
