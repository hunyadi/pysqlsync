import dataclasses
from dataclasses import dataclass
from typing import Annotated, Any

from strong_typing.inspection import (
    DataclassInstance,
    TypeLike,
    get_annotation,
    unwrap_annotated_type,
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


@dataclass
class FieldProperties:
    """
    Captures type information associated with a field type.

    :param plain_type: Unadorned type without any metadata.
    :param metadata: Any metadata that is not a constraint such as identity, primary key or unique.
    :param is_primary: True if the field is a primary key.
    """

    plain_type: TypeLike
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


def get_field_properties(field_type: TypeLike) -> FieldProperties:
    metadata = getattr(field_type, "__metadata__", None)
    if metadata is None:
        # field has a type without annotations or constraints
        return FieldProperties(field_type, (), False)

    # field has a type of Annotated[T, ...]
    plain_type = unwrap_annotated_type(field_type)

    # check for constraints
    is_primary = is_primary_key_type(field_type)

    # filter annotations that represent constraints
    metadata = tuple(item for item in metadata if not is_constraint(item))

    return FieldProperties(plain_type, metadata, is_primary)
