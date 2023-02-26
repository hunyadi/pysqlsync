import dataclasses
import typing
from dataclasses import dataclass
from typing import Annotated, Any, TypeVar

from strong_typing.inspection import get_annotation

T = TypeVar("T")


class PrimaryKeyTag:
    "Marks a field as the primary key of a table."


PrimaryKey = Annotated[T, PrimaryKeyTag()]


def is_primary_key_type(field_type: type) -> bool:
    "Checks if the field type is marked as the primary key of a table."

    return get_annotation(field_type, PrimaryKeyTag) is not None


def get_primary_key_name(class_type: type) -> str:
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

    :param field_type: Type without constraint annotations such as identity, primary key, or unique.
    :param inner_type: Type with no metadata (annotations).
    :param metadata: Any metadata that is not a constraint such as identity, primary key or unique.
    :param is_primary: True if the field is a primary key.
    """

    field_type: type
    inner_type: type
    metadata: list
    is_primary: bool


def get_field_properties(field_type: type) -> FieldProperties:
    metadata = getattr(field_type, "__metadata__", None)
    if metadata is None:
        # field has a type without annotations or constraints
        return FieldProperties(field_type, field_type, [], False)

    # field has a type of Annotated[T, ...]
    inner_type = typing.get_args(field_type)[0]

    # check for constraints
    is_primary = is_primary_key_type(field_type)

    # filter annotations that represent constraints
    metadata = [item for item in metadata if not is_constraint(item)]

    if metadata:
        # type becomes Annotated[T, ...]
        outer_type: type = Annotated[(inner_type, *metadata)]  # type: ignore
    else:
        # type becomes a regular type
        outer_type = inner_type

    return FieldProperties(outer_type, inner_type, metadata, is_primary)
