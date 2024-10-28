import dataclasses
import sys
from typing import Any, Optional

from strong_typing.inspection import DataclassField, DataclassInstance, dataclass_fields

from .key_types import PrimaryKey


def key_value_fields(
    cls: type[DataclassInstance], key: str
) -> tuple[DataclassField, list[DataclassField]]:
    """
    Separates the key and the value fields in a data-class definition.

    :param cls: The data-class type to transform.
    :param key: The field to be come the primary key.
    """

    source_fields = dataclass_fields(cls)
    key_field: Optional[DataclassField] = None
    value_fields: list[DataclassField] = []
    for source_field in source_fields:
        if source_field.name == key:
            key_field = source_field
        else:
            value_fields.append(source_field)

    if key_field is None:
        raise ValueError(f"key field not found: {key}")

    return key_field, value_fields


def make_entity(cls: type[DataclassInstance], key: str) -> type[DataclassInstance]:
    """
    Transforms a regular data-class type into an entity type.

    :param cls: The data-class type to transform.
    :param key: The field to become the primary key.
    """

    key_field, value_fields = key_value_fields(cls, key)
    key_type = key_field.type

    primary_key_type = PrimaryKey[key_type]  # type: ignore
    target_fields: list[tuple[str, Any, dataclasses.Field]] = [
        (key_field.name, primary_key_type, dataclasses.field(default=key_field.default))
    ]
    for value_field in value_fields:
        target_fields.append(
            (
                value_field.name,
                value_field.type,
                dataclasses.field(default=value_field.default),
            )
        )

    class_name = cls.__name__
    class_module = cls.__module__
    class_doc = cls.__doc__

    if sys.version_info >= (3, 12):
        class_type = dataclasses.make_dataclass(
            class_name, target_fields, module=class_module
        )
    else:
        class_type = dataclasses.make_dataclass(
            class_name, target_fields, namespace={"__module__": class_module}
        )

    class_type.__doc__ = class_doc
    setattr(sys.modules[class_module], class_name, class_type)
    return class_type
