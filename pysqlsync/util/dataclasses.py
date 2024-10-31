import dataclasses
import sys
import types
from typing import Optional

from strong_typing.docstring import Docstring, DocstringParam, parse_type
from strong_typing.inspection import DataclassField, DataclassInstance, dataclass_fields

from ..formation.inspection import is_struct_type
from ..model.properties import get_field_properties


def make_dataclass(
    name: str,
    fields: list[DataclassField],
    *,
    module: Optional[types.ModuleType] = None,
    docstring: Optional[Docstring] = None,
) -> type[DataclassInstance]:
    """
    Dynamically creates a new data class type.

    :param name: Name of the newly created type.
    :param fields: Fields of the new data class type to create.
    :param module: The module to assign the new type to.
    :param docstring: The doc-string to set on the newly created type.
    """

    field_names = set(field.name for field in fields)

    if docstring is not None:
        docstring.params = {
            name: param
            for name, param in docstring.params.items()
            if name in field_names
        }

    dataclass_fields = [(field.name, field.type, field.default) for field in fields]
    module_name = module.__name__ if module is not None else None
    if sys.version_info >= (3, 12):
        data_type = dataclasses.make_dataclass(
            name, dataclass_fields, module=module_name
        )
    else:
        namespace = {"__module__": module_name} if module is not None else None
        data_type = dataclasses.make_dataclass(
            name, dataclass_fields, namespace=namespace
        )
    if module is not None:
        setattr(module, data_type.__name__, data_type)

    if docstring is not None:
        data_type.__doc__ = str(docstring)

    return data_type


def flatten_dataclass(
    cls: type[DataclassInstance],
) -> tuple[list[DataclassField], Docstring]:
    """
    Extracts nested structures into a flat list of data-class fields.
    """

    fields: list[DataclassField] = []
    docstring = parse_type(cls)

    for field in dataclass_fields(cls):
        props = get_field_properties(field.type)
        plain_type = props.plain_type
        if is_struct_type(plain_type):
            nested_docstring = parse_type(plain_type)
            for subfield in dataclass_fields(plain_type):
                name = f"{field.name}__{subfield.name}"
                fields.append(
                    DataclassField(
                        name,
                        subfield.type,
                        subfield.default,
                    )
                )
                param = nested_docstring.params.get(subfield.name)
                if param is not None:
                    docstring.params[name] = DocstringParam(
                        name, param.description, param.param_type
                    )
        else:
            fields.append(field)

    return fields, docstring
