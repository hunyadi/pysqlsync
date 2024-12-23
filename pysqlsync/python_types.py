import dataclasses
import enum
import inspect
import io
import re
import sys
import textwrap
import typing
from types import ModuleType
from typing import TextIO

from strong_typing.docstring import has_docstring, parse_type
from strong_typing.inspection import (
    DataclassInstance,
    get_module_classes,
    is_dataclass_type,
    is_type_enum,
)
from strong_typing.name import TypeFormatter


def module_to_stream(module: ModuleType, target: TextIO) -> None:
    "Generates Python code of all declared types in a module."

    print("from __future__ import annotations", file=target)
    print("import dataclasses", file=target)
    print("import enum", file=target)
    print("from datetime import datetime, date, time", file=target)
    print("from decimal import Decimal", file=target)
    print("from uuid import UUID", file=target)
    print(
        "from typing import Annotated, Dict, List, Literal, Optional, Union",
        file=target,
    )
    print(
        "from strong_typing.auxiliary import int16, int32, int64, float32, float64, MaxLength, Precision, TimePrecision",
        file=target,
    )
    print(file=target)

    for cls in get_module_classes(module):
        if is_type_enum(cls):
            enum_class_to_stream(cls, target)
        elif is_dataclass_type(cls):
            dataclass_to_stream(cls, target)
        else:
            raise NotImplementedError(
                "classes in module must be of enumeration or data-class type"
            )
        print(file=target)


def module_to_code(module: ModuleType) -> str:
    "Generates Python code of all declared types in a module."

    with io.StringIO() as out:
        module_to_stream(module, out)
        return out.getvalue()


def dataclass_to_stream(typ: type[DataclassInstance], target: TextIO) -> None:
    "Generates Python code corresponding to a dataclass type."

    print("@dataclasses.dataclass", file=target)
    print(f"class {typ.__name__}:", file=target)

    # check if class has a doc-string other than the auto-generated string assigned by @dataclass
    if has_docstring(typ):
        if "\n" in typing.cast(str, typ.__doc__):
            ds = parse_type(typ)
            print('    """', file=target)

            if ds.short_description:
                _wrap_print(ds.short_description, file=target)
                if ds.long_description:
                    print(file=target)
                    _wrap_print(ds.long_description, file=target)

            if ds.short_description and (ds.params or ds.returns):
                print(file=target)

            for name, param in ds.params.items():
                _wrap_print(f":param {name}: {param.description}", file=target)
            if ds.returns:
                _wrap_print(f":returns: {ds.returns.description}", file=target)

            print('    """', file=target)
        else:
            print(f"    {repr(typ.__doc__)}", file=target)
        print(file=target)

    # class variables (e.g. "primary_key")
    field_names = [field.name for field in dataclasses.fields(typ)]
    variables = {
        name: value
        for name, value in inspect.getmembers(typ, lambda m: not inspect.isroutine(m))
        if not re.match(r"^__.+__$", name) and name not in field_names
    }
    if variables:
        for name, value in variables.items():
            print(f"    {name} = {repr(value)}", file=target)
        print(file=target)

    # member variables
    fmt = TypeFormatter(context=sys.modules[typ.__module__])
    for field in dataclasses.fields(typ):
        type_name = fmt.python_type_to_str(field.type)
        metadata = dict(field.metadata)

        field_initializer: dict[str, str] = {}
        if field.default is not dataclasses.MISSING:
            field_initializer["default"] = repr(field.default)
        if field.default_factory is not dataclasses.MISSING:
            field_initializer["default_factory"] = fmt.python_type_to_str(
                field.default_factory
            )
        if metadata:
            field_initializer["metadata"] = repr(metadata)

        if not field_initializer:
            initializer = ""
        elif field.default is not dataclasses.MISSING and len(field_initializer) == 1:
            initializer = f" = {repr(field.default)}"
        else:
            initializer_list = ", ".join(
                f"{key} = {value}" for key, value in field_initializer.items()
            )
            initializer = f" = field({initializer_list})"

        print(f"    {field.name}: {type_name}{initializer}", file=target)


def dataclasses_to_stream(types: list[type[DataclassInstance]], target: TextIO) -> None:
    "Generates Python code corresponding to a set of dataclass types."

    for typ in types:
        dataclass_to_stream(typ, target)


def dataclass_to_code(data_type: type[DataclassInstance]) -> str:
    "Generates Python code corresponding to a dataclass type."

    return dataclasses_to_code([data_type])


def dataclasses_to_code(types: list[type[DataclassInstance]]) -> str:
    "Generates Python code corresponding to a set of dataclass types."

    f = io.StringIO()
    dataclasses_to_stream(types, f)
    return f.getvalue()


def enum_class_to_stream(enum_class: type[enum.Enum], target: TextIO) -> None:
    "Writes an enumeration class as a class definition."

    print("@enum.unique", file=target)
    print(f"class {enum_class.__name__}(enum.Enum):", file=target)
    if enum_class.__doc__:
        print(f"    {repr(enum_class.__doc__)}", file=target)
        print(file=target)

    for e in enum_class:
        value = repr(e.value)
        print(f"    {e.name} = {value}", file=target)

    for e in enum_class:
        if not e.__doc__ or e.__doc__ == enum_class.__doc__:
            continue

        print(
            f"{enum_class.__name__}.{e.name}.__doc__ = {repr(e.__doc__)}",
            file=target,
        )


def enum_class_to_code(enum_class: type[enum.Enum]) -> str:
    "Returns an enumeration class as a class definition string."

    with io.StringIO() as out:
        enum_class_to_stream(enum_class, out)
        return out.getvalue()


def _wrap_print(text: str, file: TextIO) -> None:
    if not text:
        return

    # wrap long lines
    for line in textwrap.wrap(
        text,
        width=139,
        initial_indent="    ",
        subsequent_indent="    ",
        break_long_words=False,
        break_on_hyphens=False,
    ):
        print(line, file=file)
